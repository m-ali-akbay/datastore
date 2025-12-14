use std::{cmp::Ordering, collections::BTreeMap, io::{self, Read, Seek, SeekFrom, Write}, mem::replace};

use crate::{book::{Book, SectionIndex}, hash_table::{HashTable, HashTableEntry, HashTableScanner, SliceHasher, SliceHasherBuilder}};

use super::HashTableScanFilter;

pub struct SectionHeader {
    pub end_offset: u64,
}

pub struct BookHashTable<H, B> {
    hasher_builder: H,
    book: B,
    section_count: SectionIndex,
    sections: BTreeMap<SectionIndex, SectionHeader>,
}

impl<H: SliceHasherBuilder, B: Book> BookHashTable<H, B> {
    pub fn new(
        hasher_builder: H,
        book: B,
        section_count: SectionIndex,
    ) -> Self {
        Self {
            hasher_builder,
            book,
            section_count,
            sections: BTreeMap::new(),
        }
    }

    pub fn load(
        hasher_builder: H,
        book: B,
        section_count: SectionIndex,
        sections: impl Iterator<Item = (SectionIndex, SectionHeader)>,
    ) -> io::Result<Self> {
        Ok(Self {
            hasher_builder,
            book,
            section_count,
            sections: sections.collect(),
        })
    }

    pub fn export<T>(
        &self,
        callback: impl FnOnce(
            &B,
            &mut dyn Iterator<Item = (SectionIndex, &SectionHeader)>,
        ) -> T,
    ) -> io::Result<T> {
        let mut sections = self.sections.iter().map(|(k, v)| (*k, v));
        Ok(callback(&self.book, &mut sections))
    }
}

impl<H: SliceHasherBuilder, B: Book> HashTable for BookHashTable<H, B> {
    fn insert(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        let mut hasher = self.hasher_builder.build();
        hasher.update(key);
        let hash = hasher.finalize();
        let section_index = hash % self.section_count;
        let mut section = self.book.section(section_index);
        let section_header = self.sections.entry(section_index).or_insert(SectionHeader {
            end_offset: 0,
        });
        section.seek(SeekFrom::Start(section_header.end_offset))?;

        let key_size = key.len() as u32;
        let value_size = value.len() as u32;
        section.write_all(&key_size.to_le_bytes())?;
        section.write_all(&value_size.to_le_bytes())?;
        section.write_all(key)?;
        section.write_all(value)?;
    
        let new_end = section.stream_position()?;
        section_header.end_offset = new_end;
        Ok(())
    }

    fn scan(&self, filter: HashTableScanFilter) -> io::Result<impl HashTableScanner> {
        let section_index = match filter {
            HashTableScanFilter::All => None,
            HashTableScanFilter::Key(key) => {
                let section_index = {
                    let mut hasher = self.hasher_builder.build();
                    hasher.update(key);
                    let hash = hasher.finalize();
                    hash % self.section_count
                };
                Some(section_index)
            },
            HashTableScanFilter::Hash(hash) => {
                let section_index = hash % self.section_count;
                Some(section_index)
            },
        };
        let section_scanners = match section_index {
            Some(index) => {
                match self.sections.get(&index) {
                    Some(sh) => SectionScannerIterator::Single(SectionScanner {
                        section: self.book.section(index),
                        section_end: sh.end_offset,
                    }),
                    None => SectionScannerIterator::None,
                }
            },
            None => SectionScannerIterator::Many(self.sections.iter().map(|(&section_index, section_header)| {
                SectionScanner {
                    section: self.book.section(section_index),
                    section_end: section_header.end_offset,
                }
            })),
        };
        let multi_scanner = MultiSectionScanner {
            scanners: section_scanners,
            current_scanner: None,
        };
        Ok(FilterScanner {
            filter,
            hasher_builder: &self.hasher_builder,
            key_buffer: [0u8; 256],
            scanner: multi_scanner,
        })
    }
}

struct FilterScanner<'key, Scanner, H> {
    filter: HashTableScanFilter<'key>,
    hasher_builder: H,
    key_buffer: [u8; 256],
    scanner: Scanner,
}

impl<'key, Scanner: HashTableScanner, H: SliceHasherBuilder> HashTableScanner for FilterScanner<'key, Scanner, H> {
    fn next(&mut self) -> io::Result<Option<impl HashTableEntry + use<'key, Scanner, H>>> {
        loop {
            let mut entry = match self.scanner.next()? {
                Some(e) => e,
                None => return Ok(None),
            };
            match &self.filter {
                HashTableScanFilter::Key(expected_key) => {
                    let mut key_reader = entry.key()?;
                    let mut expected_key = *expected_key;
                    loop {
                        let read_size = expected_key.len().min(self.key_buffer.len());
                        if read_size == 0 {
                            drop(key_reader);
                            return Ok(Some(entry));
                        }
                        let read_size = key_reader.read(&mut self.key_buffer[..read_size])?;
                        if read_size == 0 {
                            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Unexpected EOF when reading key"));
                        }
                        if &self.key_buffer[..read_size] != &expected_key[..read_size] {
                            continue;
                        }
                        expected_key = &expected_key[read_size..];
                    }
                },
                HashTableScanFilter::Hash(expected_hash) => {
                    let mut hasher = self.hasher_builder.build();
                    let mut key_reader = entry.key()?;
                    loop {
                        let read_size = key_reader.read(&mut self.key_buffer)?;
                        if read_size == 0 {
                            break;
                        }
                        hasher.update(&self.key_buffer[..read_size]);
                        
                        // try compare
                        if let Some(compare_result) = hasher.try_compare(*expected_hash) {
                            if !compare_result {
                                continue;
                            }
                            break;
                        }
                    }
                    let key_hash = hasher.finalize();
                    if &key_hash != expected_hash {
                        continue;
                    }
                    drop(key_reader);
                    return Ok(Some(entry));
                },
                HashTableScanFilter::All => {
                    return Ok(Some(entry));
                },
            }
        }
    }
}

struct MultiSectionScanner<Section, I: Iterator<Item = SectionScanner<Section>>> {
    scanners: I,
    current_scanner: Option<SectionScanner<Section>>,
}

impl<Section: Read + Seek + Clone, I: Iterator<Item = SectionScanner<Section>>> HashTableScanner for MultiSectionScanner<Section, I> {
    fn next(&mut self) -> io::Result<Option<impl HashTableEntry + use<Section, I>>> {
        loop {
            if let Some(scanner) = &mut self.current_scanner {
                if let Some(entry) = scanner.next()? {
                    return Ok(Some(entry));
                }
                self.current_scanner = None;
            }
            if let Some(next_scanner) = self.scanners.next() {
                self.current_scanner = Some(next_scanner);
                continue;
            }
            return Ok(None);
        }
    }
}

enum SectionScannerIterator<Section, I: Iterator<Item = SectionScanner<Section>>> {
    Single(SectionScanner<Section>),
    None,
    Many(I),
}

impl<Section, I: Iterator<Item = SectionScanner<Section>>> Iterator for SectionScannerIterator<Section, I> {
    type Item = SectionScanner<Section>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            SectionScannerIterator::Single(..) => {
                let section_scanner = replace(self, SectionScannerIterator::None);
                if let SectionScannerIterator::Single(section_scanner) = section_scanner {
                    Some(section_scanner)
                } else {
                    None
                }
            },
            SectionScannerIterator::Many(iter) => iter.next(),
            SectionScannerIterator::None => None,
        }
    }
}

struct SectionScanner<Section> {
    section: Section,
    section_end: u64,
}

struct ScannerEntry<Reader: Read + Seek + Clone> {
    reader: Reader,
    key_size: u32,
    value_size: u32,
}

impl<Reader: Read + Seek + Clone> SectionScanner<Reader> {
    fn next(&mut self) -> io::Result<Option<ScannerEntry<Reader>>> {
        match self.section.stream_position()?.cmp(&self.section_end) {
            Ordering::Greater => {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "Section stream position exceeded section end"));
            },
            Ordering::Equal => {
                return Ok(None);
            },
            Ordering::Less => {},
        };

        let mut size_buf = [0u8; 4];

        self.section.read_exact(&mut size_buf)?;
        let key_size = u32::from_le_bytes(size_buf);

        self.section.read_exact(&mut size_buf)?;
        let value_size = u32::from_le_bytes(size_buf);

        let reader = self.section.clone();

        self.section.seek_relative((key_size + value_size) as i64)?;

        Ok(Some(ScannerEntry {
            reader,
            key_size,
            value_size,
        }))
    }
}

impl<Reader: Read + Seek + Clone> HashTableEntry for ScannerEntry<Reader> {
    fn key_size(&self) -> u32 {
        self.key_size
    }

    fn value_size(&self) -> u32 {
        self.value_size
    }

    fn key(&mut self) -> io::Result<impl Read + '_> {
        let reader = self.reader.clone();
        Ok(reader.take(self.key_size as u64))
    }

    fn value(&mut self) -> io::Result<impl Read + '_> {
        let mut reader = self.reader.clone();
        reader.seek(SeekFrom::Current(self.key_size as i64))?;
        Ok(reader.take(self.value_size as u64))
    }
}
