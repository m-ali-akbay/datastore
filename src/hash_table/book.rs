use std::{cmp::Ordering, collections::BTreeMap, io::{self, Read, Seek, SeekFrom, Write}, mem::replace, ops::Bound};

use crate::{book::{Book, SectionIndex}, hash_table::{HashTable, HashTableEntry, HashTableScanner, SliceHasher, SliceHasherBuilder}};

use super::HashTableScanFilter;

pub struct SectionHeader {
    pub end_offset: u64,
}

pub type IndexChunk = u32;
pub type IndexChunkSize = u32;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct IndexKey {
    pub section_index: SectionIndex,
    pub index_chunk: IndexChunk,
}

pub struct IndexHeader {
    pub bloom_filter: u64,
    pub first_entry_offset: u64,
}

pub struct BookHashTable<H, B> {
    hasher_builder: H,
    book: B,
    section_count: SectionIndex,
    sections: BTreeMap<SectionIndex, SectionHeader>,
    index_chunk_size: IndexChunkSize,
    indexes: BTreeMap<IndexKey, IndexHeader>,
}

impl<H: SliceHasherBuilder, B: Book> BookHashTable<H, B> {
    pub fn new(
        hasher_builder: H,
        book: B,
        section_count: SectionIndex,
        index_chunk_size: IndexChunkSize,
    ) -> Self {
        Self {
            hasher_builder,
            book,
            section_count,
            sections: BTreeMap::new(),
            index_chunk_size,
            indexes: BTreeMap::new(),
        }
    }

    pub fn load(
        hasher_builder: H,
        book: B,
        section_count: SectionIndex,
        sections: impl Iterator<Item = (SectionIndex, SectionHeader)>,
        index_chunk_size: IndexChunkSize,
        indexes: impl Iterator<Item = (IndexKey, IndexHeader)>,
    ) -> io::Result<Self> {
        Ok(Self {
            hasher_builder,
            book,
            section_count,
            sections: sections.collect(),
            index_chunk_size,
            indexes: indexes.collect(),
        })
    }

    pub fn export<T>(
        &self,
        callback: impl FnOnce(
            &B,
            &mut dyn Iterator<Item = (SectionIndex, &SectionHeader)>,
            &mut dyn Iterator<Item = (IndexKey, &IndexHeader)>,
        ) -> T,
    ) -> io::Result<T> {
        let mut sections = self.sections.iter().map(|(k, v)| (*k, v));
        let mut indexes = self.indexes.iter().map(|(k, v)| (*k, v));
        Ok(callback(&self.book, &mut sections, &mut indexes))
    }
}

impl<H: SliceHasherBuilder, B: Book> HashTable for BookHashTable<H, B> {
    fn insert(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        let mut hasher = self.hasher_builder.build();
        hasher.update(key);
        let hash = hasher.finalize();

        let section_index = hash % self.section_count;
        let bloom_index = (hash / self.section_count) as u64 % 64;
        let bloom_bit = 1u64 << bloom_index;

        let mut section = self.book.section(section_index);
        let section_header = self.sections.entry(section_index).or_insert(SectionHeader {
            end_offset: 0,
        });

        let index_chunk = (section_header.end_offset / self.index_chunk_size as u64) as IndexChunk;
        let index_key = IndexKey {
            section_index,
            index_chunk,
        };

        let index_header = self.indexes.entry(index_key).or_insert(IndexHeader {
            bloom_filter: 0,
            first_entry_offset: section_header.end_offset,
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

        index_header.bloom_filter |= bloom_bit;

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
        };
        let bloom_query = match filter {
            HashTableScanFilter::Key(key) => {
                let mut hasher = self.hasher_builder.build();
                hasher.update(key);
                let hash = hasher.finalize();
                let bloom_index = (hash / self.section_count) as u64 % 64;
                Some(1u64 << bloom_index)
            },
            _ => None,
        };
        let section_scanners = match section_index {
            Some(index) => {
                match self.sections.get(&index) {
                    Some(sh) => SectionScannerIterator::Single(SectionScanner {
                        section: self.book.section(index),
                        section_index: index,
                        section_end: sh.end_offset,
                        bloom_query,
                        index_chunk: None,
                        index_chunk_size: self.index_chunk_size,
                        index_headers: &self.indexes,
                    }),
                    None => SectionScannerIterator::None,
                }
            },
            None => SectionScannerIterator::Many(self.sections.iter().map(move |(&section_index, section_header)| {
                SectionScanner {
                    section: self.book.section(section_index),
                    section_index,
                    section_end: section_header.end_offset,
                    bloom_query,
                    index_chunk: None,
                    index_chunk_size: self.index_chunk_size,
                    index_headers: &self.indexes,
                }
            })),
        };
        let multi_scanner = MultiSectionScanner {
            scanners: section_scanners,
            current_scanner: None,
        };
        Ok(FilterScanner {
            filter,
            key_buffer: [0u8; 256],
            scanner: multi_scanner,
        })
    }
}

struct FilterScanner<'key, Scanner> {
    filter: HashTableScanFilter<'key>,
    key_buffer: [u8; 256],
    scanner: Scanner,
}

impl<'key, Scanner: HashTableScanner> HashTableScanner for FilterScanner<'key, Scanner> {
    fn next(&mut self) -> io::Result<Option<impl HashTableEntry + use<'key, Scanner>>> {
        'entry_loop: loop {
            let mut entry = match self.scanner.next()? {
                Some(e) => e,
                None => return Ok(None),
            };
            match &self.filter {
                HashTableScanFilter::Key(expected_key) => {
                    let mut key_reader = entry.key()?;
                    let mut expected_key = *expected_key;
                    loop {
                        let read_size = key_reader.read(&mut self.key_buffer)?;
                        if read_size == 0 {
                            if expected_key.is_empty() {
                                drop(key_reader);
                                return Ok(Some(entry));
                            } else {
                                continue 'entry_loop;
                            }
                        }
                        if read_size > expected_key.len() {
                            continue 'entry_loop;
                        }
                        if &self.key_buffer[..read_size] != &expected_key[..read_size] {
                            continue 'entry_loop;
                        }
                        expected_key = &expected_key[read_size..];
                    }
                },
                HashTableScanFilter::All => {
                    return Ok(Some(entry));
                },
            }
        }
    }
}

struct MultiSectionScanner<'a, Section, I: Iterator<Item = SectionScanner<'a, Section>>> {
    scanners: I,
    current_scanner: Option<SectionScanner<'a, Section>>,
}

impl<'a, Section: Read + Seek + Clone, I: Iterator<Item = SectionScanner<'a, Section>>> HashTableScanner for MultiSectionScanner<'a, Section, I> {
    fn next(&mut self) -> io::Result<Option<impl HashTableEntry + use<'a, Section, I>>> {
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

enum SectionScannerIterator<'a, Section, I: Iterator<Item = SectionScanner<'a, Section>>> {
    Single(SectionScanner<'a, Section>),
    None,
    Many(I),
}

impl<'a, Section, I: Iterator<Item = SectionScanner<'a, Section>>> Iterator for SectionScannerIterator<'a, Section, I> {
    type Item = SectionScanner<'a, Section>;

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

struct SectionScanner<'a, Section> {
    section: Section,
    section_index: SectionIndex,
    section_end: u64,
    bloom_query: Option<u64>,
    index_chunk: Option<(IndexKey, &'a IndexHeader)>,
    index_chunk_size: IndexChunkSize,
    index_headers: &'a BTreeMap<IndexKey, IndexHeader>,
}

struct ScannerEntry<Reader: Read + Seek + Clone> {
    reader: Reader,
    key_size: u32,
    value_size: u32,
}

impl<'a, Reader: Read + Seek + Clone> SectionScanner<'a, Reader> {
    fn next(&mut self) -> io::Result<Option<ScannerEntry<Reader>>> {
        let mut position = self.section.stream_position()?;

        if let Some(bloom_query) = self.bloom_query {
            let index_chunk = (position / self.index_chunk_size as u64) as IndexChunk;
            let index_key = IndexKey {
                section_index: self.section_index,
                index_chunk,
            };
            match &self.index_chunk {
                Some((current_index_key, _)) if *current_index_key == index_key => {
                    // TODO: in this case, we may skip next steps
                },
                _ => {
                    self.index_chunk = self.index_headers.get(&index_key).map(|ih| (index_key, ih));
                },
            }
            let Some((_, index_header)) = &self.index_chunk else {
                return Ok(None);
            };
            if (index_header.bloom_filter & bloom_query) == 0 {
                let next_index_header = self.index_headers.range((Bound::Excluded(index_key), Bound::Excluded(IndexKey {
                    section_index: index_key.section_index + 1,
                    index_chunk: 0,
                }))).next();
                let next_position = match next_index_header {
                    Some((_, IndexHeader { first_entry_offset, .. })) => *first_entry_offset,
                    None => self.section_end,
                };
                self.section.seek(SeekFrom::Start(next_position))?;
                position = next_position;
            }
        }

        match position.cmp(&self.section_end) {
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
