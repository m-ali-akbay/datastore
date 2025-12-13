use std::{collections::BTreeMap, io::{self, Read, Seek, SeekFrom, Write}};

use crate::{book::{Book, SectionIndex}, hash_table::{Hash, HashTable, HashTableEntry, HashTableScanner, SliceHasher, SliceHasherBuilder}};

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

    fn scan_key(&self, key: &[u8]) -> io::Result<impl HashTableScanner> {
        let mut hasher = self.hasher_builder.build();
        hasher.update(key);
        let hash = hasher.finalize();
        let section_index = hash % self.section_count;
        let section_end = self.sections.get(&section_index)
            .map(|header| header.end_offset)
            .unwrap_or(0);
        let section = self.book.section(section_index);
        let section_scanner = BookHashTableSectionScanner{
            section,
            section_end,
        };
        Ok(BookHashTableKeyScanner {
            key_buf: [0u8; 256],
            section_scanner,
            key,
        })
    }

    fn scan_hash(&self, hash: Hash) -> io::Result<impl HashTableScanner> {
        let section_index = hash % self.section_count;
        let section_end = self.sections.get(&section_index)
            .map(|header| header.end_offset)
            .unwrap_or(0);
        let section = self.book.section(section_index);
        let section_scanner = BookHashTableSectionScanner{
            section,
            section_end,
        };
        Ok(BookHashTableHashScanner {
            section_scanner,
            hasher_builder: &self.hasher_builder,
            key_buf: [0u8; 256],
            hash,
        })
    }

    fn scan_all(&self) -> io::Result<impl HashTableScanner> {
        let sections = self.sections.iter().map(|(k, v)| {
            (self.book.section(*k), v.end_offset)
        });
        Ok(BookHashTableAllScanner {
            sections,
            section_scanner: None,
        })
    }
}

struct BookHashTableSectionScanner<Section> {
    section: Section,
    section_end: u64,
}

struct Entry<Reader: Read + Seek + Clone> {
    reader: Reader,
    key_size: u32,
    value_size: u32,
}

impl<Reader: Read + Seek + Clone> HashTableScanner for BookHashTableSectionScanner<Reader> {
    fn next(&mut self) -> io::Result<Option<impl HashTableEntry + use<Reader>>> {
        // TODO: handle overflow
        if self.section.stream_position()? >= self.section_end {
            return Ok(None);
        }
        let mut size_buf = [0u8; 4];
        self.section.read_exact(&mut size_buf)?;
        let key_size = u32::from_le_bytes(size_buf);
        self.section.read_exact(&mut size_buf)?;
        let value_size = u32::from_le_bytes(size_buf);
        let entry = Entry {
            reader: self.section.clone(),
            key_size,
            value_size,
        };
        self.section.seek_relative((key_size + value_size) as i64)?;
        Ok(Some(entry))
    }
}

impl<Reader: Read + Seek + Clone> HashTableEntry for Entry<Reader> {
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

struct KeyedEntry<'key, Entry: HashTableEntry> {
    key: &'key [u8],
    entry: Entry,
}

impl<'key, Entry: HashTableEntry> HashTableEntry for KeyedEntry<'key, Entry> {
    fn key_size(&self) -> u32 {
        self.key.len() as u32
    }

    fn value_size(&self) -> u32 {
        self.entry.value_size()
    }

    fn key(&mut self) -> io::Result<impl Read + '_> {
        Ok(self.key)
    }

    fn value(&mut self) -> io::Result<impl Read + '_> {
        self.entry.value()
    }
}

struct BookHashTableKeyScanner<'key, Section> {
    key_buf: [u8; 256],
    section_scanner: BookHashTableSectionScanner<Section>,
    key: &'key [u8],
}

impl<'key, Section: Read + Seek + Clone> HashTableScanner for BookHashTableKeyScanner<'key, Section> {
    fn next(&mut self) -> io::Result<Option<impl HashTableEntry + use<'key, Section>>> {
        'entry_loop: while let Some(mut entry) = self.section_scanner.next()? {
            if entry.key_size() as usize != self.key.len() {
                continue;
            }
            {
                let mut expected_key = self.key;
                let mut key_reader = entry.key()?;
                loop {
                    let read_size = expected_key.len().min(self.key_buf.len());
                    if read_size == 0 {
                        break;
                    }
                    let read_size = key_reader.read(&mut self.key_buf[..read_size])?;
                    if read_size == 0 {
                        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Unexpected EOF when reading key"));
                    }
                    if &self.key_buf[..read_size] != &expected_key[..read_size] {
                        continue 'entry_loop;
                    }
                    expected_key = &expected_key[read_size..];
                }
            };
            return Ok(Some(KeyedEntry {
                key: self.key,
                entry,
            }));
        }
        Ok(None)
    }
}

struct BookHashTableHashScanner<'a, H, Section> {
    key_buf: [u8; 256],
    hasher_builder: &'a H,
    section_scanner: BookHashTableSectionScanner<Section>,
    hash: Hash,
}

impl<'a, H: SliceHasherBuilder, Section: Read + Seek + Clone> HashTableScanner for BookHashTableHashScanner<'a, H, Section> {
    fn next(&mut self) -> io::Result<Option<impl HashTableEntry + use<'a, H, Section>>> {
        while let Some(mut entry) = self.section_scanner.next()? {
            {
                let mut hasher = self.hasher_builder.build();
                let mut key_reader = entry.key()?;
                loop {
                    let read_size = key_reader.read(&mut self.key_buf)?;
                    if read_size == 0 {
                        break;
                    }
                    hasher.update(&self.key_buf[..read_size]);
                }
                let key_hash = hasher.finalize();
                if key_hash != self.hash {
                    continue;
                }
            };

            return Ok(Some(entry));
        }
        Ok(None)
    }
}

struct BookHashTableAllScanner<Section, Sections> {
    sections: Sections,
    section_scanner: Option<BookHashTableSectionScanner<Section>>,
}

impl<Section: Read + Seek + Clone, Sections: Iterator<Item = (Section, u64)>> HashTableScanner for BookHashTableAllScanner<Section, Sections> {
    fn next(&mut self) -> io::Result<Option<impl HashTableEntry + use<Section, Sections>>> {
        loop {
            if let Some(scanner) = &mut self.section_scanner {
                if let Some(entry) = scanner.next()? {
                    return Ok(Some(entry));
                }
                self.section_scanner = None;
            }
            if let Some((section, section_end)) = self.sections.next() {
                self.section_scanner = Some(BookHashTableSectionScanner {
                    section,
                    section_end,
                });
                continue;
            }
            return Ok(None);
        }
    }
}
