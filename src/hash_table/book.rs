use std::{cmp::Ordering, io::{self, Read, Seek, SeekFrom, Write}, mem::replace};

use crate::{book::{Book, SectionIndex}, hash_table::{HashTable, HashTableEntry, HashTableScanner, SliceHasher, SliceHasherBuilder}};

use super::HashTableScanFilter;

#[derive(Clone, Debug)]
pub struct SectionHeader {
    pub end_offset: u64,
}

pub trait SectionRegistry {
    fn resolve_section(&self, section_index: SectionIndex) -> io::Result<SectionHeader>;
    fn update_section_end_offset(&mut self, section_index: SectionIndex, end_offset: u64) -> io::Result<()>;
}

pub type IndexChunk = u32;
pub type IndexChunkSize = u32;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct IndexKey {
    pub section_index: SectionIndex,
    pub index_chunk: IndexChunk,
}

#[derive(Clone, Copy, Debug)]
pub struct IndexHeader {
    pub bloom_filter: u64,
    pub first_entry_offset: u64,
}

pub trait IndexRegistry {
    fn try_resolve_index(&self, index_key: &IndexKey) -> io::Result<Option<IndexHeader>>;
    fn try_resolve_next_index(&self, index_key: &IndexKey) -> io::Result<Option<IndexHeader>>;
    fn update_index_bloom_filter(&mut self, index_key: &IndexKey, entry_offset: u64, bloom_bit: u64) -> io::Result<()>;
}

pub struct BookHashTable<H, B, SR, IR> {
    hasher_builder: H,
    book: B,
    section_count: SectionIndex,
    section_registry: SR,
    index_chunk_size: IndexChunkSize,
    index_registry: IR,
}

impl<H: SliceHasherBuilder, B: Book, SR: SectionRegistry, IR: IndexRegistry> BookHashTable<H, B, SR, IR> {
    pub fn new(
        hasher_builder: H,
        book: B,
        section_count: SectionIndex,
        section_registry: SR,
        index_chunk_size: IndexChunkSize,
        index_registry: IR,
    ) -> Self {
        Self {
            hasher_builder,
            book,
            section_count,
            section_registry,
            index_chunk_size,
            index_registry,
        }
    }
}

impl<H: SliceHasherBuilder, B: Book, SR: SectionRegistry, IR: IndexRegistry + Clone> HashTable for BookHashTable<H, B, SR, IR> {
    fn insert(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        let mut hasher = self.hasher_builder.build();
        hasher.update(key);
        let hash = hasher.finalize();

        let section_index = hash % self.section_count;
        let bloom_index = (hash / self.section_count) as u64 % 64;
        let bloom_bit = 1u64 << bloom_index;

        let mut section = self.book.section(section_index);
        let section_header = self.section_registry.resolve_section(section_index)?;

        let index_chunk = (section_header.end_offset / self.index_chunk_size as u64) as IndexChunk;
        let index_key = IndexKey {
            section_index,
            index_chunk,
        };

        let entry_offset = section_header.end_offset;
        section.seek(SeekFrom::Start(entry_offset))?;

        let key_size = key.len() as u32;
        let value_size = value.len() as u32;
        section.write_all(&key_size.to_le_bytes())?;
        section.write_all(&value_size.to_le_bytes())?;
        section.write_all(key)?;
        section.write_all(value)?;
    
        let new_end = section.stream_position()?;
        self.section_registry.update_section_end_offset(section_index, new_end)?;

        self.index_registry.update_index_bloom_filter(&index_key, entry_offset, bloom_bit)?;

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
                match self.section_registry.resolve_section(index)? {
                    SectionHeader {
                        end_offset
                    } if end_offset > 0 => SectionScannerIterator::Single(SectionScanner {
                        section: self.book.section(index),
                        section_index: index,
                        section_end: end_offset,
                        bloom_query,
                        index_chunk: None,
                        index_chunk_size: self.index_chunk_size,
                        index_registry: self.index_registry.clone(),
                    }),
                    _ => SectionScannerIterator::None,
                }
            },
            None => SectionScannerIterator::Many(
                // TODO: optimize this by supporting iterating non-empty sections only
                (0..self.section_count)
                    .map(|section_index| (section_index, self.section_registry.resolve_section(section_index)))
                    .map(move |(section_index, section_header)| -> io::Result<SectionScanner<B::Section, IR>> {
                        let section_header = section_header?;
                        Ok(SectionScanner {
                            section: self.book.section(section_index),
                            section_index,
                            section_end: section_header.end_offset,
                            bloom_query,
                            index_chunk: None,
                            index_chunk_size: self.index_chunk_size,
                            index_registry: self.index_registry.clone(),
                        })
                    })
            ),
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

struct MultiSectionScanner<IR, Section, I: Iterator<Item = io::Result<SectionScanner<Section, IR>>>> {
    scanners: I,
    current_scanner: Option<SectionScanner<Section, IR>>,
}

impl<IR: IndexRegistry + Clone, Section: Read + Seek + Clone, I: Iterator<Item = io::Result<SectionScanner<Section, IR>>>> HashTableScanner for MultiSectionScanner<IR, Section, I> {
    fn next(&mut self) -> io::Result<Option<impl HashTableEntry + use<IR, Section, I>>> {
        loop {
            if let Some(scanner) = &mut self.current_scanner {
                if let Some(entry) = scanner.next()? {
                    return Ok(Some(entry));
                }
                self.current_scanner = None;
            }
            if let Some(next_scanner) = self.scanners.next() {
                self.current_scanner = Some(next_scanner?);
                continue;
            }
            return Ok(None);
        }
    }
}

enum SectionScannerIterator<Section, IR, I: Iterator<Item = io::Result<SectionScanner<Section, IR>>>> {
    Single(SectionScanner<Section, IR>),
    None,
    Many(I),
}

impl<IR, Section, I: Iterator<Item = io::Result<SectionScanner<Section, IR>>>> Iterator for SectionScannerIterator<Section, IR, I> {
    type Item = io::Result<SectionScanner<Section, IR>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            SectionScannerIterator::Single(..) => {
                let section_scanner = replace(self, SectionScannerIterator::None);
                if let SectionScannerIterator::Single(section_scanner) = section_scanner {
                    Some(Ok(section_scanner))
                } else {
                    None
                }
            },
            SectionScannerIterator::Many(iter) => iter.next(),
            SectionScannerIterator::None => None,
        }
    }
}

struct SectionScanner<Section, IR> {
    section: Section,
    section_index: SectionIndex,
    section_end: u64,
    bloom_query: Option<u64>,
    index_chunk: Option<(IndexKey, IndexHeader)>,
    index_chunk_size: IndexChunkSize,
    index_registry: IR,
}

struct ScannerEntry<Reader: Read + Seek + Clone> {
    reader: Reader,
    key_size: u32,
    value_size: u32,
}

impl<Reader: Read + Seek + Clone, IR: IndexRegistry> SectionScanner<Reader, IR> {
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
                    self.index_chunk = self.index_registry.try_resolve_index(&index_key)?.map(|ih| (index_key, ih));
                },
            }
            let Some((_, index_header)) = &self.index_chunk else {
                return Ok(None);
            };
            if (index_header.bloom_filter & bloom_query) == 0 {
                let next_index_header = self.index_registry.try_resolve_next_index(&index_key)?;
                let next_position = match next_index_header {
                    Some(IndexHeader { first_entry_offset, .. }) => first_entry_offset,
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
