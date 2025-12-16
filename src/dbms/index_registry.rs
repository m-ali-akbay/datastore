use core::slice;
use std::{cmp::Ordering, collections::{BTreeMap, BTreeSet}, fs::File, io::{self, Read, Seek}, ops::Bound, sync::{Arc, RwLock}};

use crate::{dbms::wal::{WALReader, WriteAheadLog}, hash_table::book::{IndexHeader, IndexKey, IndexRegistry}};

pub struct ManagedIndexRegistry<WAL> {
    file: File,
    cache: Vec<(IndexKey, IndexHeader)>,
    map: BTreeMap<IndexKey, usize>,
    hot: BTreeSet<usize>,
    wal: WAL,
}

#[derive(Clone, Debug)]
pub enum IndexEvent {
    Updated(u32, IndexKey, IndexHeader),
}

impl IndexEvent {
    pub fn read(reader: &mut impl Read) -> io::Result<Self> {
        let mut tag: u8 = 0;
        reader.read_exact(slice::from_mut(&mut tag))?;

        match tag {
            1 => {
                let mut cache_idx_buffer = [0u8; 4];
                reader.read_exact(&mut cache_idx_buffer)?;
                let cache_idx = u32::from_le_bytes(cache_idx_buffer);

                let key = read_index_key(reader)?;
                let header = read_index_header(reader)?;
                Ok(IndexEvent::Updated(cache_idx, key, header))
            }
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Unknown IndexEvent type")),
        }
    }

    pub fn write(&self, writer: &mut impl io::Write) -> io::Result<()> {
        match self {
            IndexEvent::Updated(cache_idx, key, header) => {
                writer.write_all(&[1u8])?;
                writer.write_all(&cache_idx.to_le_bytes())?;
                write_index_key(writer, key)?;
                write_index_header(writer, header)?;
            }
        }
        Ok(())
    }
}

const INDEX_KEY_SIZE: usize = 8;

fn read_index_key(reader: &mut impl Read) -> io::Result<IndexKey> {
    let mut buffer = [0u8; INDEX_KEY_SIZE];
    reader.read_exact(&mut buffer)?;

    let section_index = u32::from_le_bytes(buffer[0..4].try_into().unwrap());
    let index_chunk = u32::from_le_bytes(buffer[4..8].try_into().unwrap());

    Ok(IndexKey {
        section_index,
        index_chunk,
    })
}

fn write_index_key(writer: &mut impl io::Write, key: &IndexKey) -> io::Result<()> {
    writer.write_all(&key.section_index.to_le_bytes())?;
    writer.write_all(&key.index_chunk.to_le_bytes())?;
    Ok(())
}

const INDEX_HEADER_SIZE: usize = 16;

fn read_index_header(reader: &mut impl Read) -> io::Result<IndexHeader> {
    let mut buffer = [0u8; INDEX_HEADER_SIZE];
    reader.read_exact(&mut buffer)?;

    let bloom_filter = u64::from_le_bytes(buffer[0..8].try_into().unwrap());
    let first_entry_offset = u64::from_le_bytes(buffer[8..16].try_into().unwrap());

    Ok(IndexHeader {
        bloom_filter,
        first_entry_offset,
    })
}

fn write_index_header(writer: &mut impl io::Write, header: &IndexHeader) -> io::Result<()> {
    writer.write_all(&header.bloom_filter.to_le_bytes())?;
    writer.write_all(&header.first_entry_offset.to_le_bytes())?;
    Ok(())
}

const ENTRY_SIZE: usize = INDEX_KEY_SIZE + INDEX_HEADER_SIZE;

fn read_index_entry(reader: &mut impl Read) -> io::Result<(IndexKey, IndexHeader)> {
    let key = read_index_key(reader)?;
    let header = read_index_header(reader)?;
    Ok((key, header))
}

fn write_index_entry(writer: &mut impl io::Write, key: &IndexKey, header: &IndexHeader) -> io::Result<()> {
    write_index_key(writer, key)?;
    write_index_header(writer, header)?;
    Ok(())
}

impl<WAL> ManagedIndexRegistry<WAL> {
    fn apply(&mut self, event: IndexEvent) -> io::Result<()> {
        match event {
            IndexEvent::Updated(cache_idx, key, header) => {
                match self.cache.len().cmp(&(cache_idx as usize)) {
                    Ordering::Less => return Err(io::Error::new(io::ErrorKind::InvalidData, "Out of order index event")),
                    Ordering::Equal => self.cache.push((key.clone(), header)),
                    Ordering::Greater => self.cache[cache_idx as usize] = (key.clone(), header),
                }
                self.map.insert(key.clone(), cache_idx as usize);
                self.hot.insert(cache_idx as usize);
            },
        }
        Ok(())
    }

    pub fn load(mut file: File, mut old_wal: impl WALReader<Event=IndexEvent>, new_wal: WAL) -> io::Result<Self> {
        let count = file.metadata()?.len() as usize / ENTRY_SIZE;
        file.seek(io::SeekFrom::Start(0))?;
        let cache = (0..count)
            .map(|_| read_index_entry(&mut file))
            .collect::<io::Result<Vec<_>>>()?;
        let map = cache
            .iter()
            .enumerate()
            .map(|(i, (key, _))| (key.clone(), i))
            .collect();
        let mut registry = Self { file, cache, map, hot: BTreeSet::new(), wal: new_wal };
        while let Some(event) = old_wal.read_next()? {
            registry.apply(event)?;
        }
        Ok(registry)
    }

    pub fn save(&mut self) -> io::Result<()> {
        for cache_idx in self.hot.iter() {
            let (key, header) = &self.cache[*cache_idx];
            self.file.seek(io::SeekFrom::Start(*cache_idx as u64 * ENTRY_SIZE as u64))?;
            write_index_entry(&mut self.file, key, header)?;
        }
        self.file.sync_all()?;
        self.hot.clear();
        Ok(())
    }
}

// TODO: make IndexKey and IndexHeader assigned types for further optimization on resolve methods
impl<WAL: WriteAheadLog<Event=IndexEvent>> IndexRegistry for Arc<RwLock<ManagedIndexRegistry<WAL>>> {
    fn try_resolve_index(&self, index_key: &IndexKey) -> io::Result<Option<IndexHeader>> {
        let lock = self.read().map_err(|_| io::Error::new(io::ErrorKind::Other, "Lock poisoned"))?;
        if let Some(&index) = lock.map.get(index_key) {
            let (_, header) = &lock.cache[index];
            Ok(Some(header.clone()))
        } else {
            Ok(None)
        }
    }

    fn try_resolve_next_index(&self, index_key: &IndexKey) -> io::Result<Option<IndexHeader>> {
        let lock = self.read().map_err(|_| io::Error::new(io::ErrorKind::Other, "Lock poisoned"))?;
        let Some(index) = lock.map.range((Bound::Excluded(index_key.clone()), Bound::Excluded(IndexKey { 
            section_index: index_key.section_index + 1, 
            index_chunk: 0,
        })))
            .next() else {
            return Ok(None);
        };
        let (_, header) = &lock.cache[*index.1];
        Ok(Some(header.clone()))
    }

    fn update_index_bloom_filter(&mut self, index_key: &IndexKey, entry_offset: u64, bloom_bit: u64) -> io::Result<()> {
        let mut lock = self.write().map_err(|_| io::Error::new(io::ErrorKind::Other, "Lock poisoned"))?;
        let event = if let Some(&cache_idx) = lock.map.get(index_key) {
            let header = &mut lock.cache[cache_idx].1;
            let old_bloom_filter = header.bloom_filter;
            let new_bloom_filter = old_bloom_filter | bloom_bit;
            if new_bloom_filter == old_bloom_filter {
                return Ok(());
            }
            let mut index_header = header.clone();
            index_header.bloom_filter = new_bloom_filter;
            IndexEvent::Updated(cache_idx as u32, index_key.clone(), index_header)
        } else {
            let cache_idx = lock.cache.len();
            let index_header = IndexHeader {
                bloom_filter: bloom_bit,
                first_entry_offset: entry_offset,
            };
            IndexEvent::Updated(cache_idx as u32, index_key.clone(), index_header)
        };
        lock.wal.record(event.clone())?;
        lock.apply(event)?;
        Ok(())
    }
}
