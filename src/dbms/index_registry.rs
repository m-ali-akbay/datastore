use std::{collections::BTreeMap, fs::File, io::{self, Read, Seek, Write}, ops::Bound, sync::{Arc, RwLock}};

use crate::hash_table::book::{IndexHeader, IndexKey, IndexRegistry};

pub struct ManagedIndexRegistry {
    file: File,
    cache: Vec<(IndexKey, IndexHeader)>,
    map: BTreeMap<IndexKey, usize>,
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

impl ManagedIndexRegistry {
    pub fn load(mut file: File) -> io::Result<Self> {
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
        Ok(Self { file, cache, map })
    }

    pub fn save(&mut self) -> io::Result<()> {
        let size = (self.cache.len() * ENTRY_SIZE) as u64;
        self.file.set_len(size)?;
        self.file.seek(io::SeekFrom::Start(0))?;
        for (key, header) in &self.cache {
            write_index_entry(&mut self.file, key, header)?;
        }
        self.file.flush()?;
        Ok(())
    }
}

// TODO: make IndexKey and IndexHeader owned types for further optimization on resolve methods
impl IndexRegistry for Arc<RwLock<ManagedIndexRegistry>> {
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
        if let Some(&index) = lock.map.get(index_key) {
            let header = &mut lock.cache[index].1;
            header.bloom_filter |= bloom_bit;
            Ok(())
        } else {
            let index = lock.cache.len();
            let index_header = IndexHeader {
                bloom_filter: bloom_bit,
                first_entry_offset: entry_offset,
            };
            lock.cache.push((index_key.clone(), index_header));
            lock.map.insert(index_key.clone(), index);
            Ok(())
        }
    }
}
