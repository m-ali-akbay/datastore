use std::{collections::BTreeMap, fs::File, io::{self, Read, Seek, Write}, sync::{Arc, RwLock}};

use crate::{book::pager::{PageHeader, PageKey, PageRegistry}, pager::PageIndex};

pub struct ManagedPageRegistry {
    file: File,
    cache: Vec<PageKey>,
    map: BTreeMap<PageKey, PageIndex>,
}

const ENTRY_SIZE: usize = 8;

fn read_page_key(reader: &mut impl Read) -> io::Result<PageKey> {
    let mut buffer = [0u8; ENTRY_SIZE];
    reader.read_exact(&mut buffer)?;

    let section_index = u32::from_le_bytes(buffer[0..4].try_into().unwrap());
    let section_page_index = u32::from_le_bytes(buffer[4..8].try_into().unwrap());

    Ok(PageKey {
        section_index,
        section_page_index,
    })
}

fn write_page_key(writer: &mut impl io::Write, key: &PageKey) -> io::Result<()> {
    writer.write_all(&key.section_index.to_le_bytes())?;
    writer.write_all(&key.section_page_index.to_le_bytes())?;
    Ok(())
}

impl ManagedPageRegistry {
    pub fn load(mut file: File) -> io::Result<Self> {
        let count = file.metadata()?.len() as usize / ENTRY_SIZE;
        file.seek(io::SeekFrom::Start(0))?;
        let cache = (0..count)
            .map(|_| read_page_key(&mut file))
            .collect::<io::Result<Vec<_>>>()?;
        let map = cache
            .iter()
            .enumerate()
            .map(|(i, key)| (key.clone(), i as PageIndex))
            .collect();
        Ok(Self { file, cache, map })
    }

    pub fn save(&mut self) -> io::Result<()> {
        let size = (self.cache.len() * ENTRY_SIZE) as u64;
        self.file.set_len(size)?;
        self.file.seek(io::SeekFrom::Start(0))?;
        for key in &self.cache {
            write_page_key(&mut self.file, key)?;
        }
        self.file.flush()?;
        Ok(())
    }
}

impl PageRegistry for Arc<RwLock<ManagedPageRegistry>> {
    fn try_resolve_page(&self, key: &PageKey) -> io::Result<Option<PageHeader>> {
        let lock = self.read().map_err(|_| io::Error::new(io::ErrorKind::Other, "Lock poisoned"))?;
        if let Some(&pager_page_index) = lock.map.get(key) {
            Ok(Some(PageHeader {
                pager_page_index,
            }))
        } else {
            Ok(None)
        }
    }

    fn resolve_page(&self, key: &PageKey) -> io::Result<PageHeader> {
        if let Some(page_header) = self.try_resolve_page(key)? {
            return Ok(page_header);
        }
        let mut lock = self.write().map_err(|_| io::Error::new(io::ErrorKind::Other, "Lock poisoned"))?;
        if let Some(&pager_page_index) = lock.map.get(key) {
            return Ok(PageHeader {
                pager_page_index,
            });
        }
        let pager_page_index = lock.cache.len() as PageIndex;
        lock.cache.push(key.clone());
        lock.map.insert(key.clone(), pager_page_index);
        Ok(PageHeader {
            pager_page_index,
        })
    }
}
