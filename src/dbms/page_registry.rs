use std::{cmp::Ordering, collections::BTreeMap, fs::File, io::{self, Read, Seek}, slice};

use crate::{book::pager::{PageHeader, PageKey, PageRegistry}, dbms::wal::WriteAheadLog, pager::PageIndex};

pub struct ManagedPageRegistry<WAL> {
    file: File,
    cache: Vec<PageKey>,
    map: BTreeMap<PageKey, PageIndex>,
    hot: Vec<(PageKey, PageIndex)>,
    wal: Option<WAL>,
}

#[derive(Clone, Debug)]
pub enum PageEvent {
    Assigned(PageKey, PageIndex),
}

impl PageEvent {
    pub fn read(reader: &mut impl Read) -> io::Result<Self> {
        let mut tag: u8 = 0;
        reader.read_exact(slice::from_mut(&mut tag))?;

        match tag {
            1 => {
                let key = read_page_key(reader)?;
                let mut index_buffer = [0u8; 4];
                reader.read_exact(&mut index_buffer)?;
                let pager_page_index = u32::from_le_bytes(index_buffer);
                Ok(PageEvent::Assigned(key, pager_page_index))
            }
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Unknown PageEvent type")),
        }
    }

    pub fn write(&self, writer: &mut impl io::Write) -> io::Result<()> {
        match self {
            PageEvent::Assigned(key, pager_page_index) => {
                writer.write_all(&[1u8])?;
                write_page_key(writer, key)?;
                writer.write_all(&pager_page_index.to_le_bytes())?;
            }
        }
        Ok(())
    }
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

impl<WAL> ManagedPageRegistry<WAL> {
    pub fn apply(&mut self, event: PageEvent) -> io::Result<()> {
        match event {
            PageEvent::Assigned(key, pager_page_index) => {
                match self.cache.len().cmp(&(pager_page_index as usize)) {
                    Ordering::Less => return Err(io::Error::new(io::ErrorKind::InvalidData, "Out of order page event")),
                    Ordering::Equal => self.cache.push(key.clone()),
                    Ordering::Greater => self.cache[pager_page_index as usize] = key.clone(),
                }
                self.map.insert(key.clone(), pager_page_index);
                self.hot.push((key, pager_page_index));
            }
        }
        Ok(())
    }

    pub fn with_wal(mut self, wal: WAL) -> Self {
        self.wal = Some(wal);
        self
    }

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
        Ok(Self { file, cache, map, hot: Vec::new(), wal: None })
    }

    pub fn save(&mut self) -> io::Result<()> {
        for (page_key, page_index) in self.hot.iter() {
            self.file.seek(io::SeekFrom::Start(*page_index as u64 * ENTRY_SIZE as u64))?;
            write_page_key(&mut self.file, page_key)?;
        }
        self.file.sync_all()?;
        self.hot.clear();
        Ok(())
    }
}

impl<WAL: WriteAheadLog<Event=PageEvent>> PageRegistry for ManagedPageRegistry<WAL> {
    fn try_resolve_page(&self, key: &PageKey) -> io::Result<Option<PageHeader>> {
        if let Some(&pager_page_index) = self.map.get(key) {
            Ok(Some(PageHeader {
                pager_page_index,
            }))
        } else {
            Ok(None)
        }
    }

    fn resolve_page(&mut self, key: &PageKey) -> io::Result<PageHeader> {
        if let Some(page_header) = self.try_resolve_page(key)? {
            return Ok(page_header);
        }
        if let Some(&pager_page_index) = self.map.get(key) {
            return Ok(PageHeader {
                pager_page_index,
            });
        }
        let pager_page_index = self.cache.len() as PageIndex;
        let event = PageEvent::Assigned(key.clone(), pager_page_index);
        self.wal.record(event.clone())?;
        self.apply(event)?;
        Ok(PageHeader {
            pager_page_index,
        })
    }
}
