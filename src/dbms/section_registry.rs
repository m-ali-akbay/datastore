use core::slice;
use std::{collections::{BTreeSet}, fs::File, io::{self, Read, Seek}, sync::{Arc, RwLock}};

use crate::{book::SectionIndex, dbms::wal::{WALReader, WriteAheadLog}, hash_table::book::{SectionHeader, SectionRegistry}};

pub struct ManagedSectionRegistry<WAL> {
    file: File,
    cache: Vec<SectionHeader>,
    hot: BTreeSet<SectionIndex>,
    wal: WAL,
}

#[derive(Clone, Debug)]
pub enum SectionEvent {
    Updated(SectionIndex, SectionHeader),
}

impl SectionEvent {
    pub fn read(reader: &mut impl Read) -> io::Result<Self> {
        let mut tag: u8 = 0;
        reader.read_exact(slice::from_mut(&mut tag))?;

        match tag {
            1 => {
                let mut index_buffer = [0u8; 4];
                reader.read_exact(&mut index_buffer)?;
                let section_index = u32::from_le_bytes(index_buffer);
                let header = read_section_header(reader)?;
                Ok(SectionEvent::Updated(section_index, header))
            }
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Unknown SectionEvent type")),
        }
    }

    pub fn write(&self, writer: &mut impl io::Write) -> io::Result<()> {
        match self {
            SectionEvent::Updated(section_index, header) => {
                writer.write_all(&[1u8])?;
                writer.write_all(&section_index.to_le_bytes())?;
                write_section_header(writer, header)?;
            },
        }
        Ok(())
    }
}

const ENTRY_SIZE: usize = 8;

fn read_section_header(reader: &mut impl Read) -> io::Result<SectionHeader> {
    let mut buffer = [0u8; ENTRY_SIZE];
    reader.read_exact(&mut buffer)?;

    let end_offset = u64::from_le_bytes(buffer[0..8].try_into().unwrap());

    Ok(SectionHeader {
        end_offset,
    })
}

fn write_section_header(writer: &mut impl io::Write, header: &SectionHeader) -> io::Result<()> {
    writer.write_all(&header.end_offset.to_le_bytes())?;
    Ok(())
}

impl<WAL> ManagedSectionRegistry<WAL> {
    fn apply(&mut self, event: SectionEvent) -> io::Result<()> {
        match event {
            SectionEvent::Updated(section_index, header) => {
                if self.cache.len() <= section_index as usize {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "Section index out of bounds"));
                }
                self.cache[section_index as usize] = header.clone();
                self.hot.insert(section_index);
            }
        }
        Ok(())
    }

    pub fn load(mut file: File, section_count: SectionIndex, mut old_wal: impl WALReader<Event=SectionEvent>, new_wal: WAL) -> io::Result<Self> {
        let size = section_count as u64 * ENTRY_SIZE as u64;
        file.set_len(size)?;

        file.seek(io::SeekFrom::Start(0))?;
        let cache = (0..section_count)
            .map(|_| read_section_header(&mut file))
            .collect::<io::Result<Vec<_>>>()?;
        let mut registry = Self { file, cache, hot: BTreeSet::new(), wal: new_wal };
        while let Some(event) = old_wal.read_next()? {
            registry.apply(event)?;
        }
        Ok(registry)
    }

    pub fn save(&mut self) -> io::Result<()> {
        for &section_index in self.hot.iter() {
            let header = &self.cache[section_index as usize];
            self.file.seek(io::SeekFrom::Start(section_index as u64 * ENTRY_SIZE as u64))?;
            write_section_header(&mut self.file, header)?;
        }
        self.file.sync_all()?;
        self.hot.clear();
        Ok(())
    }
}

impl<WAL: WriteAheadLog<Event=SectionEvent>> SectionRegistry for Arc<RwLock<ManagedSectionRegistry<WAL>>> {
    fn resolve_section(&self, section_index: SectionIndex) -> io::Result<SectionHeader> {
        let registry = self.read().map_err(|_| io::Error::new(io::ErrorKind::Other, "Lock poisoned"))?;
        registry.cache.get(section_index as usize)
            .cloned()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Section not found"))
    }

    fn update_section_end_offset(&mut self, section_index: SectionIndex, end_offset: u64) -> io::Result<()> {
        let mut registry = self.write().map_err(|_| io::Error::new(io::ErrorKind::Other, "Lock poisoned"))?;
        let event = if let Some(header) = registry.cache.get_mut(section_index as usize) {
            if header.end_offset >= end_offset {
                return Ok(());
            }
            let mut header = header.clone();
            header.end_offset = end_offset;
            SectionEvent::Updated(section_index, header)
        } else {
            return Err(io::Error::new(io::ErrorKind::NotFound, "Section not found"));
        };
        registry.wal.record(event.clone())?;
        registry.apply(event)
    }
}
