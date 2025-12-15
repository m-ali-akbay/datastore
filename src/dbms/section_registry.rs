use std::{fs::File, io::{self, Read, Seek, Write}, sync::{Arc, RwLock}};

use crate::{book::{SectionIndex}, hash_table::book::{SectionHeader, SectionRegistry}};

pub struct ManagedSectionRegistry {
    file: File,
    cache: Vec<SectionHeader>,
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

impl ManagedSectionRegistry {
    pub fn load(mut file: File, section_count: SectionIndex) -> io::Result<Self> {
        let size = section_count as u64 * ENTRY_SIZE as u64;
        file.set_len(size)?;

        file.seek(io::SeekFrom::Start(0))?;
        let cache = (0..section_count)
            .map(|_| read_section_header(&mut file))
            .collect::<io::Result<Vec<_>>>()?;
        Ok(Self { file, cache })
    }

    pub fn save(&mut self) -> io::Result<()> {
        self.file.seek(io::SeekFrom::Start(0))?;
        for header in &self.cache {
            write_section_header(&mut self.file, header)?;
        }
        self.file.flush()?;
        Ok(())
    }
}

impl SectionRegistry for Arc<RwLock<ManagedSectionRegistry>> {
    fn resolve_section(&self, section_index: SectionIndex) -> io::Result<SectionHeader> {
        let registry = self.read().map_err(|_| io::Error::new(io::ErrorKind::Other, "Lock poisoned"))?;
        registry.cache.get(section_index as usize)
            .cloned()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Section not found"))
    }

    fn update_section_end_offset(&mut self, section_index: SectionIndex, end_offset: u64) -> io::Result<()> {
        let mut registry = self.write().map_err(|_| io::Error::new(io::ErrorKind::Other, "Lock poisoned"))?;
        if let Some(header) = registry.cache.get_mut(section_index as usize) {
            header.end_offset = header.end_offset.max(end_offset);
            Ok(())
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, "Section not found"))
        }
    }
}
