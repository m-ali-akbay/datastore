use core::slice;
use std::{fs::{self, create_dir_all}, io::{self}, path::Path, sync::{Arc, RwLock}};

use crate::{dbms::{index_registry::IndexEvent, section_registry::SectionEvent, wal::{ConvertWAL, FileWAL, FileWALReader, FilterMapWALReader, SerializableEvent}}, pager::{PageSize, fs::FilePager}};
use crate::hash_table::{self, HashTable, book::{BookHashTable, IndexChunkSize}, prefix_hasher::PrefixHasherBuilder};
use crate::dbms::{index_registry::ManagedIndexRegistry, page_registry::{ManagedPageRegistry, PageEvent}, section_registry::ManagedSectionRegistry};
use crate::book::{SectionIndex, pager::PagerBook};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct HashTableConfig {
    pub page_size: PageSize,
    pub section_count: SectionIndex,
    pub index_chunk_size: IndexChunkSize,
}

impl Default for HashTableConfig {
    fn default() -> Self {
        HashTableConfig {
            page_size: 4096,
            section_count: 1024,
            index_chunk_size: 4096,
        }
    }
}

#[derive(Clone, Debug)]
enum HashTableEvent {
    PageEvent(PageEvent),
    SectionEvent(SectionEvent),
    IndexEvent(IndexEvent),
}

impl From<PageEvent> for HashTableEvent {
    fn from(event: PageEvent) -> Self {
        HashTableEvent::PageEvent(event)
    }
}

impl Into<PageEvent> for HashTableEvent {
    fn into(self) -> PageEvent {
        match self {
            HashTableEvent::PageEvent(event) => event,
            _ => panic!("Not a PageEvent"),
        }
    }
}

impl From<SectionEvent> for HashTableEvent {
    fn from(event: SectionEvent) -> Self {
        HashTableEvent::SectionEvent(event)
    }
}

impl Into<SectionEvent> for HashTableEvent {
    fn into(self) -> SectionEvent {
        match self {
            HashTableEvent::SectionEvent(event) => event,
            _ => panic!("Not a SectionEvent"),
        }
    }
}

impl From<IndexEvent> for HashTableEvent {
    fn from(event: IndexEvent) -> Self {
        HashTableEvent::IndexEvent(event)
    }
}

impl Into<IndexEvent> for HashTableEvent {
    fn into(self) -> IndexEvent {
        match self {
            HashTableEvent::IndexEvent(event) => event,
            _ => panic!("Not an IndexEvent"),
        }
    }
}

impl SerializableEvent for HashTableEvent {
    fn read(reader: &mut impl io::Read) -> io::Result<Self> {
        let mut tag: u8 = 0;
        reader.read_exact(slice::from_mut(&mut tag))?;

        match tag {
            1 => {
                let event = PageEvent::read(reader)?;
                Ok(HashTableEvent::PageEvent(event))
            }
            2 => {
                let event = SectionEvent::read(reader)?;
                Ok(HashTableEvent::SectionEvent(event))
            }
            3 => {
                let event = IndexEvent::read(reader)?;
                Ok(HashTableEvent::IndexEvent(event))
            }
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Unknown HashTableEvent type")),
        }
    }

    fn write(&self, writer: &mut impl io::Write) -> io::Result<()> {
        match self {
            HashTableEvent::PageEvent(event) => {
                writer.write_all(&[1u8])?;
                event.write(writer)?;
            },
            HashTableEvent::SectionEvent(event) => {
                writer.write_all(&[2u8])?;
                event.write(writer)?;
            },
            HashTableEvent::IndexEvent(event) => {
                writer.write_all(&[3u8])?;
                event.write(writer)?;
            },
        }
        Ok(())
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Header {
    #[serde(flatten)]
    config: HashTableConfig,
}

type TWAL = FileWAL<HashTableEvent>;

type TPager = FilePager;

type TPageRegistryWal = ConvertWAL<PageEvent, TWAL>;
type TPageRegistry = Arc<RwLock<ManagedPageRegistry<TPageRegistryWal>>>;

type TBook = PagerBook<
    TPager,
    TPageRegistry,
>;

type TSectionRegistryWal = ConvertWAL<SectionEvent, TWAL>;
type TSectionRegistry = Arc<RwLock<ManagedSectionRegistry<TSectionRegistryWal>>>;

type TIndexRegistryWal = ConvertWAL<IndexEvent, TWAL>;
type TIndexRegistry = Arc<RwLock<ManagedIndexRegistry<TIndexRegistryWal>>>;

type THashTable = BookHashTable<
    PrefixHasherBuilder,
    TBook,
    TSectionRegistry,
    TIndexRegistry,
>;

pub struct ManagedHashTable {
    hash_table: THashTable,
    pager: TPager,
    page_registry: TPageRegistry,
    section_registry: TSectionRegistry,
    index_registry: TIndexRegistry,
    wal: TWAL,
}

impl ManagedHashTable {
    pub fn open(dir_path: impl AsRef<Path>, config: HashTableConfig) -> io::Result<Self> {
        create_dir_all(&dir_path)?;

        let header_path = dir_path.as_ref().join("header.json");

        let header = if header_path.try_exists()? {
            let header_file = fs::OpenOptions::new()
                .read(true)
                .open(&header_path)?;
            let header: Header = serde_json::from_reader(&header_file)
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, format!("Failed to parse metadata: {}", err)))?;

            if header.config.page_size != config.page_size {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "Page size in metadata does not match the provided configuration"));
            }

            if header.config.section_count != config.section_count {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "Section count in metadata does not match the provided configuration"));
            }

            if header.config.index_chunk_size != config.index_chunk_size {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "Index chunk size in metadata does not match the provided configuration"));
            }

            header
        } else {
            let header_file = fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(&header_path)?;
            let header = Header {
                config,
            };

            serde_json::to_writer_pretty(&header_file, &header)
                .map_err(|err| io::Error::new(io::ErrorKind::Other, format!("Failed to write metadata: {}", err)))?;

            header
        };

        let wal_path = dir_path.as_ref().join("events.log");
        let wal_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&wal_path)?;
        // TODO: avoid cloning the file handle
        let wal = FileWAL::load(wal_file.try_clone()?)?;

        let pages_path = dir_path.as_ref().join("pages.dat");
        let pages_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&pages_path)?;
        let pager = FilePager::new(pages_file, header.config.page_size)?;

        let page_registry_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&dir_path.as_ref().join("pages.reg"))?;
        let page_registry = Arc::new(RwLock::new(ManagedPageRegistry::load(
            page_registry_file,
            // TODO: implement MUX WAL reader and writer
            FilterMapWALReader::new(FileWALReader::new(wal_file.try_clone()?)?, |event| {
                match event {
                    HashTableEvent::PageEvent(page_event) => Some(page_event),
                    _ => None,
                }
            }),
            TPageRegistryWal::new(wal.clone()),
        )?));

        let section_registry_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&dir_path.as_ref().join("sections.reg"))?;
        let section_registry = Arc::new(RwLock::new(
            ManagedSectionRegistry::load(
                section_registry_file,
                header.config.section_count,
                FilterMapWALReader::new(FileWALReader::new(wal_file.try_clone()?)?, |event| {
                    match event {
                        HashTableEvent::SectionEvent(section_event) => Some(section_event),
                        _ => None,
                    }
                }),
                TSectionRegistryWal::new(wal.clone()),
            )?
        ));

        let index_registry_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&dir_path.as_ref().join("indexes.reg"))?;
        let index_registry = Arc::new(RwLock::new(
            ManagedIndexRegistry::load(
                index_registry_file,
                FilterMapWALReader::new(FileWALReader::new(wal_file.try_clone()?)?, |event| {
                    match event {
                        HashTableEvent::IndexEvent(index_event) => Some(index_event),
                        _ => None,
                    }
                }),
                TIndexRegistryWal::new(wal.clone()),
            )?
        ));

        let book = PagerBook::new(
            pager.clone(),
            page_registry.clone(),
        );

        let hash_table = BookHashTable::new(
            PrefixHasherBuilder,
            book,
            header.config.section_count,
            section_registry.clone(),
            header.config.index_chunk_size,
            index_registry.clone(),
        );

        let mut managed = ManagedHashTable {
            hash_table,
            pager,
            page_registry,
            section_registry,
            index_registry,
            wal,
        };

        managed.full_sync()?;

        Ok(managed)
    }
}

impl ManagedHashTable {
    pub fn quick_sync(&mut self) -> io::Result<()> {
        self.pager.sync()?;
        self.wal.sync()?;

        Ok(())
    }

    pub fn full_sync(&mut self) -> io::Result<()> {
        self.quick_sync()?;

        // TODO: Acquire locks in a consistent order to avoid deadlocks
        let mut page_registry = self.page_registry.write().map_err(|_| io::Error::new(io::ErrorKind::Other, "Lock poisoned"))?;
        let mut section_registry = self.section_registry.write().map_err(|_| io::Error::new(io::ErrorKind::Other, "Lock poisoned"))?;
        let mut index_registry = self.index_registry.write().map_err(|_| io::Error::new(io::ErrorKind::Other, "Lock poisoned"))?;

        page_registry.save()?;
        section_registry.save()?;
        index_registry.save()?;

        // TODO: Keeping the file size unchanged would make it more optimal to append new events later on as no allocation is needed
        self.wal.clear()?;

        Ok(())
    }
}

impl HashTable for ManagedHashTable {
    fn insert(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.hash_table.insert(key, value)
    }

    fn scan(&self, filter: hash_table::HashTableScanFilter) -> io::Result<impl hash_table::HashTableScanner> {
        self.hash_table.scan(filter)
    }
}
