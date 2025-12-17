use core::slice;
use std::{fs::{self, create_dir_all}, io::{self}, path::Path};

use crate::{dbms::{index_registry::IndexEvent, section_registry::SectionEvent, wal::{ConvertWAL, FileWAL, FileWALReader, SerializableEvent, WALReader}}, pager::{PageSize, fs::FilePager}};
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
type TPageRegistry = ManagedPageRegistry<TPageRegistryWal>;

type TBook = PagerBook<
    TPager,
    TPageRegistry,
>;

type TSectionRegistryWal = ConvertWAL<SectionEvent, TWAL>;
type TSectionRegistry = ManagedSectionRegistry<TSectionRegistryWal>;

type TIndexRegistryWal = ConvertWAL<IndexEvent, TWAL>;
type TIndexRegistry = ManagedIndexRegistry<TIndexRegistryWal>;

type THashTable = BookHashTable<
    PrefixHasherBuilder,
    TBook,
    TSectionRegistry,
    TIndexRegistry,
>;

/// ## Guarantees:
/// - All operations are persisted on disk as soon as and only if `sync` is called.
/// - Duration of `sync` is independent of size of entries BUT their count.
/// - `scan`s using `HashTableScanFilter::Key` will iterate over entries in the order of inserts.
/// - `insert` operations are O(1) on average, and duration depends on the size of the entry being inserted.
pub struct ManagedHashTable {
    hash_table: THashTable,
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
        let mut page_registry = ManagedPageRegistry::load(
            page_registry_file,
        )?;

        let section_registry_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&dir_path.as_ref().join("sections.reg"))?;
        let mut section_registry = ManagedSectionRegistry::load(
            section_registry_file,
            header.config.section_count,
        )?;

        let index_registry_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&dir_path.as_ref().join("indexes.reg"))?;
        let mut index_registry = ManagedIndexRegistry::load(
            index_registry_file,
        )?;

        let mut wal_reader = FileWALReader::<HashTableEvent>::new(wal_file)?;
        while let Some(event) = wal_reader.read_next()? {
            match event {
                HashTableEvent::PageEvent(page_event) => page_registry.apply(page_event)?,
                HashTableEvent::SectionEvent(section_event) => section_registry.apply(section_event)?,
                HashTableEvent::IndexEvent(index_event) => index_registry.apply(index_event)?,
            }
        }

        let wal = FileWAL::load(wal_reader.into_file())?;
        let page_registry = ManagedPageRegistry::with_wal(page_registry, ConvertWAL::new(wal.clone()));
        let section_registry = ManagedSectionRegistry::with_wal(section_registry, ConvertWAL::new(wal.clone()));
        let index_registry = ManagedIndexRegistry::with_wal(index_registry, ConvertWAL::new(wal.clone()));

        let book = PagerBook::new(
            pager,
            page_registry,
        );

        let hash_table = BookHashTable::new(
            PrefixHasherBuilder,
            book,
            header.config.section_count,
            section_registry,
            header.config.index_chunk_size,
            index_registry,
        );

        let mut managed = ManagedHashTable {
            hash_table,
            wal,
        };

        managed.full_sync()?;

        Ok(managed)
    }
}

impl ManagedHashTable {
    pub fn sync(&mut self) -> io::Result<()> {
        self.hash_table.book().pager().sync()?;
        self.wal.sync()?;

        Ok(())
    }

    pub fn full_sync(&mut self) -> io::Result<()> {
        self.sync()?;

        // TODO: Acquire locks in a consistent order to avoid deadlocks

        self.hash_table.book().registry()?.save()?;

        self.hash_table.section_registry().save()?;

        self.hash_table.index_registry().save()?;

        self.wal.clear()?;

        Ok(())
    }
}

impl HashTable for ManagedHashTable {
    fn insert(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.hash_table.insert(key, value)
    }

    fn scan<'a>(&'a self, filter: hash_table::HashTableScanFilter<'a>) -> io::Result<impl hash_table::HashTableScanner + 'a> {
        self.hash_table.scan(filter)
    }
}
