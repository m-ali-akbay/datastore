use std::{fs::create_dir_all, io::{self}, path::Path, sync::{Arc, RwLock}};

use crate::{book::{SectionIndex, pager::PagerBook}, dbms::{index_registry::ManagedIndexRegistry, page_registry::ManagedPageRegistry, section_registry::ManagedSectionRegistry}, hash_table::{self, HashTable, book::{BookHashTable, IndexChunkSize}, prefix_hasher::PrefixHasherBuilder}, pager::{PageSize, fs::FilePager}};

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

#[derive(serde::Serialize, serde::Deserialize)]
struct Header {
    #[serde(flatten)]
    config: HashTableConfig,
}

type TPager = FilePager;

type TPageRegistry = Arc<RwLock<ManagedPageRegistry>>;

type TBook = PagerBook<
    TPager,
    TPageRegistry,
>;

type TSectionRegistry = Arc<RwLock<ManagedSectionRegistry>>;
type TIndexRegistry = Arc<RwLock<ManagedIndexRegistry>>;

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
}

impl ManagedHashTable {
    pub fn open(dir_path: impl AsRef<Path>, config: HashTableConfig) -> io::Result<Self> {
        create_dir_all(&dir_path)?;

        let header_path = dir_path.as_ref().join("header.json");

        let header = if header_path.try_exists()? {
            let header_file = std::fs::OpenOptions::new()
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
            let header_file = std::fs::OpenOptions::new()
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

        let pages_path = dir_path.as_ref().join("pages.dat");

        let pages_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&pages_path)?;

        let pager = FilePager::new(pages_file, header.config.page_size)?;

        let page_registry_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&dir_path.as_ref().join("pages.reg"))?;
        let page_registry = Arc::new(RwLock::new(ManagedPageRegistry::load(page_registry_file)?));

        let section_registry_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&dir_path.as_ref().join("sections.reg"))?;
        let section_registry = Arc::new(RwLock::new(
            ManagedSectionRegistry::load(
                section_registry_file,
                header.config.section_count,
            )?
        ));

        let index_registry_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&dir_path.as_ref().join("indexes.reg"))?;
        let index_registry = Arc::new(RwLock::new(ManagedIndexRegistry::load(index_registry_file)?));

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

        Ok(ManagedHashTable {
            hash_table,
            pager,
            page_registry,
            section_registry,
            index_registry,
        })
    }
}

impl ManagedHashTable {
    pub fn save(&mut self) -> io::Result<()> {
        // TODO: Acquire locks in a consistent order to avoid deadlocks
        let mut page_registry = self.page_registry.write().map_err(|_| io::Error::new(io::ErrorKind::Other, "Lock poisoned"))?;
        let mut section_registry = self.section_registry.write().map_err(|_| io::Error::new(io::ErrorKind::Other, "Lock poisoned"))?;
        let mut index_registry = self.index_registry.write().map_err(|_| io::Error::new(io::ErrorKind::Other, "Lock poisoned"))?;

        // TODO: implement WAL
        self.pager.flush()?;
        page_registry.save()?;
        section_registry.save()?;
        index_registry.save()?;

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
