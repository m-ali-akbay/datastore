use std::{fs::create_dir_all, io::Read, path::Path, sync::Arc};

use serde::{Deserialize, Serialize};

use crate::{block::{fs::FileBlockStorage, range::{RangeBlockStorage, RangeBlockStorageError}}, heap::FastHeapStorage, keymap::{HeapKeyMap, KeyMap, KeyMapEntryReader, KeyMapError, KeyMapIterator}, page::{FastPageStorage, OCCUPIED_SIZE_BYTES, PageStorageError}};

#[derive(thiserror::Error, Debug)]
pub enum KVStoreError {
    #[error("I/O error: {0}")]
    IOError(#[from] std::io::Error),

    #[error("Configuration mismatch")]
    ConfigMismatch(KVStoreConfig),

    #[error("Invalid metadata")]
    InvalidMetadata,

    #[error("Page storage error: {0}")]
    PageStorageError(#[from] PageStorageError),

    #[error("Key map error: {0}")]
    KeyMapError(#[from] KeyMapError),

    #[error("Range block storage error: {0}")]
    RangeBlockStorageError(#[from] RangeBlockStorageError),

    #[error("Key not found")]
    KeyNotFound,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KVStoreConfig {
    pub block_size: usize,
    pub page_count: usize,
}

impl Default for KVStoreConfig {
    fn default() -> Self {
        KVStoreConfig {
            block_size: 4096,
            page_count: 1024,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Metadata {
    #[serde(flatten)]
    config: KVStoreConfig,
}

pub struct KVStore {
    keymap: HeapKeyMap<Arc<FastHeapStorage<Arc<FastPageStorage<RangeBlockStorage<Arc<FileBlockStorage>>, RangeBlockStorage<Arc<FileBlockStorage>>>>>>>,
}

impl KVStore {
    pub fn open(dir_path: impl AsRef<Path>, config: KVStoreConfig) -> Result<Self, KVStoreError> {
        create_dir_all(&dir_path)?;

        let metadata_path = dir_path.as_ref().join("metadata.json");
        let pages_path = dir_path.as_ref().join("pages.bin");

        let page_header_block_count = config.page_count * OCCUPIED_SIZE_BYTES / config.block_size
            + if (config.page_count * OCCUPIED_SIZE_BYTES) % config.block_size != 0 {
                1
            } else {
                0
            };

        let pages_file = if metadata_path.try_exists()? {
            let metadata_file = std::fs::OpenOptions::new()
                .read(true)
                .open(&metadata_path)?;
            let metadata: Metadata = serde_json::from_reader(metadata_file)
                .map_err(|_| KVStoreError::InvalidMetadata)?;

            if metadata.config != config {
                return Err(KVStoreError::ConfigMismatch(metadata.config));
            }

            let pages_file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&pages_path)?;

            pages_file
        } else {
            let metadata_file = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(&metadata_path)?;
            let metadata = Metadata {
                config: config.clone(),
            };

            serde_json::to_writer_pretty(metadata_file, &metadata)
                .map_err(|_| KVStoreError::InvalidMetadata)?;

            let pages_file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&pages_path)?;

            pages_file.set_len(
                (
                    page_header_block_count * config.block_size
                    + config.page_count * config.block_size
                ) as u64
            )?;

            pages_file
        };

        let pages = Arc::new(FileBlockStorage::new(pages_file, config.block_size, page_header_block_count + config.page_count)?);

        let header = RangeBlockStorage::new(pages.clone(), 0..page_header_block_count)?;
        let pages = RangeBlockStorage::new(pages.clone(), page_header_block_count..(page_header_block_count + config.page_count))?;

        let page_storage =
            Arc::new(FastPageStorage::new(header, pages)?);

        let heap_storage = Arc::new(FastHeapStorage::new(page_storage));

        let keymap = HeapKeyMap::new(heap_storage);

        Ok(KVStore {
            keymap,
        })
    }
}

impl KVStore {
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), KVStoreError> {
        Ok(self.keymap.insert(key, value)?)
    }

    pub fn get<'buf>(&self, key: &[u8], buffer: &'buf mut Vec<u8>) -> Result<&'buf [u8], KVStoreError> {
        let mut iter = self.keymap.iter(Some(key))?;
        let Some(mut entry) = KeyMapIterator::next(&mut iter)? else {
            return Err(KVStoreError::KeyNotFound);
        };
        let mut value_reader = KeyMapEntryReader::value(&mut entry)?;
        let offset = buffer.len();
        let len = value_reader.read_to_end(buffer)?;
        Ok(&buffer[offset..offset + len])
    }

    pub fn iter(&self, key: Option<&[u8]>) -> Result<impl KVStoreIterator, KVStoreError> {
        Ok(self.keymap.iter(key)?)
    }
}

pub trait KVStoreIterator {
    fn next(&mut self) -> Result<Option<impl KVStoreEntryReader>, KVStoreError>;
}

impl<T: KeyMapIterator> KVStoreIterator for T {
    fn next(&mut self) -> Result<Option<impl KVStoreEntryReader>, KVStoreError> {
        match self.next() {
            Ok(Some(entry)) => Ok(Some(entry)),
            Ok(None) => Ok(None),
            Err(e) => Err(KVStoreError::from(e)),
        }
    }
}

pub trait KVStoreEntryReader {
    fn key(&mut self) -> std::io::Result<impl Read>;
    fn value(&mut self) -> std::io::Result<impl Read>;
}

impl<T: KeyMapEntryReader> KVStoreEntryReader for T {
    fn key(&mut self) -> std::io::Result<impl Read> {
        self.key()
    }

    fn value(&mut self) -> std::io::Result<impl Read> {
        self.value()
    }
}
