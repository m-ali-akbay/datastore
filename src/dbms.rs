pub mod keymap;

use std::{fs::{File, create_dir_all, rename}, io::{Read, Seek}, path::{Path, PathBuf}};

use crate::{dbms::keymap::{KeyMapConfig, KeyMapOpenError, ManagedKeyMap, open_key_map}, heap::HeapStorageError, keymap::{KeyMap, KeyMapEntryReader, KeyMapError, KeyMapIterator}};

#[derive(thiserror::Error, Debug)]
pub enum KVStoreError {
    #[error("I/O error: {0}")]
    IOError(#[from] std::io::Error),

    #[error("Block size configuration mismatch: expected {0} bytes")]
    BlockSizeConfigMismatch(usize),

    #[error("Invalid metadata")]
    InvalidMetadata,

    #[error("Key map open error: {0}")]
    KeyMapOpenError(#[from] KeyMapOpenError),

    #[error("Key map error: {0}")]
    KeyMapError(#[from] KeyMapError),

    #[error("Key not found")]
    KeyNotFound,
}

#[derive(Clone, Debug)]
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
    config: KeyMapConfig,
    revision: u64,
}

pub struct KVStore {
    dir_path: PathBuf,
    metadata_file: File,
    metadata: Metadata,
    keymap: ManagedKeyMap,
}

impl KVStore {
    pub fn open(dir_path: impl AsRef<Path>, config: KVStoreConfig) -> Result<Self, KVStoreError> {
        create_dir_all(&dir_path)?;

        let metadata_path = dir_path.as_ref().join("metadata.json");

        let (metadata, metadata_file) = if metadata_path.try_exists()? {
            let metadata_file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&metadata_path)?;
            let metadata: Metadata = serde_json::from_reader(&metadata_file)
                .map_err(|_| KVStoreError::InvalidMetadata)?;

            if metadata.config.block_size != config.block_size {
                return Err(KVStoreError::BlockSizeConfigMismatch(metadata.config.block_size));
            }

            (metadata, metadata_file)
        } else {
            let metadata_file = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(&metadata_path)?;
            let metadata = Metadata {
                config: KeyMapConfig {
                    block_size: config.block_size,
                    page_count: config.page_count,
                },
                revision: 0,
            };

            serde_json::to_writer_pretty(&metadata_file, &metadata)
                .map_err(|_| KVStoreError::InvalidMetadata)?;

            (metadata, metadata_file)
        };

        let pages_path = dir_path.as_ref().join(format!("pages.rev-{}.dat", metadata.revision));
        let keymap = open_key_map(pages_path, metadata.config.clone())?;

        Ok(KVStore {
            dir_path: dir_path.as_ref().to_path_buf(),
            metadata_file,
            metadata,
            keymap,
        })
    }
}

impl KVStore {
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), KVStoreError> {
        match self.keymap.insert(key, value) {
            Ok(()) => Ok(()),
            Err(KeyMapError::HeapStorageError(HeapStorageError::FullHeap)) => {
                let tmp_pages_path = self.dir_path.join(format!("pages.rev-{}.dat.tmp", self.metadata.revision + 1));
                let pages_path = self.dir_path.join(format!("pages.rev-{}.dat", self.metadata.revision + 1));

                if tmp_pages_path.try_exists()? {
                    std::fs::remove_file(&tmp_pages_path)?;
                }

                let mut config = self.metadata.config.clone();
                config.page_count += config.page_count / 2;
                let mut keymap = open_key_map(&tmp_pages_path, config.clone())?;

                let mut iter = self.keymap.iter(None)?;
                let mut key_buffer = Vec::new();
                let mut value_buffer = Vec::new();
                loop {
                    let Some(mut entry) = KeyMapIterator::next(&mut iter)? else {
                        break;
                    };
                    key_buffer.clear();
                    value_buffer.clear();
                    {
                        let mut key_reader = KeyMapEntryReader::key(&mut entry)?;
                        key_reader.read_to_end(&mut key_buffer)?;
                    }
                    {
                        let mut value_reader = KeyMapEntryReader::value(&mut entry)?;
                        value_reader.read_to_end(&mut value_buffer)?;
                    }

                    keymap.insert(&key_buffer, &value_buffer)?;
                }
                drop(iter);

                rename(&tmp_pages_path, &pages_path)?;

                self.metadata.config = config;
                self.metadata.revision += 1;
                self.metadata_file.set_len(0)?;
                self.metadata_file.seek(std::io::SeekFrom::Start(0))?;
                serde_json::to_writer_pretty(&self.metadata_file, &self.metadata)
                    .map_err(|_| KVStoreError::InvalidMetadata)?;
                self.keymap = keymap;

                let old_revision = self.metadata.revision - 1;
                let old_pages_path = self.dir_path.join(format!("pages.rev-{}.dat", old_revision));
                std::fs::remove_file(&old_pages_path)?;

                Ok(self.keymap.insert(key, value)?)
            },
            Err(err) => Err(err.into()),
        }
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
