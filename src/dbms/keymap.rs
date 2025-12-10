use std::{path::Path, sync::Arc};

use serde::{Serialize, Deserialize};

use crate::{block::{fs::FileBlockStorage, range::{RangeBlockStorage, RangeBlockStorageError}}, heap::FastHeapStorage, keymap::HeapKeyMap, page::{FastPageStorage, OCCUPIED_SIZE_BYTES, PageStorageError}};

#[derive(thiserror::Error, Debug)]
pub enum KeyMapOpenError {
    #[error("I/O error: {0}")]
    IOError(#[from] std::io::Error),

    #[error("Page storage error: {0}")]
    PageStorageError(#[from] PageStorageError),

    #[error("Range block storage error: {0}")]
    RangeBlockStorageError(#[from] RangeBlockStorageError),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyMapConfig {
    pub block_size: usize,
    pub page_count: usize,
}

pub type ManagedKeyMap = HeapKeyMap<Arc<FastHeapStorage<Arc<FastPageStorage<RangeBlockStorage<Arc<FileBlockStorage>>, RangeBlockStorage<Arc<FileBlockStorage>>>>>>>;

pub fn open_key_map(pages_path: impl AsRef<Path>, config: KeyMapConfig) -> Result<ManagedKeyMap, KeyMapOpenError> {
    let page_header_block_count = config.page_count * OCCUPIED_SIZE_BYTES / config.block_size
        + if (config.page_count * OCCUPIED_SIZE_BYTES) % config.block_size != 0 {
            1
        } else {
            0
        };
    let pages_path = pages_path.as_ref();

    let pages_file = if pages_path.try_exists()? {
        let pages_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&pages_path)?;

        pages_file
    } else {
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

    Ok(keymap)
}
