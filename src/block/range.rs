use std::ops::Range;

use super::{BlockStorage, BlockStorageError};

#[derive(thiserror::Error, Debug)]
pub enum RangeBlockStorageError {
    #[error("Invalid range")]
    InvalidRange,
}

pub struct RangeBlockStorage<Storage> {
    storage: Storage,
    start_index: usize,
    block_count: usize,
}

impl<Storage: BlockStorage> RangeBlockStorage<Storage> {
    pub fn new(storage: Storage, range: Range<usize>) -> Result<Self, RangeBlockStorageError> {
        if range.start > range.end || range.end > storage.block_count() {
            return Err(RangeBlockStorageError::InvalidRange);
        }
        Ok(RangeBlockStorage {
            storage,
            start_index: range.start,
            block_count: range.len(),
        })
    }

    pub fn into_inner(self) -> Storage {
        self.storage
    }
}

impl<Storage: BlockStorage> BlockStorage for RangeBlockStorage<Storage> {
    type Block = Storage::Block;

    fn block_size(&self) -> usize {
        self.storage.block_size()
    }

    fn block_count(&self) -> usize {
        self.block_count
    }

    fn get_block(&self, index: usize) -> Result<Self::Block, BlockStorageError> {
        if index >= self.block_count {
            return Err(BlockStorageError::OutOfBounds);
        }
        self.storage.get_block(self.start_index + index)
    }
}
