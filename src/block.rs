pub mod memory;
pub mod range;
pub mod fs;
// pub mod subdivide;

#[derive(thiserror::Error, Debug)]
pub enum BlockStorageError {
    #[error("I/O error: {0}")]
    IOError(#[from] std::io::Error),
    #[error("Out of bounds")]
    OutOfBounds,
}

pub trait Block {
    fn size(&self) -> usize;
    fn index(&self) -> usize;
    fn read(&self, offset: usize, buffer: &mut [u8]) -> Result<(), BlockStorageError>;
    fn write(&self, offset: usize, buffer: &[u8]) -> Result<(), BlockStorageError>;
}

pub trait BlockStorage {
    type Block: Block;

    fn block_size(&self) -> usize;
    fn block_count(&self) -> usize;
    fn get_block(&self, index: usize) -> Result<Self::Block, BlockStorageError>;
}
