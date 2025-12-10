use std::sync::{Arc, RwLock};

use crate::block::Block;

use super::{BlockStorage, BlockStorageError};

#[derive(thiserror::Error, Debug)]
pub enum MemoryBlockStorageError {
    #[error("Buffer size misalignment")]
    BufferSizeMisalignment,
}

pub struct MemoryBlock<Buffer> {
    index: usize,
    block_size: usize,
    storage: Arc<MemoryBlockStorage<Buffer>>,
}

impl<Buffer: AsRef<[u8]> + AsMut<[u8]>> Block for MemoryBlock<Buffer> {
    fn size(&self) -> usize {
        self.block_size
    }

    fn index(&self) -> usize {
        self.index
    }

    fn read(&self, offset: usize, buffer: &mut [u8]) -> Result<(), BlockStorageError> {
        if offset + buffer.len() > self.block_size {
            return Err(BlockStorageError::OutOfBounds);
        }
        let start = self.index * self.block_size + offset;
        let end = start + buffer.len();
        let data = self.storage.buffer.read().map_err(|err| BlockStorageError::IOError(std::io::Error::new(std::io::ErrorKind::Other, format!("RwLock read error: {}", err))))?;
        let data = data.as_ref();
        if end > data.len() {
            return Err(BlockStorageError::OutOfBounds);
        }
        buffer.copy_from_slice(&data[start..end]);
        Ok(())
    }

    fn write(&self, offset: usize, buffer: &[u8]) -> Result<(), BlockStorageError> {
        if offset + buffer.len() > self.block_size {
            return Err(BlockStorageError::OutOfBounds);
        }
        let start = self.index * self.block_size + offset;
        let end = start + buffer.len();
        let mut data = self.storage.buffer.write().map_err(|err| BlockStorageError::IOError(std::io::Error::new(std::io::ErrorKind::Other, format!("RwLock write error: {}", err))))?;
        let data = data.as_mut();
        if end > data.len() {
            return Err(BlockStorageError::OutOfBounds);
        }
        data[start..end].copy_from_slice(buffer);
        Ok(())
    }
}

#[derive(Debug)]
pub struct MemoryBlockStorage<Buffer> {
    buffer: RwLock<Buffer>,
    block_count: usize,
    block_size: usize,
}

impl<'a, Buffer: AsRef<[u8]> + AsMut<[u8]> + 'a> BlockStorage for Arc<MemoryBlockStorage<Buffer>> {
    type Block = MemoryBlock<Buffer>;

    fn block_size(&self) -> usize {
        self.block_size
    }

    fn block_count(&self) -> usize {
        self.block_count
    }

    fn get_block(&self, index: usize) -> Result<Self::Block, BlockStorageError> {
        if index >= self.block_count() {
            return Err(BlockStorageError::OutOfBounds);
        }
        Ok(MemoryBlock {
            index,
            block_size: self.block_size,
            storage: self.clone(),
        })
    }
}

impl MemoryBlockStorage<Vec<u8>> {
    pub fn allocate(block_size: usize, block_count: usize) -> Self {
        MemoryBlockStorage {
            buffer: RwLock::new(vec![0u8; block_size * block_count]),
            block_count,
            block_size,
        }
    }
}

impl<Buffer: AsRef<[u8]> + AsMut<[u8]>> MemoryBlockStorage<Buffer> {
    pub fn from_buffer(buffer: Buffer, block_size: usize) -> Result<Self, MemoryBlockStorageError> {
        let total_size = buffer.as_ref().len();
        if total_size % block_size != 0 {
            return Err(MemoryBlockStorageError::BufferSizeMisalignment);
        }
        Ok(MemoryBlockStorage {
            buffer: RwLock::new(buffer),
            block_count: total_size / block_size,
            block_size,
        })
    }

    pub fn try_into_buffer(self: Arc<Self>) -> Result<Buffer, Arc<Self>> {
        let storage = Arc::try_unwrap(self)?;
        match storage.buffer.into_inner() {
            Ok(buffer) => Ok(buffer),
            Err(rwlock) => Err(Arc::new(MemoryBlockStorage {
                // TODO: is poisioned lock handling needed here?
                buffer: RwLock::new(rwlock.into_inner()),
                ..storage
            })),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_block_storage() {
        const BLOCK_SIZE: usize = 10;
        const BLOCK_COUNT: usize = 4;

        // prepare data
        let mut data = [0u8; BLOCK_SIZE * BLOCK_COUNT];
        for (i, byte) in data.iter_mut().enumerate() {
            *byte = i as u8;
        }

        // read test
        let storage = Arc::new(MemoryBlockStorage::from_buffer(&mut data, BLOCK_SIZE).unwrap());
        for i in 0..BLOCK_COUNT {
            let block = storage.get_block(i).unwrap();
            let mut buffer = vec![0u8; BLOCK_SIZE];
            block.read(0, &mut buffer).unwrap();
            for j in 0..BLOCK_SIZE {
                assert_eq!(buffer[j], (i * BLOCK_SIZE + j) as u8);
            }
        }

        // write test
        let storage = storage;
        for i in 0..BLOCK_COUNT {
            let block = storage.get_block(i).unwrap();
            let buffer: Vec<u8> = (0..BLOCK_SIZE).map(|j| 255 - (i * BLOCK_SIZE + j) as u8).collect();
            block.write(0, &buffer).unwrap();
        }
        let buffer = storage.try_into_buffer().unwrap();
        for i in 0..(BLOCK_SIZE * BLOCK_COUNT) {
            assert_eq!(buffer[i], 255 - (i as u8));
        }

        // out of bounds test (index)
        let storage = Arc::new(MemoryBlockStorage::from_buffer(buffer, BLOCK_SIZE).unwrap());
        assert!(matches!(storage.get_block(BLOCK_COUNT), Err(BlockStorageError::OutOfBounds)));

        // out of bounds test (buffer)
        let block = storage.get_block(0).unwrap();
        let mut buffer = vec![0u8; BLOCK_SIZE + 1];
        assert!(matches!(block.read(0, &mut buffer), Err(BlockStorageError::OutOfBounds)));
    }
}
