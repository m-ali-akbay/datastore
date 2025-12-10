use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex, MutexGuard};
use crate::block::Block;

use super::{BlockStorage, BlockStorageError};

pub struct FileBlockStorage {
    // TODO: use pool of independent file handles for better concurrency
    file: Mutex<File>,
    block_size: usize,
    block_count: usize,
}

impl FileBlockStorage {
    pub fn new(file: File, block_size: usize, block_count: usize) -> Result<Self, std::io::Error> {
        let file_size = file.metadata()?.len() as usize;
        if file_size % block_size != 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "File size is not a multiple of block size",
            ));
        }
        if file_size / block_size != block_count {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "File size does not match block count",
            ));
        }
        Ok(FileBlockStorage {
            file: Mutex::new(file),
            block_size,
            block_count,
        })
    }
}

impl BlockStorage for Arc<FileBlockStorage> {
    type Block = FileBlock;

    fn block_size(&self) -> usize {
        self.block_size
    }

    fn block_count(&self) -> usize {
        self.block_count
    }

    fn get_block(&self, index: usize) -> Result<Self::Block, BlockStorageError> {
        if index >= self.block_count {
            return Err(BlockStorageError::OutOfBounds);
        }
        Ok(FileBlock {
            index,
            block_size: self.block_size,
            storage: self.clone(),
        })
    }
}

pub struct FileBlock {
    index: usize,
    block_size: usize,
    storage: Arc<FileBlockStorage>,
}

impl FileBlock {
    fn seek(&self, offset: usize) -> Result<MutexGuard<'_, File>, BlockStorageError> {
        if offset > self.block_size {
            return Err(BlockStorageError::OutOfBounds);
        }
        let mut file = self.storage.file.lock().map_err(|err| {
            BlockStorageError::IOError(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to lock file mutex: {}", err)))
        })?;
        file.seek(SeekFrom::Start((self.index * self.block_size + offset) as u64))?;
        Ok(file)
    }
}

impl Block for FileBlock {
    fn index(&self) -> usize {
        self.index
    }

    fn size(&self) -> usize {
        self.block_size
    }

    fn read(&self, offset: usize, buffer: &mut [u8]) -> Result<(), BlockStorageError> {
        if offset + buffer.len() > self.block_size {
            return Err(BlockStorageError::OutOfBounds);
        }
        let mut file = self.seek(offset)?;
        file.read_exact(buffer)?;
        Ok(())
    }

    fn write(&self, offset: usize, buffer: &[u8]) -> Result<(), BlockStorageError> {
        if offset + buffer.len() > self.block_size {
            return Err(BlockStorageError::OutOfBounds);
        }
        let mut file = self.seek(offset)?;
        file.write_all(buffer)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_file_block_storage() {
        let mut temp_file = NamedTempFile::new().unwrap();
        let data = vec![0u8; 1024];
        temp_file.write_all(&data).unwrap();
        temp_file.flush().unwrap();
        
        let file = temp_file.reopen().unwrap();
        let storage = Arc::new(FileBlockStorage::new(file, 256, 4).unwrap());

        assert_eq!(storage.block_size(), 256);
        assert_eq!(storage.block_count(), 4);

        let block = storage.get_block(2).unwrap();
        assert_eq!(block.index(), 2);
        assert_eq!(block.size(), 256);

        let mut buffer = vec![0u8; 256];
        block.read(0, &mut buffer).unwrap();
        assert_eq!(buffer, vec![0u8; 256]);

        let write_data = vec![1u8; 256];
        block.write(0, &write_data).unwrap();

        let mut read_back = vec![0u8; 256];
        block.read(0, &mut read_back).unwrap();
        assert_eq!(read_back, write_data);

        // TODO: do more comprehensive tests
    }
}
