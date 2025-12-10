// NOTE: DO NOT USE

use std::{iter::once, ops::Range};

use super::{BlockStorage, BlockStorageError};

pub struct SubdiviedBlockStorage<Storage> {
    storage: Storage,
    block_size: usize,
    block_count: usize,
}

struct BufferMapEntry {
    block_index: usize,
    block_range: Range<usize>,
    buffer_range: Range<usize>,
}

impl<Storage: BlockStorage> SubdiviedBlockStorage<Storage> {
    pub fn new(storage: Storage, block_size: usize) -> Result<Self, BlockStorageError> {
        if storage.block_size() % block_size != 0 {
            return Err(BlockStorageError::BufferSizeMisalignment);
        }
        let block_count = (storage.block_size() * storage.block_count()) / block_size;
        Ok(SubdiviedBlockStorage {
            storage,
            block_size,
            block_count,
        })
    }

    pub fn into_inner(self) -> Storage {
        self.storage
    }

    fn map(&self, index: usize, buffer_size: usize) -> Result<impl Iterator<Item = BufferMapEntry> + 'static, BlockStorageError> {
        let start_byte = index * self.block_size;
        let size = buffer_size;
        if size % self.block_size != 0 {
            return Err(BlockStorageError::BufferSizeMisalignment);
        }
        if start_byte + size > self.block_size * self.block_count {
            return Err(BlockStorageError::OutOfBounds);
        }

        let storage_block_size = self.storage.block_size();

        let start_block = start_byte / storage_block_size;
        let end_block = (start_byte + size - 1) / storage_block_size;
        let start_offset = start_byte % storage_block_size;
        let end_offset = (start_byte + size - 1) % storage_block_size + 1;

        // Head: from start_offset to end of first block (or to end_offset if same block)
        let head_end = if start_block == end_block {
            end_offset
        } else {
            storage_block_size
        };
        let head_buffer_len = head_end - start_offset;
        
        let head = BufferMapEntry {
            block_index: start_block,
            block_range: start_offset..head_end,
            buffer_range: 0..head_buffer_len,
        };

        // Middle: full blocks between start and end
        let middle = (start_block + 1..end_block).map(move |block_index| {
            let buffer_start = (block_index - start_block) * storage_block_size - start_offset;
            BufferMapEntry {
                block_index,
                block_range: 0..storage_block_size,
                buffer_range: buffer_start..(buffer_start + storage_block_size),
            }
        });

        // Tail: from start of last block to end_offset (only if end_block != start_block)
        let (tail_buffer_start, tail_buffer_end) = if start_block == end_block {
            // When in same block, tail should be empty (already covered by head)
            (0, 0)
        } else {
            let start = (end_block - start_block) * storage_block_size - start_offset;
            (start, size)
        };
        let tail = BufferMapEntry {
            block_index: end_block,
            block_range: 0..end_offset,
            buffer_range: tail_buffer_start..tail_buffer_end,
        };

        Ok(
            once(head)
            .chain(middle)
            .chain(once(tail))
            .filter(|entry| entry.buffer_range.start < entry.buffer_range.end)
        )
    }
}

impl<Storage: BlockStorage> BlockStorage for SubdiviedBlockStorage<Storage> {
    fn block_size(&self) -> usize {
        self.block_size
    }

    fn block_count(&self) -> usize {
        self.block_count
    }

    fn read_blocks(&self, index: usize, buffer: &mut [u8]) -> Result<(), BlockStorageError> {
        let mapped_entries = self.map(index, buffer.len())?;
        for entry in mapped_entries {
            let mut temp_buffer = vec![0u8; self.storage.block_size()];
            self.storage.read_blocks(entry.block_index, &mut temp_buffer)?;
            buffer[entry.buffer_range]
                .copy_from_slice(&temp_buffer[entry.block_range]);
        }
        Ok(())
    }

    fn write_blocks(&mut self, index: usize, buffer: &[u8]) -> Result<(), BlockStorageError> {
        let mapped_entries = self.map(index, buffer.len())?;
        for entry in mapped_entries {
            let mut temp_buffer = vec![0u8; self.storage.block_size()];
            self.storage.read_blocks(entry.block_index, &mut temp_buffer)?;
            temp_buffer[entry.block_range]
                .copy_from_slice(&buffer[entry.buffer_range]);
            self.storage.write_blocks(entry.block_index, &temp_buffer)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::memory::MemoryBlockStorage;

    #[test]
    fn test_subdivided_block_storage_map() {
        let block_size = 10;
        let block_count = 4;
        let subdivided_block_size = 5;

        let storage = MemoryBlockStorage::new(block_size, block_count);
        let subdivided = SubdiviedBlockStorage::new(storage, subdivided_block_size).unwrap();

        // Test case 1: Mapping spans multiple storage blocks
        let mapped_entries = subdivided.map(1, 15).unwrap().collect::<Vec<_>>();
        assert_eq!(mapped_entries.len(), 2);

        assert_eq!(mapped_entries[0].block_index, 0);
        assert_eq!(mapped_entries[0].block_range, 5..10);
        assert_eq!(mapped_entries[0].buffer_range, 0..5);

        assert_eq!(mapped_entries[1].block_index, 1);
        assert_eq!(mapped_entries[1].block_range, 0..10);
        assert_eq!(mapped_entries[1].buffer_range, 5..15);

        // Test case 2: Mapping starts at storage block boundary
        let mapped_entries = subdivided.map(0, 10).unwrap().collect::<Vec<_>>();
        assert_eq!(mapped_entries.len(), 1);
        assert_eq!(mapped_entries[0].block_index, 0);
        assert_eq!(mapped_entries[0].block_range, 0..10);
        assert_eq!(mapped_entries[0].buffer_range, 0..10);

        // Test case 3: Mapping within a single storage block
        let mapped_entries = subdivided.map(0, 5).unwrap().collect::<Vec<_>>();
        assert_eq!(mapped_entries.len(), 1);
        assert_eq!(mapped_entries[0].block_index, 0);
        assert_eq!(mapped_entries[0].block_range, 0..5);
        assert_eq!(mapped_entries[0].buffer_range, 0..5);

        // Test case 4: Mapping ends at storage block boundary
        let mapped_entries = subdivided.map(0, 20).unwrap().collect::<Vec<_>>();
        assert_eq!(mapped_entries.len(), 2);
        assert_eq!(mapped_entries[0].block_index, 0);
        assert_eq!(mapped_entries[0].block_range, 0..10);
        assert_eq!(mapped_entries[0].buffer_range, 0..10);
        assert_eq!(mapped_entries[1].block_index, 1);
        assert_eq!(mapped_entries[1].block_range, 0..10);
        assert_eq!(mapped_entries[1].buffer_range, 10..20);

        // Test case 5: Mapping spans three storage blocks
        let mapped_entries = subdivided.map(1, 25).unwrap().collect::<Vec<_>>();
        assert_eq!(mapped_entries.len(), 3);
        assert_eq!(mapped_entries[0].block_index, 0);
        assert_eq!(mapped_entries[0].block_range, 5..10);
        assert_eq!(mapped_entries[0].buffer_range, 0..5);
        assert_eq!(mapped_entries[1].block_index, 1);
        assert_eq!(mapped_entries[1].block_range, 0..10);
        assert_eq!(mapped_entries[1].buffer_range, 5..15);
        assert_eq!(mapped_entries[2].block_index, 2);
        assert_eq!(mapped_entries[2].block_range, 0..10);
        assert_eq!(mapped_entries[2].buffer_range, 15..25);

        // Test case 6: Single subdivided block in the middle of storage block
        let mapped_entries = subdivided.map(1, 5).unwrap().collect::<Vec<_>>();
        assert_eq!(mapped_entries.len(), 1);
        assert_eq!(mapped_entries[0].block_index, 0);
        assert_eq!(mapped_entries[0].block_range, 5..10);
        assert_eq!(mapped_entries[0].buffer_range, 0..5);

        // Test case 7: Mapping at the end of available space
        let mapped_entries = subdivided.map(6, 10).unwrap().collect::<Vec<_>>();
        assert_eq!(mapped_entries.len(), 1);
        assert_eq!(mapped_entries[0].block_index, 3);
        assert_eq!(mapped_entries[0].block_range, 0..10);
        assert_eq!(mapped_entries[0].buffer_range, 0..10);
    }

    #[test]
    fn test_subdivided_block_storage() {
        let block_size = 10;
        let block_count = 10;
        let subdivided_block_size = 5;

        // prepare data
        let mut data = vec![0u8; block_size * block_count];
        for (i, byte) in data.iter_mut().enumerate() {
            *byte = i as u8;
        }

        let storage = MemoryBlockStorage::from_vec(data.clone(), block_size).unwrap();
        let subdivided = SubdiviedBlockStorage::new(storage, subdivided_block_size).unwrap();

        // test block_count
        assert_eq!(subdivided.block_count(), (block_size * block_count) / subdivided_block_size);

        // read test - read 2 blocks starting at index 1
        let mut buffer = vec![0u8; subdivided_block_size * 2];
        subdivided.read_blocks(1, &mut buffer).unwrap();
        assert_eq!(buffer, data[subdivided_block_size * 1..subdivided_block_size * 3]);

        // write test - write 2 blocks starting at index 1
        let storage = MemoryBlockStorage::from_vec(data.clone(), block_size).unwrap();
        let mut subdivided = SubdiviedBlockStorage::new(storage, subdivided_block_size).unwrap();
        let write_data = vec![100u8; subdivided_block_size * 2];
        subdivided.write_blocks(1, &write_data).unwrap();
        let mut buffer = vec![0u8; block_size * block_count];
        subdivided.into_inner().read_blocks(0, &mut buffer).unwrap();
        data[subdivided_block_size * 1..subdivided_block_size * 3].copy_from_slice(&write_data);
        assert_eq!(buffer, data);

        // out of bounds test (index)
        let storage = MemoryBlockStorage::from_vec(data.clone(), block_size).unwrap();
        let subdivided = SubdiviedBlockStorage::new(storage, subdivided_block_size).unwrap();
        let result = subdivided.read_blocks(subdivided.block_count(), &mut buffer);
        assert!(matches!(result, Err(BlockStorageError::OutOfBounds)));

        // out of bounds test (size)
        let result = subdivided.read_blocks(0, &mut vec![0u8; (subdivided.block_count() + 1) * subdivided_block_size]);
        assert!(matches!(result, Err(BlockStorageError::OutOfBounds)));
    }
}
