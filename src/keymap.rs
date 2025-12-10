use std::io::Read;

use crate::{heap::{HeapEntryIterator, HeapStorage, HeapStorageError}};

#[derive(thiserror::Error, Debug)]
pub enum KeyMapError {
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Heap storage error: {0}")]
    HeapStorageError(#[from] HeapStorageError),

    #[error("Buffer too small")]
    BufferTooSmall,
}

pub trait KeyMapIterator {
    fn next(&mut self) -> Result<Option<impl KeyMapEntryReader>, KeyMapError>;
}

pub trait KeyMap {
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), KeyMapError>;
    fn iter(&self, key: Option<&[u8]>) -> Result<impl KeyMapIterator, KeyMapError>;
}

pub trait KeyMapEntryReader {
    fn key(&mut self) -> std::io::Result<impl Read>;
    fn value(&mut self) -> std::io::Result<impl Read>;
}

pub struct HeapKeyMap<Heap: HeapStorage> {
    heap: Heap,
}

impl<Heap: HeapStorage> HeapKeyMap<Heap> {
    pub fn new(heap: Heap) -> Self {
        HeapKeyMap { heap }
    }
}

impl<Heap: HeapStorage> KeyMap for HeapKeyMap<Heap> {
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<(), KeyMapError> {
        let entry = Entry { key, value };
        let entry_size = entry.size();
        // TODO: encode by iterator of slices to avoid allocation
        let mut buffer = vec![0u8; entry_size];
        entry.encode(&mut buffer);
        let heap = &mut self.heap;
        let desired_page_index = to_u64(key) as usize % heap.page_count();
        heap.insert_entry(desired_page_index, &buffer)?;
        Ok(())
    }

    fn iter(&self, key: Option<&[u8]>) -> Result<impl KeyMapIterator, KeyMapError> {
        let start_page_index = if let Some(key) = key {
            to_u64(key) as usize % self.heap.page_count()
        } else {
            0
        };
        Ok(HeapKeyMapIterator {
            heap_iterator: self.heap.iter_entries(start_page_index)?,
            key,
        })
    }
}

pub struct HeapKeyMapIterator<'key, HeapIter: HeapEntryIterator> {
    heap_iterator: HeapIter,
    key: Option<&'key [u8]>,
}

impl<'key, HeapIter: HeapEntryIterator> KeyMapIterator for HeapKeyMapIterator<'key, HeapIter> {
    fn next(&mut self) -> Result<Option<impl KeyMapEntryReader>, KeyMapError> {
        'entry_loop: loop {
            let Some(heap_reader) = self.heap_iterator.next()? else {
                return Ok(None);
            };
            let mut entry_reader = HeapKeyMapEntryReader {
                state: HeapKeyMapEntryReaderState::New,
                heap_reader
            };
            if let Some(key) = &self.key {
                let mut key_reader = entry_reader.key()?;
                let mut buffer = [0u8; 1024];
                let mut total_read = 0;
                loop {
                    let to_read = (key.len() - total_read).min(buffer.len());
                    let read = key_reader.read(&mut buffer[..to_read])?;
                    if read == 0 {
                        // key is shorter than expected
                        continue 'entry_loop;
                    }
                    total_read += read;
                    if total_read > key.len() {
                        // key is longer than expected
                        continue 'entry_loop;
                    }
                    if &buffer[..read] != &key[total_read - read..total_read] {
                        // key does not match
                        continue 'entry_loop;
                    }
                    if total_read == key.len() {
                        break;
                    }
                }
                drop(key_reader);
                entry_reader.state = HeapKeyMapEntryReaderState::ReadingFromKeyBuffer { key };
            }
            return Ok(Some(entry_reader));
        }
    }
}

enum HeapKeyMapEntryReaderState<'key> {
    New,
    ReadingKey { remaining: usize },
    ReadingFromKeyBuffer { key: &'key [u8] },
    ReadingValue,
}

pub struct HeapKeyMapEntryReader<'key, HeapReader: Read> {
    state: HeapKeyMapEntryReaderState<'key>,
    heap_reader: HeapReader,
}

impl<'key, HeapReader: Read> KeyMapEntryReader for HeapKeyMapEntryReader<'key, HeapReader> {
    fn key(&mut self) -> std::io::Result<impl Read> {
        match &self.state {
            HeapKeyMapEntryReaderState::New => {
                let mut key_size_bytes = [0u8; 2];
                self.heap_reader.read_exact(&mut key_size_bytes)?;
                let key_size = u16::from_le_bytes(key_size_bytes) as usize;
                self.state = HeapKeyMapEntryReaderState::ReadingKey { remaining: key_size };
                Ok(self)
            }
            HeapKeyMapEntryReaderState::ReadingKey { .. } => {
                return Ok(self);
            }
            HeapKeyMapEntryReaderState::ReadingFromKeyBuffer { .. } => {
                Ok(self)
            }
            _ => Err(std::io::Error::new(std::io::ErrorKind::Other, "Invalid state for reading key"))
        }
    }

    fn value(&mut self) -> std::io::Result<impl Read> {
        if let HeapKeyMapEntryReaderState::New = self.state {
            self.key()?;
        }
        if let HeapKeyMapEntryReaderState::ReadingKey { remaining } = &mut self.state {
            let mut skip_buffer = [0u8; 1024];
            while *remaining > 0 {
                let to_read = (*remaining).min(skip_buffer.len());
                let read = self.heap_reader.read(&mut skip_buffer[..to_read])?;
                if read == 0 {
                    return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Unexpected EOF while skip-reading key"));
                }
                *remaining -= read;
            }
            self.state = HeapKeyMapEntryReaderState::ReadingValue;
        }
        if let HeapKeyMapEntryReaderState::ReadingFromKeyBuffer { .. } = self.state {
            self.state = HeapKeyMapEntryReaderState::ReadingValue;
        }
        let HeapKeyMapEntryReaderState::ReadingValue = self.state else {
            return Err(std::io::Error::new(std::io::ErrorKind::Other, "Invalid state for reading value"));
        };
        return Ok(self);
    }
}

impl<'key, HeapReader: Read> Read for HeapKeyMapEntryReader<'key, HeapReader> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match &mut self.state {
            HeapKeyMapEntryReaderState::ReadingKey { remaining } => {
                let to_read = (*remaining).min(buf.len());
                let read = self.heap_reader.read(&mut buf[..to_read])?;
                *remaining -= read;
                Ok(read)
            }
            HeapKeyMapEntryReaderState::ReadingFromKeyBuffer { key } => {
                let to_read = key.len().min(buf.len());
                buf[..to_read].copy_from_slice(&key[..to_read]);
                *key = &key[to_read..];
                Ok(to_read)
            }
            HeapKeyMapEntryReaderState::ReadingValue => {
                let read = self.heap_reader.read(buf)?;
                Ok(read)
            }
            _ => Err(std::io::Error::new(std::io::ErrorKind::Other, "Invalid state for reading")),
        }
    }
}

fn to_u64(bytes: &[u8]) -> u64 {
    let mut array = [0u8; 8];
    let len = bytes.len().min(8);
    array[..len].copy_from_slice(&bytes[..len]);
    u64::from_le_bytes(array)
}

struct Entry<'a> {
    key: &'a [u8],
    value: &'a [u8],
}

impl<'a> Entry<'a> {
    fn size(&self) -> usize {
        2 + self.key.len() + self.value.len()
    }

    fn encode(&self, buffer: &mut [u8]) {
        if buffer.len() != self.size() {
            panic!("Buffer size mismatch");
        }
        let key_len = self.key.len() as u16;
        buffer[..2].copy_from_slice(&key_len.to_le_bytes());
        buffer[2..2 + self.key.len()].copy_from_slice(self.key);
        buffer[2 + self.key.len()..].copy_from_slice(self.value);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{block::memory::MemoryBlockStorage, heap::FastHeapStorage, page::FastPageStorage};

    #[test]
    fn test_heap_key_map_insert_get() {
        let header = Arc::new(MemoryBlockStorage::allocate(4, 2));
        let pages = Arc::new(MemoryBlockStorage::allocate(64, 4));

        let page_storage = Arc::new(FastPageStorage::new(header, pages).unwrap());
        let heap_storage = Arc::new(FastHeapStorage::new(page_storage));

        let mut heap_key_map = HeapKeyMap::new(heap_storage);

        let key1 = b"key1";
        let value1 = b"value1";
        heap_key_map.insert(key1, value1).unwrap();

        let mut iter = heap_key_map.iter(Some(key1)).unwrap();
        let mut buf = Vec::new();
        iter.next().unwrap().unwrap().value().unwrap().read_to_end(&mut buf).unwrap();
        assert_eq!(buf, value1);
        drop(iter);

        let key2 = b"another_key";
        let value2 = b"another_value";
        heap_key_map.insert(key2, value2).unwrap();

        let mut iter = heap_key_map.iter(Some(key2)).unwrap();
        let mut buf = Vec::new();
        iter.next().unwrap().unwrap().value().unwrap().read_to_end(&mut buf).unwrap();
        assert_eq!(buf, value2);
        drop(iter);
    }
}
