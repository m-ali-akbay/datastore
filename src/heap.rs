use std::{cell::RefCell, cmp::min, io::Read, iter::once, sync::Arc};

use crate::page::{Page, PageStorage, PageStorageError};

#[derive(thiserror::Error, Debug)]
pub enum HeapStorageError {
    #[error("Page storage error: {0}")]
    PageStorageError(#[from] PageStorageError),

    #[error("Heap is full")]
    FullHeap,

    #[error("Heap has zero pages")]
    ZeroHeap,

    #[error("Buffer too small")]
    BufferTooSmall,

    #[error("Entry out of bounds")]
    EntryOutOfBounds,
}

pub trait HeapStorage {
    fn page_count(&self) -> usize;
    fn iter_entries(&self, start_page_index: usize) -> Result<impl HeapEntryIterator, HeapStorageError>;
    fn insert_entry(&mut self, desired_page_index: usize, data: &[u8]) -> Result<(), HeapStorageError>;
}

pub trait HeapEntryIterator {
    // TODO: make this only for mutable references
    fn next(&self) -> Result<Option<impl Read>, HeapStorageError>;
}

struct PageIndexIterator {
    current_index: usize,
    current_offset: usize,
    page_count: usize,
}

impl Iterator for PageIndexIterator {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_offset >= self.page_count {
            return None;
        }
        let index = self.current_index;
        self.current_index += 1;
        self.current_offset += 1;
        if self.current_index == self.page_count {
            self.current_index = 0;
        }
        return Some(index);
    }
}

impl PageIndexIterator {
    fn new(start_index: usize, page_count: usize) -> Self {
        PageIndexIterator {
            current_index: start_index,
            current_offset: 0,
            page_count,
        }
    }
}

pub struct FastHeapStorage<Pages: PageStorage> {
    pages: Pages,
}

impl<Pages: PageStorage> FastHeapStorage<Pages> {
    pub fn new(pages: Pages) -> Self {
        FastHeapStorage { pages }
    }
}

impl<Pages: PageStorage> HeapStorage for Arc<FastHeapStorage<Pages>> {
    fn page_count(&self) -> usize {
        self.pages.page_count()
    }

    fn iter_entries(&self, start_page_index: usize) -> Result<impl HeapEntryIterator, HeapStorageError> {
        let page_iterator = PageIndexIterator::new(start_page_index, self.page_count());
        let heap_iterator = FastHeapIterator::new(self.clone(), page_iterator)?;
        Ok(RefCell::new(heap_iterator))
    }

    fn insert_entry(&mut self, desired_page_index: usize, mut data: &[u8]) -> Result<(), HeapStorageError> {
        let mut parts = Vec::<(Pages::Page, &[u8])>::new();
        for page_index in PageIndexIterator::new(desired_page_index, self.page_count()) {
            let page = self.pages.get_page(page_index)?;
            let page_free = page.free_size()?;
            let data_free = page_free.saturating_sub(FastHeapEntryHeader::SIZE);
            if data_free == 0 {
                continue;
            }
            
            let part_payload_size = min(data.len(), data_free);
            let part_payload = &data[..part_payload_size];
            data = &data[part_payload_size..];

            parts.push((page, part_payload));

            if !data.is_empty() {
                continue;
            }

            for ((index, (page, part_payload)), next_part) in parts.iter().enumerate().zip(
                parts.iter().skip(1).map(Some).chain(once(None))
            ) {
                let head = index == 0;
                let next = next_part.map(|(page, _)| Result::<_, HeapStorageError>::Ok(FastHeapEntryPointer {
                    page_index: page.index() as u32,
                    entry_offset: page.occupied_size()? as u16,
                })).transpose()?;
                let header = FastHeapEntryHeader {
                    head,
                    next,
                    payload_length: part_payload.len() as u16,
                };
                header.append_to(page)?;
                page.append(part_payload)?;
            }

            return Ok(());
        }
        Err(HeapStorageError::FullHeap)
    }
}

pub struct FastHeapIterator<Pages: PageStorage> {
    heap: Arc<FastHeapStorage<Pages>>,
    page_index_iterator: PageIndexIterator,
    current_page: Pages::Page,
    current_entry_offset: usize,
}

impl<Pages: PageStorage> FastHeapIterator<Pages> {
    fn new(heap: Arc<FastHeapStorage<Pages>>, mut page_index_iterator: PageIndexIterator) -> Result<Self, HeapStorageError> {
        let first_page_index = page_index_iterator.next().ok_or(HeapStorageError::ZeroHeap)?;
        let current_page = heap.pages.get_page(first_page_index)?;
        Ok(FastHeapIterator {
            heap,
            page_index_iterator,
            current_page,
            current_entry_offset: 0,
        })
    }

    fn next_head_entry_header(&mut self) -> Result<Option<(usize, FastHeapEntryHeader, usize)>, HeapStorageError> {
        loop {
            let mut occupied_size = self.current_page.occupied_size()?;
            // TODO: handle overflow of entry offset
            while self.current_entry_offset as usize >= occupied_size {
                let Some(next_page_index) = self.page_index_iterator.next() else {
                    return Ok(None);
                };
                self.current_page = self.heap.pages.get_page(next_page_index)?;
                self.current_entry_offset = 0;
                occupied_size = self.current_page.occupied_size()?;
            }

            let header = FastHeapEntryHeader::load_from(self.current_entry_offset, &self.current_page)?;
            if !header.head {
                self.current_entry_offset += FastHeapEntryHeader::SIZE + header.payload_length as usize;
                continue;
            }

            let payload_offset = self.current_entry_offset + FastHeapEntryHeader::SIZE;

            self.current_entry_offset = payload_offset + header.payload_length as usize;

            return Ok(Some((self.current_page.index(), header, payload_offset)));
        }
    }
}

impl<Pages: PageStorage> HeapEntryIterator for RefCell<FastHeapIterator<Pages>> {
    fn next(&self) -> Result<Option<impl Read>, HeapStorageError> {
        let Some((page_index, header, payload_offset)) = self.borrow_mut().next_head_entry_header()? else {
            return Ok(None);
        };

        let page = self.borrow_mut().heap.pages.get_page(page_index)?;

        Ok(Some(FastHeapEntryReader {
            storage: self.borrow().heap.clone(),
            page,
            payload_offset,
            payload_remaining: header.payload_length as usize,
            entry_header: header,
        }))
    }
}

#[derive(Copy, Clone, Debug)]
struct FastHeapEntryPointer {
    page_index: u32,
    entry_offset: u16,
}

impl FastHeapEntryPointer {
    const SIZE: usize = 6;

    fn decode(buffer: [u8; Self::SIZE]) -> Self {
        let page_index = u32::from_le_bytes(buffer[0..4].try_into().unwrap());
        let entry_offset = u16::from_le_bytes(buffer[4..6].try_into().unwrap());
        FastHeapEntryPointer {
            page_index,
            entry_offset,
        }
    }

    fn encode(&self) -> [u8; Self::SIZE] {
        let mut buffer = [0u8; Self::SIZE];
        buffer[0..4].copy_from_slice(&self.page_index.to_le_bytes());
        buffer[4..6].copy_from_slice(&self.entry_offset.to_le_bytes());
        buffer
    }
}

#[derive(Debug)]
struct FastHeapEntryHeader {
    head: bool,
    next: Option<FastHeapEntryPointer>,
    payload_length: u16,
}

impl FastHeapEntryHeader {
    const SIZE: usize = 1 + FastHeapEntryPointer::SIZE + 2; // flags + next + payload_length

    fn decode(buffer: [u8; Self::SIZE]) -> Self {
        let flags = buffer[0];

        let head = (flags & 0x01) != 0;
        let incomplete = (flags & 0x02) != 0;

        let next = if incomplete {
            Some(FastHeapEntryPointer::decode(
                buffer[1..][..FastHeapEntryPointer::SIZE].try_into().unwrap(),
            ))
        } else {
            None
        };

        let payload_length = u16::from_le_bytes(buffer[1 + FastHeapEntryPointer::SIZE..][..2].try_into().unwrap());

        FastHeapEntryHeader {
            head,
            payload_length,
            next,
        }
    }

    fn encode(&self) -> [u8; Self::SIZE] {
        let mut buffer = [0u8; Self::SIZE];

        let mut flags = 0u8;
        if self.head {
            flags |= 0x01;
        }
        if self.next.is_some() {
            flags |= 0x02;
        }
        buffer[0] = flags;

        if let Some(next) = &self.next {
            buffer[1..][..FastHeapEntryPointer::SIZE].copy_from_slice(&next.encode());
        }

        buffer[1 + FastHeapEntryPointer::SIZE..][..2].copy_from_slice(&self.payload_length.to_le_bytes());

        buffer
    }

    fn load_from(offset: usize, page: &impl Page) -> Result<FastHeapEntryHeader, PageStorageError> {
        let mut buffer = [0u8; Self::SIZE];
        page.read(offset, &mut buffer)?; // TODO: check read size
        Ok(FastHeapEntryHeader::decode(buffer))
    }

    fn append_to(&self, page: &impl Page) -> Result<(), PageStorageError> {
        page.append(&self.encode())
    }
}

pub struct FastHeapEntryReader<Pages: PageStorage> {
    storage: Arc<FastHeapStorage<Pages>>,
    page: Pages::Page,
    entry_header: FastHeapEntryHeader,
    payload_offset: usize,
    payload_remaining: usize,
}

impl<Pages: PageStorage> Read for FastHeapEntryReader<Pages> {
    fn read(&mut self, buffer: &mut [u8]) -> Result<usize, std::io::Error> {
        if buffer.len() == 0 {
            return Ok(0);
        }
        if self.payload_remaining == 0 {
            let Some(next) = self.entry_header.next else {
                return Ok(0);
            };
            // TODO: better error types
            self.page = self.storage.pages.get_page(next.page_index as usize).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("Page storage error: {}", e)))?;
            self.entry_header = FastHeapEntryHeader::load_from(next.entry_offset as usize, &self.page).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("Page storage error: {}", e)))?;
            self.payload_offset = next.entry_offset as usize + FastHeapEntryHeader::SIZE;
            self.payload_remaining = self.entry_header.payload_length as usize;
        }

        let to_read = min(buffer.len(), self.payload_remaining);
        self.page.read(self.payload_offset, &mut buffer[..to_read]).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("Page read error: {}", e)))?;
        self.payload_offset += to_read;
        self.payload_remaining -= to_read;

        Ok(to_read)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::block::memory::MemoryBlockStorage;
    use crate::page::FastPageStorage;

    #[test]
    fn test_heap_storage_insert_and_read() {
        let header = Arc::new(MemoryBlockStorage::allocate(4, 2));
        let pages = Arc::new(MemoryBlockStorage::allocate(512, 8));

        let page_storage = Arc::new(FastPageStorage::new(header, pages).unwrap());
        let mut heap_storage = Arc::new(FastHeapStorage::new(page_storage));

        let data = b"Hello, world! This is a test of the heap storage system.";
        heap_storage.insert_entry(0, data).unwrap();

        let heap_iterator = heap_storage.iter_entries(0).unwrap();
        let mut entry_reader = heap_iterator.next().unwrap().unwrap();

        let mut read_data = Vec::new();
        let mut buffer = [0u8; 10];
        loop {
            let bytes_read = entry_reader.read(&mut buffer).unwrap();
            if bytes_read == 0 {
                break;
            }
            read_data.extend_from_slice(&buffer[..bytes_read]);
        }
        assert_eq!(&read_data[..], &data[..]);
    }
}
