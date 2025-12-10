use std::sync::{Arc, Mutex};

use crate::{block::{Block, BlockStorage, BlockStorageError}};

#[derive(thiserror::Error, Debug)]
pub enum PageStorageError {
    #[error("Block storage error: {0}")]
    BlockStorageError(#[from] BlockStorageError),
    #[error("Page size exceeds")]
    PageSizeExceeds,
    #[error("Header size is invalid")]
    InvalidHeaderSize,
    #[error("Poisoned lock")]
    PoisonedLock,
    #[error("Out of bounds")]
    OutOfBounds,
}

pub trait Page {
    fn index(&self) -> usize;
    fn occupied_size(&self) -> Result<usize, PageStorageError>;
    fn free_size(&self) -> Result<usize, PageStorageError>;
    fn read<'buf>(&self, offset: usize, buffer: &'buf mut [u8]) -> Result<(), PageStorageError>;
    fn write(&self, buffer: &[u8]) -> Result<(), PageStorageError>;
    fn append(&self, buffer: &[u8]) -> Result<(), PageStorageError>;
}

pub trait PageStorage {
    type Page: Page;

    fn page_size(&self) -> usize;
    fn page_count(&self) -> usize;
    fn get_page(&self, index: usize) -> Result<Self::Page, PageStorageError>;
}

pub type OccupiedSize = u16;
pub const OCCUPIED_SIZE_BYTES: usize = OccupiedSize::BITS as usize / 8;

struct FastPageStorageResources<Header: BlockStorage, Pages: BlockStorage> {
    header: Header,
    pages: Pages,
}

pub struct FastPageStorage<Header: BlockStorage, Pages: BlockStorage> {
    // NOTE: this is segregated to avoid cyclic references between FastPageStorage and FastPage/CacheEntry
    resources: Arc<FastPageStorageResources<Header, Pages>>,

    page_count: usize,

    // TODO: make this RW lock per page
    cache: Mutex<Vec<CacheEntry<Header, Pages>>>,
}

impl<Header: BlockStorage, Pages: BlockStorage> FastPageStorage<Header, Pages> {
    pub fn new(
        header: Header,
        pages: Pages,
    ) -> Result<Self, PageStorageError> {
        if pages.block_size() > u16::MAX as usize {
            return Err(PageStorageError::PageSizeExceeds);
        }

        if header.block_size() % OCCUPIED_SIZE_BYTES != 0 {
            return Err(PageStorageError::InvalidHeaderSize);
        }

        let page_count_from_header = (header.block_count() * header.block_size()) / OCCUPIED_SIZE_BYTES;
        let page_count = pages.block_count().min(page_count_from_header);

        Ok(FastPageStorage {
            resources: Arc::new(FastPageStorageResources { header, pages }),
            page_count,
            cache: Mutex::new(Vec::new()),
        })
    }
}

impl<Header: BlockStorage, Pages: BlockStorage> PageStorage for Arc<FastPageStorage<Header, Pages>> {
    type Page = FastPage<Header, Pages>;

    fn page_size(&self) -> usize {
        self.resources.pages.block_size()
    }

    fn page_count(&self) -> usize {
        self.page_count
    }

    fn get_page(&self, index: usize) -> Result<FastPage<Header, Pages>, PageStorageError> {
        let mut cache = self.cache.lock().map_err(|_| PageStorageError::PoisonedLock)?;
        let cache_entry = match cache.iter_mut().find(|entry| entry.page_index == index) {
            Some(entry) => entry,
            None => {
                let header_offset = index * OCCUPIED_SIZE_BYTES;
                let header_block_index = header_offset / self.resources.header.block_size();
                let header_block_offset = header_offset % self.resources.header.block_size();

                let header_block = self.resources.header.get_block(header_block_index)?;

                cache.push(CacheEntry {
                    page_index: index,
                    page_size: self.resources.pages.block_size(),
                    header_block,
                    header_offset: header_block_offset,
                    page_block: None,
                    references: 0,
                });
                cache.last_mut().unwrap()
            },
        };
        cache_entry.references += 1;

        Ok(FastPage {
            storage: self.clone(),
            page_index: index,
        })
    }
}

pub struct FastPage<Header: BlockStorage, Pages: BlockStorage> {
    storage: Arc<FastPageStorage<Header, Pages>>,
    page_index: usize,
}

impl<Header, Pages> Drop for FastPage<Header, Pages>
where
    Header: BlockStorage,
    Pages: BlockStorage,
{
    fn drop(&mut self) {
        let mut cache = self.storage.cache.lock().unwrap();
        let Some((entry_index, entry)) = cache.iter_mut().enumerate().find(|(_, entry)| entry.page_index == self.page_index) else {
            panic!("Cache entry not found for page index {} while drop", self.page_index);
        };
        entry.references -= 1;
        if entry.references == 0 {
            cache.remove(entry_index);
        }
    }
}

impl<Header: BlockStorage, Pages: BlockStorage> FastPage<Header, Pages> {
    fn with_cache_entry<F, R>(&self, f: F) -> Result<R, PageStorageError>
    where
        F: FnOnce(&mut CacheEntry<Header, Pages>) -> Result<R, PageStorageError>,
    {
        let mut cache = self.storage.cache.lock().map_err(|_| PageStorageError::PoisonedLock)?;
        let entry = cache.iter_mut().find(|entry| entry.page_index == self.page_index).expect(format!("Cache entry not found for page index {} while access", self.page_index).as_str());
        f(entry)
    }

    fn with_cache_entry_and_page<F, R>(&self, f: F) -> Result<R, PageStorageError>
    where
        F: FnOnce(&CacheEntry<Header, Pages>, &Pages::Block) -> Result<R, PageStorageError>,
    {
        self.with_cache_entry(|entry| {
            if entry.page_block.is_none() {
                entry.page_block = Some(self.storage.resources.pages.get_block(self.page_index)?);
            }
            let page_block = entry.page_block.as_ref().unwrap();
            f(entry, page_block)
        })
    }
}

impl<Header: BlockStorage, Pages: BlockStorage> Page for FastPage<Header, Pages> {
    fn index(&self) -> usize {
        self.page_index
    }

    fn occupied_size(&self) -> Result<usize, PageStorageError> {
        self.with_cache_entry(|entry| entry.occupied_size())
    }

    fn free_size(&self) -> Result<usize, PageStorageError> {
        self.with_cache_entry(|entry| entry.free_size())
    }

    fn read<'buf>(&self, offset: usize, buffer: &'buf mut [u8]) -> Result<(), PageStorageError> {
        self.with_cache_entry_and_page(|entry, page_block| {
            let occupied_size = entry.occupied_size()?;
            if offset + buffer.len() > occupied_size {
                return Err(PageStorageError::OutOfBounds);
            }
            page_block.read(offset, buffer)?;
            Ok(())
        })
    }

    fn write(&self, buffer: &[u8]) -> Result<(), PageStorageError> {
        self.with_cache_entry_and_page(|entry, page_block| {
            if buffer.len() > page_block.size() {
                return Err(PageStorageError::PageSizeExceeds);
            }
            page_block.write(0, buffer)?;
            entry.write_occupied_size(buffer.len())?;
            Ok(())
        })
    }

    fn append(&self, buffer: &[u8]) -> Result<(), PageStorageError> {
        self.with_cache_entry_and_page(|entry, page_block| {
            let occupied_size = entry.occupied_size()?;
            let free_size = page_block.size() - occupied_size;
            if buffer.len() > free_size {
                return Err(PageStorageError::PageSizeExceeds);
            }
            page_block.write(occupied_size, buffer)?;
            entry.write_occupied_size(occupied_size + buffer.len())?;
            Ok(())
        })
    }
}

struct CacheEntry<Header: BlockStorage, Pages: BlockStorage> {
    page_index: usize,
    page_size: usize,
    header_block: Header::Block,
    header_offset: usize,
    page_block: Option<Pages::Block>,
    references: usize,
}

impl<Header: BlockStorage, Pages: BlockStorage> CacheEntry<Header, Pages> {
    fn occupied_size(&self) -> Result<usize, PageStorageError> {
        let mut size_bytes = [0u8; OCCUPIED_SIZE_BYTES];
        self.header_block.read(self.header_offset, &mut size_bytes)?;
        Ok(OccupiedSize::from_le_bytes(size_bytes) as usize)
    }

    fn free_size(&self) -> Result<usize, PageStorageError> {
        Ok(self.page_size - self.occupied_size()?)
    }

    fn write_occupied_size(&self, size: usize) -> Result<(), PageStorageError> {
        let size_bytes = (size as OccupiedSize).to_le_bytes();
        self.header_block.write(self.header_offset, &size_bytes)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::memory::MemoryBlockStorage;

    #[test]
    fn test_page_storage() {
        let header = [0u8; 16];
        let pages = [0u8; 512];

        let header = Arc::new(MemoryBlockStorage::from_buffer(header, 8).unwrap());
        let pages = Arc::new(MemoryBlockStorage::from_buffer(pages, 64).unwrap());

        let page_storage = Arc::new(FastPageStorage::new(header, pages).unwrap());

        // write page
        let page = page_storage.get_page(0).unwrap();
        let write_data = vec![1u8; 64 - 3];
        page.write(&write_data).unwrap();
        assert_eq!(page.occupied_size().unwrap(), write_data.len());

        // append page
        let page = page_storage.get_page(0).unwrap();
        let append_data = vec![2u8; 2];
        page.append(&append_data).unwrap();
        assert_eq!(page.occupied_size().unwrap(), write_data.len() + append_data.len());

        // read page
        let page = page_storage.get_page(0).unwrap();
        let mut buffer = vec![0u8; write_data.len() + append_data.len()];
        page.read(0, &mut buffer).unwrap();
        assert_eq!(&buffer[..write_data.len()], &write_data[..]);
        assert_eq!(&buffer[write_data.len()..], &append_data[..]);

        // override page
        let page = page_storage.get_page(0).unwrap();
        let override_data = vec![3u8; 3];
        page.write(&override_data).unwrap();
        assert_eq!(page.occupied_size().unwrap(), override_data.len());
        let mut buffer = vec![0u8; override_data.len()];
        page.read(0, &mut buffer).unwrap();
        assert_eq!(&buffer[..override_data.len()], &override_data[..]);

        // page size exceeds
        let page = page_storage.get_page(0).unwrap();
        let large_data = vec![4u8; 64 + 1];
        assert!(matches!(page.write(&large_data), Err(PageStorageError::PageSizeExceeds)));
        assert!(matches!(page.append(&large_data), Err(PageStorageError::PageSizeExceeds)));

        // out of bounds read
        let page = page_storage.get_page(0).unwrap();
        assert!(matches!(page.read(override_data.len(), &mut vec![0u8; 1]), Err(PageStorageError::OutOfBounds)));
        assert!(matches!(page.read(0, &mut vec![0u8; override_data.len() + 1]), Err(PageStorageError::OutOfBounds)));
    }
}
