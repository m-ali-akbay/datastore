use std::{collections::BTreeMap, io::{self, Read, Seek, Write}, sync::{Arc, RwLock}};

use crate::pager::{Page, PageIndex, PageSize, Pager};

struct MemoryPagerInner {
    page_size: PageSize,
    pages: RwLock<BTreeMap<PageIndex, Arc<RwLock<Box<[u8]>>>>>,
}

#[derive(Clone)]
pub struct MemoryPager {
    inner: Arc<MemoryPagerInner>,
}

impl MemoryPager {
    pub fn new(page_size: PageSize) -> Self {
        Self {
            inner: Arc::new(MemoryPagerInner {
                page_size,
                pages: RwLock::new(BTreeMap::new()),
            }),
        }
    }
}

impl Pager for MemoryPager {
    type Page = MemoryPage;

    fn page_size(&self) -> PageSize {
        self.inner.page_size
    }
    
    fn page(&self, page_index: PageIndex) -> io::Result<Self::Page> {
        return Ok(MemoryPage {
            index: page_index,
            pager: self.clone(),
            page: None,
            offset: 0,
        });
    }
}

#[derive(Clone)]
pub struct MemoryPage {
    index: PageIndex,
    pager: MemoryPager,
    page: Option<Arc<RwLock<Box<[u8]>>>>,
    offset: u64,
}

impl MemoryPage {
    fn try_get(&mut self) -> io::Result<Option<Arc<RwLock<Box<[u8]>>>>> {
        if let Some(page) = &self.page {
            return Ok(Some(page.clone()));
        }
        let pages = self.pager.inner.pages.read().map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Poisoned lock"))?;
        if let Some(page) = pages.get(&self.index) {
            self.page = Some(page.clone());
            return Ok(Some(page.clone()));
        }
        Ok(None)
    }

    fn get_or_create(&mut self) -> io::Result<Arc<RwLock<Box<[u8]>>>> {
        if let Some(page) = self.try_get()? {
            return Ok(page);
        }
        drop(self.page.take());
        let mut pages = self.pager.inner.pages.write().map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Poisoned lock"))?;
        let page = pages.entry(self.index).or_insert_with(|| {
            Arc::new(RwLock::new(vec![0u8; self.pager.inner.page_size as usize].into_boxed_slice()))
        });
        self.page = Some(page.clone());
        Ok(page.clone())
    }   
}

impl Page for MemoryPage {
    fn index(&self) -> PageIndex {
        self.index
    }
}

impl Read for MemoryPage {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let page_size = self.pager.page_size() as u64;
        if self.offset == page_size {
            return Ok(0);
        }
        let read_size = (page_size - self.offset).min(buf.len() as u64) as usize;
        let end = self.offset as usize + read_size;
        match self.try_get()? {
            Some(page) => {
                let page = page.read().map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Poisoned lock"))?;
                buf[..read_size].copy_from_slice(&page[self.offset as usize..end]);
            },
            None => {
                buf[..read_size].fill(0);
            },
        };
        self.offset = end as u64;
        Ok(read_size)
    }
}

impl Write for MemoryPage {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let page_size = self.pager.page_size() as u64;
        let write_size = (page_size - self.offset).min(buf.len() as u64) as usize;
        let page = self.get_or_create()?;
        let mut page = page.write().map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Poisoned lock"))?;
        let end = self.offset as usize + write_size;
        page[self.offset as usize..end]
            .copy_from_slice(&buf[..write_size]);
        self.offset = end as u64;
        Ok(write_size)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Seek for MemoryPage {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        let page_size = self.pager.page_size() as u64;
        let (anchor, offset, is_forward) = match pos {
            io::SeekFrom::Start(offset) => (0u64, offset, true),
            io::SeekFrom::End(offset @ 0..) => (page_size, offset as u64, true),
            io::SeekFrom::End(offset @ ..0) => (page_size, -offset as u64, false),
            io::SeekFrom::Current(offset @ 0..) => (self.offset, offset as u64, true),
            io::SeekFrom::Current(offset @ ..0) => (self.offset, -offset as u64, false),
        };
        let new_offset = if is_forward {
            anchor.checked_add(offset).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Seek overflow"))?
        } else {
            anchor.checked_sub(offset).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Seek underflow"))?
        };
        if new_offset > page_size {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Seek out of bounds"));
        }
        self.offset = new_offset;
        Ok(self.offset)
    }

    fn rewind(&mut self) -> io::Result<()> {
        self.offset = 0;
        Ok(())
    }

    fn stream_position(&mut self) -> io::Result<u64> {
        Ok(self.offset)
    }

    fn seek_relative(&mut self, offset: i64) -> io::Result<()> {
        let page_size = self.pager.page_size() as u64;
        if offset >= 0 {
            let new_offset = self.offset.checked_add(offset as u64).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Seek overflow"))?;
            if new_offset > page_size {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "Seek out of bounds"));
            }
            self.offset = new_offset;
        } else {
            let new_offset = self.offset.checked_sub(-offset as u64).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Seek underflow"))?;
            self.offset = new_offset;
        }
        Ok(())
    }
}
