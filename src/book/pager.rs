use std::{cmp::min, collections::BTreeMap, io::{self, Read, Seek, Write}, sync::{Arc, RwLock}};

use crate::{book::{Book, Section, SectionIndex, SectionPageIndex}, pager::{Page, PageIndex, Pager}};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct PageKey {
    pub section_index: SectionIndex,
    pub section_page_index: SectionPageIndex,
}

pub struct PageHeader {
    pub pager_page_index: PageIndex,
}

struct Inner {
    pages: BTreeMap<PageKey, PageHeader>,
}

#[derive(Clone)]
pub struct PagerBook<Pager> {
    pager: Pager,
    inner: Arc<RwLock<Inner>>,
}

impl<P: Pager> PagerBook<P> {
    pub fn new(pager: P) -> Self {
        Self {
            pager,
            inner: Arc::new(RwLock::new(Inner {
                pages: BTreeMap::new(),
            })),
        }
    }

    pub fn load(
        pager: P,
        pages: impl Iterator<Item = (PageKey, PageHeader)>,
    ) -> io::Result<Self> {
        Ok(Self {
            pager,
            inner: Arc::new(RwLock::new(Inner {
                pages: pages.collect(),
            })),
        })
    }

    pub fn export<T>(
        &self,
        callback: impl FnOnce(
            &P,
            &mut dyn Iterator<Item = (PageKey, &PageHeader)>,
        ) -> T,
    ) -> io::Result<T> {
        let inner = self.inner.read().map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Poisoned lock"))?;
        let mut pages = inner.pages.iter().map(|(k, v)| (*k, v));
        Ok(callback(&self.pager, &mut pages))
    }
}

impl<P: Pager + Clone> Book for PagerBook<P> {
    type Section = PagerBookSection<P>;

    fn section(&self, section_index: SectionIndex) -> Self::Section {
        PagerBookSection {
            book: self.clone(),
            section_index,
            current_page: None,
            section_offset: 0,
        }
    }
}

#[derive(Clone)]
pub struct PagerBookSection<P: Pager> {
    book: PagerBook<P>,
    section_index: SectionIndex,
    current_page: Option<P::Page>,
    section_offset: u64,
}

impl<P: Pager + Clone> Section for PagerBookSection<P> {
    fn index(&self) -> SectionIndex {
        self.section_index
    }
}

impl<P: Pager> PagerBookSection<P> {
    fn try_get_current_page(&mut self) -> io::Result<Option<P::Page>> {
        let section_page_index = (self.section_offset / self.book.pager.page_size() as u64) as SectionPageIndex;
        if let Some(page) = &self.current_page {
            if page.index() == section_page_index {
                return Ok(Some(page.clone()));
            }
            self.current_page = None;
        }
        let inner = self.book.inner.read().map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Poisoned lock"))?;
        let page_key = PageKey {
            section_index: self.section_index,
            section_page_index,
        };
        if let Some(page_header) = inner.pages.get(&page_key) {
            let page = self.book.pager.page(page_header.pager_page_index)?;
            self.current_page = Some(page.clone());
            return Ok(Some(page));
        }
        Ok(None)
    }

    fn get_or_assign_current_page(&mut self) -> io::Result<P::Page> {
        if let Some(page) = self.try_get_current_page()? {
            return Ok(page);
        }
        let section_page_index = (self.section_offset / self.book.pager.page_size() as u64) as SectionPageIndex;
        let mut inner = self.book.inner.write().map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Poisoned lock"))?;
        let page_key = PageKey {
            section_index: self.section_index,
            section_page_index,
        };
        let pager_page_index = inner.pages.len() as PageIndex;
        let page = self.book.pager.page(pager_page_index)?;
        inner.pages.insert(page_key, PageHeader { pager_page_index });
        self.current_page = Some(page.clone());
        Ok(page)
    }
}

impl<P: Pager> Read for PagerBookSection<P> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let page_size = self.book.pager.page_size() as u64;
        let page_offset = self.section_offset % page_size;
        let max_read_size = min(buf.len() as u64, page_size - page_offset) as usize;
        let read_size = if let Some(mut page) = self.try_get_current_page()? {
            page.seek(io::SeekFrom::Start(page_offset))?;
            page.read(&mut buf[..max_read_size])?
        } else {
            buf[..max_read_size].fill(0);
            max_read_size
        };
        self.section_offset += read_size as u64;
        Ok(read_size)
    }
}

impl<P: Pager> Write for PagerBookSection<P> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let page_size = self.book.pager.page_size() as u64;
        let page_offset = self.section_offset % page_size;
        let max_write_size = min(buf.len() as u64, page_size - page_offset) as usize;
        let mut page = self.get_or_assign_current_page()?;
        page.seek(io::SeekFrom::Start(page_offset))?;
        let written = page.write(&buf[..max_write_size])?;
        self.section_offset += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        if let Some(mut page) = self.try_get_current_page()? {
            page.flush()?;
        }
        Ok(())
    }
}

impl<P: Pager> Seek for PagerBookSection<P> {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        let section_size = self.section_offset;
        let new_offset = match pos {
            io::SeekFrom::Start(offset) => offset,
            io::SeekFrom::End(..) => {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "Seek from end not supported"));
            },
            io::SeekFrom::Current(offset) => {
                if offset >= 0 {
                    section_size.checked_add(offset as u64).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Seek overflow"))?
                } else {
                    section_size.checked_sub(-offset as u64).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Seek underflow"))?
                }
            },
        };
        self.section_offset = new_offset;
        Ok(self.section_offset)
    }

    fn rewind(&mut self) -> io::Result<()> {
        self.section_offset = 0;
        Ok(())
    }

    fn seek_relative(&mut self, offset: i64) -> io::Result<()> {
        if offset >= 0 {
            self.section_offset = self.section_offset.checked_add(offset as u64).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Seek overflow"))?;
        } else {
            self.section_offset = self.section_offset.checked_sub(-offset as u64).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Seek underflow"))?;
        }
        Ok(())
    }

    fn stream_position(&mut self) -> io::Result<u64> {
        Ok(self.section_offset)
    }
}
