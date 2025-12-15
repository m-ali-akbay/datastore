use std::{cmp::min, collections::BTreeMap, io::{self, Read, Seek, SeekFrom, Write}, sync::{Arc, RwLock}};

use crate::{book::{Book, Section, SectionIndex, SectionPageIndex}, pager::{PageIndex, Pager}};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct PageKey {
    pub section_index: SectionIndex,
    pub section_page_index: SectionPageIndex,
}

#[derive(Clone)]
pub struct PageHeader {
    pub pager_page_index: PageIndex,
}

pub trait PageRegistry {
    fn try_resolve_page(&self, key: &PageKey) -> io::Result<Option<PageHeader>>;
    fn resolve_page(&self, key: &PageKey) -> io::Result<PageHeader>;
}

pub type PagerBookMemoryHeader = Arc<RwLock<BTreeMap<PageKey, PageHeader>>>;

impl PageRegistry for PagerBookMemoryHeader {
    fn try_resolve_page(&self, key: &PageKey) -> io::Result<Option<PageHeader>> {
        let lock = self.read().map_err(|_| io::Error::new(io::ErrorKind::Other, "Lock poisoned"))?;
        Ok(lock.get(key).cloned())
    }

    fn resolve_page(&self, key: &PageKey) -> io::Result<PageHeader> {
        if let Some(page_header) = self.try_resolve_page(key)? {
            return Ok(page_header);
        }
        let mut lock = self.write().map_err(|_| io::Error::new(io::ErrorKind::Other, "Lock poisoned"))?;
        let pager_page_index = lock.len() as PageIndex;
        Ok(lock.entry(*key).or_insert_with(|| PageHeader { pager_page_index }).clone())
    }
}

#[derive(Clone)]
pub struct PagerBook<Pager, Registry> {
    pager: Pager,
    registry: Registry,
}

impl<P: Pager, R: PageRegistry> PagerBook<P, R> {
    pub fn new(pager: P, registry: R) -> Self {
        Self {
            pager,
            registry,
        }
    }
}

impl<P: Pager + Clone, R: PageRegistry + Clone> Book for PagerBook<P, R> {
    type Section = PagerBookSection<P, R>;

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
pub struct PagerBookSection<P: Pager, R: PageRegistry> {
    book: PagerBook<P, R>,
    section_index: SectionIndex,
    current_page: Option<(P::Page, SectionPageIndex)>,
    section_offset: u64,
}

impl<P: Pager + Clone, R: PageRegistry + Clone> Section for PagerBookSection<P, R> {
    fn index(&self) -> SectionIndex {
        self.section_index
    }
}

impl<P: Pager, R: PageRegistry> PagerBookSection<P, R> {
    fn try_fetch_current_page(&mut self) -> io::Result<()> {
        let section_page_index = (self.section_offset / self.book.pager.page_size() as u64) as SectionPageIndex;
        if let Some((_, current_section_page_index)) = &self.current_page {
            if *current_section_page_index == section_page_index {
                return Ok(());
            }
            self.current_page = None;
        }
        let page_key = PageKey {
            section_index: self.section_index,
            section_page_index,
        };
        if let Some(page_header) = self.book.registry.try_resolve_page(&page_key)? {
            let page = self.book.pager.page(page_header.pager_page_index)?;
            self.current_page = Some((page, section_page_index));
        }
        return Ok(());
    }

    fn get_or_assign_current_page(&mut self) -> io::Result<&mut P::Page> {
        self.try_fetch_current_page()?;
        let Self {
            section_offset,
            book,
            current_page,
            section_index,
        } = self;
        if let Some((page, _)) = current_page {
            return Ok(page);
        }
        let section_page_index = (*section_offset / book.pager.page_size() as u64) as SectionPageIndex;
        let page_key = PageKey {
            section_index: *section_index,
            section_page_index,
        };
        let PageHeader { pager_page_index } = book.registry.resolve_page(&page_key)?;
        let page = book.pager.page(pager_page_index)?;
        *current_page = Some((page, section_page_index));
        Ok(&mut current_page.as_mut().unwrap().0)
    }
}

impl<P: Pager, R: PageRegistry> Read for PagerBookSection<P, R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let page_size = self.book.pager.page_size() as u64;
        let page_offset = self.section_offset % page_size;
        let max_read_size = min(buf.len() as u64, page_size - page_offset) as usize;
        self.try_fetch_current_page()?;
        let read_size = if let Some((page, _)) = self.current_page.as_mut() {
            page.seek(SeekFrom::Start(page_offset))?;
            page.read(&mut buf[..max_read_size])?
        } else {
            buf[..max_read_size].fill(0);
            max_read_size
        };
        self.section_offset += read_size as u64;
        Ok(read_size)
    }
}

impl<P: Pager, H: PageRegistry> Write for PagerBookSection<P, H> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let page_size = self.book.pager.page_size() as u64;
        let page_offset = self.section_offset % page_size;
        let max_write_size = min(buf.len() as u64, page_size - page_offset) as usize;
        let page = self.get_or_assign_current_page()?;
        page.seek(SeekFrom::Start(page_offset))?;
        let written = page.write(&buf[..max_write_size])?;
        self.section_offset += written as u64;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.try_fetch_current_page()?;
        if let Some((page, _)) = self.current_page.as_mut() {
            page.flush()?;
        }
        Ok(())
    }
}

impl<P: Pager, H: PageRegistry> Seek for PagerBookSection<P, H> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let new_offset = match pos {
            SeekFrom::Start(offset) => offset,
            SeekFrom::End(..) => {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "Seek from end not supported"));
            },
            SeekFrom::Current(offset) => {
                if offset >= 0 {
                    self.section_offset.checked_add(offset as u64).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Seek overflow"))?
                } else {
                    self.section_offset.checked_sub(-offset as u64).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Seek underflow"))?
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
        self.section_offset = if offset >= 0 {
            self.section_offset.checked_add(offset as u64).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Seek overflow"))?
        } else {
            self.section_offset.checked_sub(-offset as u64).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Seek underflow"))?
        };
        Ok(())
    }

    fn stream_position(&mut self) -> io::Result<u64> {
        Ok(self.section_offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pager::{memory::MemoryPager, PageSize};
    use std::io::{Read, Seek, SeekFrom, Write};

    fn create_test_book(page_size: PageSize) -> PagerBook<MemoryPager, PagerBookMemoryHeader> {
        PagerBook::new(MemoryPager::new(page_size), Arc::new(RwLock::new(BTreeMap::new())))
    }

    #[test]
    fn test_basic_read_write() -> io::Result<()> {
        let book = create_test_book(1024);
        let mut section = book.section(0);

        section.write(b"Hello, World!")?;
        section.rewind()?;
        
        let mut buffer = vec![0u8; 13];
        section.read(&mut buffer)?;
        assert_eq!(&buffer, b"Hello, World!");
        Ok(())
    }

    #[test]
    fn test_multi_page_operations() -> io::Result<()> {
        let book = create_test_book(64);
        let mut section = book.section(0);

        // Write across multiple pages
        let data = vec![42u8; 200];
        let mut offset = 0;
        while offset < data.len() {
            offset += section.write(&data[offset..])?;
        }

        // Read back
        section.rewind()?;
        let mut buffer = vec![0u8; 200];
        let mut offset = 0;
        while offset < buffer.len() {
            let read = section.read(&mut buffer[offset..])?;
            if read == 0 { break; }
            offset += read;
        }
        assert_eq!(buffer, data);
        Ok(())
    }

    #[test]
    fn test_seeking() -> io::Result<()> {
        let book = create_test_book(1024);
        let mut section = book.section(0);

        section.write(b"0123456789ABCDEF")?;

        section.seek(SeekFrom::Start(5))?;
        let mut buf = [0u8; 3];
        section.read(&mut buf)?;
        assert_eq!(&buf, b"567");

        section.seek(SeekFrom::Current(2))?;
        section.read(&mut buf)?;
        assert_eq!(&buf, b"ABC");

        assert!(section.seek(SeekFrom::End(0)).is_err());
        Ok(())
    }

    #[test]
    fn test_multiple_sections() -> io::Result<()> {
        let book = create_test_book(1024);
        
        let mut s0 = book.section(0);
        let mut s1 = book.section(1);
        
        s0.write(b"Section0")?;
        s1.write(b"Section1")?;

        s0.rewind()?;
        s1.rewind()?;

        let mut buf = vec![0u8; 8];
        s0.read(&mut buf)?;
        assert_eq!(&buf, b"Section0");
        
        s1.read(&mut buf)?;
        assert_eq!(&buf, b"Section1");
        Ok(())
    }

    #[test]
    fn test_sparse_pages() -> io::Result<()> {
        let book = create_test_book(64);
        let mut section = book.section(0);

        section.write(b"Page0")?;
        section.seek(SeekFrom::Start(128))?; // Skip to page 2
        section.write(b"Page2")?;

        // Read from different section (unallocated, should be zeros)
        let mut other_section = book.section(1);
        let mut buf = [0u8; 5];
        other_section.read(&mut buf)?;
        assert_eq!(buf, [0u8; 5]);

        // Verify page 0 and 2
        section.seek(SeekFrom::Start(0))?;
        section.read(&mut buf)?;
        assert_eq!(&buf, b"Page0");

        section.seek(SeekFrom::Start(128))?;
        section.read(&mut buf)?;
        assert_eq!(&buf, b"Page2");
        Ok(())
    }

    #[test]
    fn test_overwrite() -> io::Result<()> {
        let book = create_test_book(1024);
        let mut section = book.section(0);

        section.write(b"XXXXXXXXXX")?;
        section.seek(SeekFrom::Start(2))?;
        section.write(b"YYY")?;

        section.rewind()?;
        let mut buf = [0u8; 10];
        section.read(&mut buf)?;
        assert_eq!(&buf, b"XXYYYXXXXX");
        Ok(())
    }

    #[test]
    fn test_seek_errors() -> io::Result<()> {
        let book = create_test_book(1024);
        let mut section = book.section(0);

        // Overflow
        section.seek(SeekFrom::Start(u64::MAX - 100))?;
        assert!(section.seek(SeekFrom::Current(1000)).is_err());

        // Underflow
        section.seek(SeekFrom::Start(0))?;
        assert!(section.seek(SeekFrom::Current(-10)).is_err());
        assert!(section.seek_relative(-10).is_err());
        Ok(())
    }

    #[test]
    fn test_independent_section_positions() -> io::Result<()> {
        let book = create_test_book(1024);
        let mut s1 = book.section(0);
        let mut s2 = book.section(0);

        s1.write(b"Test")?;
        assert_eq!(s1.stream_position()?, 4);
        assert_eq!(s2.stream_position()?, 0);

        let mut buf = [0u8; 2];
        s2.read(&mut buf)?;
        assert_eq!(&buf, b"Te");
        assert_eq!(s2.stream_position()?, 2);
        assert_eq!(s1.stream_position()?, 4);
        Ok(())
    }

    #[test]
    fn test_heavy_sparse_write_read() -> io::Result<()> {
        let book = create_test_book(8);

        for size in [5, 10].into_iter() {
            for section_index in 0..2 {
                let mut data = vec![0u8; size];
                for i in 0..data.len() {
                    data[i] = ((i + section_index) % 256) as u8;
                }

                let mut section = book.section(section_index as SectionIndex);
                section.write_all(&data)?;
            }
        }

        for section_index in 0..2 {
            let mut section = book.section(section_index as SectionIndex);

            let mut data = vec![0u8; 10];
            for i in 0..data.len() {
                data[i] = ((i + section_index) % 256) as u8;
            }

            let mut read_back = vec![0u8; data.len()];
            section.read_exact(&mut read_back)?;
            assert_eq!(data, read_back);
        }

        Ok(())
    }
}
