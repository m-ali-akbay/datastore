use std::{fs::File, io::{self, Read, Seek, Write}, sync::{Arc, Mutex}};

use crate::pager::{Page, PageSize, Pager};

use super::PageIndex;

struct FilePagerResource {
    file: File,
    size: u64,
}

struct FilePagerInner {
    page_size: PageSize,
    resource: Mutex<FilePagerResource>,
}

#[derive(Clone)]
pub struct FilePager {
    inner: Arc<FilePagerInner>,
}

#[derive(Clone)]
pub struct FilePage {
    index: PageIndex,
    pager: FilePager,
    page_offset: u64,
    file_offset: u64,
}

impl FilePager {
    pub fn new(file: File, page_size: PageSize) -> io::Result<Self> {
        let size = file.metadata()?.len();
        Ok(Self {
            inner: Arc::new(FilePagerInner {
                page_size,
                resource: Mutex::new(FilePagerResource { file, size }),
            }),
        })
    }

    pub fn flush(&self) -> io::Result<()> {
        let mut resource = self.inner.resource.lock().map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Poisoned lock"))?;
        resource.file.flush()
    }
}

impl Pager for FilePager {
    type Page = FilePage;

    fn page(&self, page_index: PageIndex) -> io::Result<Self::Page> {
        Ok(FilePage {
            index: page_index,
            pager: self.clone(),
            page_offset: 0,
            file_offset: (page_index as u64).checked_mul(self.page_size() as u64).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "File offset overflow"))?,
        })
    }

    fn page_size(&self) -> PageSize {
        self.inner.page_size
    }
}

impl Page for FilePage {
    fn index(&self) -> PageIndex {
        self.index
    }
}

impl Read for FilePage {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let page_size = self.pager.page_size() as u64;
        if self.page_offset == page_size {
            return Ok(0);
        }
        let read_size = (page_size - self.page_offset).min(buf.len() as u64) as usize;
        if read_size == 0 {
            return Ok(0);
        }
        let mut resource = self.pager.inner.resource.lock().map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Poisoned lock"))?;
        if self.file_offset >= resource.size {
            buf[..read_size].fill(0);
            self.page_offset += read_size as u64;
            self.file_offset += read_size as u64;
            return Ok(read_size);
        }
        resource.file.seek(io::SeekFrom::Start(self.file_offset))?;
        let read_size = resource.file.read(&mut buf[..read_size])?;
        self.page_offset += read_size as u64;
        self.file_offset += read_size as u64;
        Ok(read_size)
    }
}

impl Write for FilePage {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let page_size = self.pager.page_size() as u64;
        let write_size = (page_size - self.page_offset).min(buf.len() as u64) as usize;
        if write_size == 0 {
            return Ok(0);
        }
        let mut resource = self.pager.inner.resource.lock().map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Poisoned lock"))?;
        resource.file.seek(io::SeekFrom::Start(self.file_offset))?;
        let written = resource.file.write(&buf[..write_size])?;
        self.page_offset += written as u64;
        self.file_offset += written as u64;
        if self.file_offset > resource.size {
            resource.size = self.file_offset;
        }
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut resource = self.pager.inner.resource.lock().map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Poisoned lock"))?;
        resource.file.flush()
    }
}

impl Seek for FilePage {
    fn seek(&mut self, pos: io::SeekFrom) -> io::Result<u64> {
        let page_size = self.pager.page_size() as u64;
        let (anchor, offset, is_forward) = match pos {
            io::SeekFrom::Start(offset) => (0u64, offset, true),
            io::SeekFrom::End(offset @ 0..) => (page_size, offset as u64, true),
            io::SeekFrom::End(offset @ ..0) => (page_size, -offset as u64, false),
            io::SeekFrom::Current(offset @ 0..) => (self.page_offset, offset as u64, true),
            io::SeekFrom::Current(offset @ ..0) => (self.page_offset, -offset as u64, false),
        };
        let new_page_offset = if is_forward {
            anchor.checked_add(offset).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Seek overflow"))?
        } else {
            anchor.checked_sub(offset).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Seek underflow"))?
        };
        if new_page_offset > page_size {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Seek out of bounds"));
        }
        let new_file_offset = (self.index as u64).checked_mul(page_size).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "File offset overflow"))?
            .checked_add(new_page_offset).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "File offset overflow"))?;
        self.page_offset = new_page_offset;
        self.file_offset = new_file_offset;
        Ok(self.page_offset)
    }

    fn rewind(&mut self) -> io::Result<()> {
        self.page_offset = 0;
        self.file_offset = (self.index as u64) * (self.pager.page_size() as u64);
        Ok(())
    }

    fn stream_position(&mut self) -> io::Result<u64> {
        Ok(self.page_offset)
    }

    fn seek_relative(&mut self, offset: i64) -> io::Result<()> {
        let page_size = self.pager.page_size() as u64;
        let new_page_offset = if offset >= 0 {
            let new_page_offset = self.page_offset.checked_add(offset as u64).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Seek overflow"))?;
            if new_page_offset > page_size {
                return Err(io::Error::new(io::ErrorKind::InvalidInput, "Seek out of bounds"));
            }
            new_page_offset
        } else {
            self.page_offset.checked_sub(-offset as u64).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Seek underflow"))?
        };
        self.page_offset = new_page_offset;
        self.file_offset = (self.index as u64).checked_mul(page_size).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "File offset overflow"))?
            .checked_add(new_page_offset).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "File offset overflow"))?;
        Ok(())
    }
}
