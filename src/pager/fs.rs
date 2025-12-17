use std::{fs::File, io::{self, Read, Seek, SeekFrom, Write}, sync::Mutex};

use crate::pager::{Page, PageSize, Pager};

use super::PageIndex;

struct FilePagerResource {
    file: File,
    size: u64,
}

pub struct FilePager {
    page_size: PageSize,
    resource: Mutex<FilePagerResource>,
}

#[derive(Clone)]
pub struct FilePage<'a> {
    index: PageIndex,
    pager: &'a FilePager,
    page_offset: u64,
    file_offset: u64,
}

impl FilePager {
    pub fn new(file: File, page_size: PageSize) -> io::Result<Self> {
        let size = file.metadata()?.len();
        Ok(Self {
            page_size,
            resource: Mutex::new(FilePagerResource { file, size }),
        })
    }

    pub fn sync(&self) -> io::Result<()> {
        let resource = self.resource.lock().map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Poisoned lock"))?;
        resource.file.sync_all()
    }
}

impl Pager for FilePager {
    type Page<'a> = FilePage<'a> where Self: 'a;

    fn page<'a>(&'a self, page_index: PageIndex) -> io::Result<Self::Page<'a>> {
        Ok(FilePage {
            index: page_index,
            pager: self,
            page_offset: 0,
            file_offset: (page_index as u64).checked_mul(self.page_size() as u64).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "File offset overflow"))?,
        })
    }

    fn page_size(&self) -> PageSize {
        self.page_size
    }
}

impl Page for FilePage<'_> {
    fn index(&self) -> PageIndex {
        self.index
    }
}

impl Read for FilePage<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let page_size = self.pager.page_size() as u64;
        if self.page_offset == page_size {
            return Ok(0);
        }
        let max_read_size = (page_size - self.page_offset).min(buf.len() as u64) as usize;
        if max_read_size == 0 {
            return Ok(0);
        }
        let mut resource = self.pager.resource.lock().map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Poisoned lock"))?;
        let read_size = if self.file_offset >= resource.size {
            buf[..max_read_size].fill(0);
            self.page_offset += max_read_size as u64;
            self.file_offset += max_read_size as u64;
            max_read_size
        } else {
            resource.file.seek(SeekFrom::Start(self.file_offset))?;
            resource.file.read(&mut buf[..max_read_size])?
        };
        self.page_offset += read_size as u64;
        self.file_offset += read_size as u64;
        Ok(read_size)
    }
}

impl Write for FilePage<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let page_size = self.pager.page_size() as u64;
        let max_write_size = (page_size - self.page_offset).min(buf.len() as u64) as usize;
        if max_write_size == 0 {
            return Ok(0);
        }
        let mut resource = self.pager.resource.lock().map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Poisoned lock"))?;
        resource.file.seek(SeekFrom::Start(self.file_offset))?;
        let write_size = resource.file.write(&buf[..max_write_size])?;
        self.page_offset += write_size as u64;
        self.file_offset += write_size as u64;
        if self.file_offset > resource.size {
            resource.size = self.file_offset;
        }
        Ok(write_size)
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut resource = self.pager.resource.lock().map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "Poisoned lock"))?;
        resource.file.flush()
    }
}

impl Seek for FilePage<'_> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let page_size = self.pager.page_size() as u64;
        let (anchor, offset, is_forward) = match pos {
            SeekFrom::Start(offset) => (0u64, offset, true),
            SeekFrom::End(offset @ 0..) => (page_size, offset as u64, true),
            SeekFrom::End(offset @ ..0) => (page_size, -offset as u64, false),
            SeekFrom::Current(offset @ 0..) => (self.page_offset, offset as u64, true),
            SeekFrom::Current(offset @ ..0) => (self.page_offset, -offset as u64, false),
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
        self.file_offset = (self.index as u64).checked_mul(self.pager.page_size() as u64).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "File offset overflow"))?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Seek, SeekFrom, Write};
    use tempfile::tempfile;

    #[test]
    fn test_file_pager() -> io::Result<()> {
        let file = tempfile()?;
        let pager = FilePager::new(file, 1024)?;

        // Write to page 0
        {
            let mut page0 = pager.page(0)?;
            let data = vec![1u8; 512];
            let written = page0.write(&data)?;
            assert_eq!(written, 512);
            page0.flush()?;
        };

        // Read from page 0
        {
            let mut page0 = pager.page(0)?;
            let mut buffer = vec![0u8; 512];
            let read = page0.read(&mut buffer)?;
            assert_eq!(read, 512);
            assert_eq!(buffer, vec![1u8; 512]);
        };

        // Write to page 1
        {
            let mut page1 = pager.page(1)?;
            let data = vec![2u8; 1024];
            let written = page1.write(&data)?;
            assert_eq!(written, 1024);
            page1.flush()?;
        };

        // Read from page 1
        {
            let mut page1 = pager.page(1)?;
            let mut buffer = vec![0u8; 1024];
            let read = page1.read(&mut buffer)?;
            assert_eq!(read, 1024);
            assert_eq!(buffer, vec![2u8; 1024]);
        };

        // Seek and read
        {
            let mut page0 = pager.page(0)?;
            page0.seek(SeekFrom::Start(256))?;
            let mut buffer = vec![0u8; 256];
            let read = page0.read(&mut buffer)?;
            assert_eq!(read, 256);
            assert_eq!(buffer, vec![1u8; 256]);
        };

        Ok(())
    }

    #[test]
    fn test_file_pager_edge_cases() -> io::Result<()> {
        let file = tempfile()?;
        let pager = FilePager::new(file, 512)?;

        // Read beyond page size
        {
            let mut page0 = pager.page(0)?;
            let mut buffer = vec![0u8; 1024];
            let read = page0.read(&mut buffer)?;
            assert_eq!(read, 512);
        };

        // Write beyond page size
        {
            let mut page0 = pager.page(0)?;
            let data = vec![3u8; 1024];
            let written = page0.write(&data)?;
            assert_eq!(written, 512);
        };

        Ok(())
    }

    #[test]
    fn test_file_page_seeking() -> io::Result<()> {
        let file = tempfile()?;
        let pager = FilePager::new(file, 256)?;

        let mut page = pager.page(0)?;

        // Seek to start
        page.seek(SeekFrom::Start(0))?;
        assert_eq!(page.stream_position()?, 0);

        // Seek to end
        page.seek(SeekFrom::End(0))?;
        assert_eq!(page.stream_position()?, 256);

        // Seek to middle
        page.seek(SeekFrom::Start(128))?;
        assert_eq!(page.stream_position()?, 128);

        // Seek forward from current
        page.seek(SeekFrom::Current(64))?;
        assert_eq!(page.stream_position()?, 192);

        // Seek backward from current
        page.seek(SeekFrom::Current(-32))?;
        assert_eq!(page.stream_position()?, 160);

        // Rewind
        page.rewind()?;
        assert_eq!(page.stream_position()?, 0);

        Ok(())
    }
}
