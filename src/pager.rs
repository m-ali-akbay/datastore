use std::io::{self, Read, Seek, Write};

pub mod memory;
pub mod fs;

pub type PageIndex = u32;

pub type PageSize = u32;

pub trait Pager {
    type Page: Page;

    /// Returns the size of each page in bytes.
    fn page_size(&self) -> PageSize;

    fn page(&self, page_index: PageIndex) -> io::Result<Self::Page>;
}

pub trait Page: Read + Write + Seek + Clone {
    fn index(&self) -> PageIndex;
}
