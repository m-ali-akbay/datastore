use std::io::{Read, Seek, Write};

pub mod pager;

pub type SectionIndex = u32;
pub type SectionPageIndex = u32;

pub trait Book {
    type Section<'a>: Section where Self: 'a;
    fn section(&self, section_index: SectionIndex) -> Self::Section<'_>;
}

pub trait Section: Read + Write + Seek + Clone {
    fn index(&self) -> SectionIndex;
}
