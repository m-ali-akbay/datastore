use std::io::{Read, Seek, Write};

pub mod pager;

pub type SectionIndex = u32;
pub type SectionPageIndex = u32;

pub trait Book {
    type Section: Section;
    fn section(&self, section_index: SectionIndex) -> Self::Section;
}

pub trait Section: Read + Write + Seek + Clone {
    fn index(&self) -> SectionIndex;
}
