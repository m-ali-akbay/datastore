use std::{fs::{create_dir_all, rename}, io::{self, Write}, path::{Path, PathBuf}};

use crate::{book::{self, SectionIndex, SectionPageIndex, pager::PagerBook}, hash_table::{self, HashTable, book::BookHashTable, prefix_hasher::PrefixHasherBuilder}, pager::{PageIndex, PageSize, fs::FilePager}};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct HashTableConfig {
    pub page_size: PageSize,
    pub section_count: SectionIndex,
}

impl Default for HashTableConfig {
    fn default() -> Self {
        HashTableConfig {
            page_size: 4096,
            section_count: 1024,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct SectionHeader {
    index: SectionIndex,
    end_offset: u64,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct PageHeader {
    section_index: SectionIndex,
    page_index: PageIndex,
    section_page_index: SectionPageIndex,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Header {
    #[serde(flatten)]
    config: HashTableConfig,
    sections: Vec<SectionHeader>,
    pages: Vec<PageHeader>,
}

type THashTable = BookHashTable<PrefixHasherBuilder, PagerBook<FilePager>>;

pub struct ManagedHashTable {
    dir_path: PathBuf,
    config: HashTableConfig,
    hash_table: THashTable,
}

impl ManagedHashTable {
    pub fn open(dir_path: impl AsRef<Path>, config: HashTableConfig) -> io::Result<Self> {
        create_dir_all(&dir_path)?;

        let header_path = dir_path.as_ref().join("header.json");

        let header = if header_path.try_exists()? {
            let header_file = std::fs::OpenOptions::new()
                .read(true)
                .open(&header_path)?;
            let header: Header = serde_json::from_reader(&header_file)
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, format!("Failed to parse metadata: {}", err)))?;

            if header.config.page_size != config.page_size {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "Page size in metadata does not match the provided configuration"));
            }

            if header.config.section_count != config.section_count {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "Section count in metadata does not match the provided configuration"));
            }

            header
        } else {
            let header_file = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(&header_path)?;
            let header = Header {
                config,
                sections: Vec::new(),
                pages: Vec::new(),
            };

            serde_json::to_writer_pretty(&header_file, &header)
                .map_err(|err| io::Error::new(io::ErrorKind::Other, format!("Failed to write metadata: {}", err)))?;

            header
        };

        let pages_path = dir_path.as_ref().join("pages.dat");

        let pages_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&pages_path)?;

        let pager = FilePager::new(pages_file, header.config.page_size)?;

        let book = PagerBook::load(
            pager,
            header.pages.iter().map(|ph| (book::pager::PageKey {
                section_index: ph.section_index,
                section_page_index: ph.section_page_index,
            }, book::pager::PageHeader {
                pager_page_index: ph.page_index,
            })),
        )?;

        let hash_table = BookHashTable::load(
            PrefixHasherBuilder,
            book,
            header.config.section_count,
            header.sections.iter().map(|sh| (sh.index, hash_table::book::SectionHeader {
                end_offset: sh.end_offset,
            })),
        )?;

        Ok(ManagedHashTable {
            dir_path: dir_path.as_ref().to_path_buf(),
            config: header.config,
            hash_table,
        })
    }
}

impl ManagedHashTable {
    pub fn save(&mut self) -> io::Result<()> {
        let (sections, pages) = self.hash_table.export(|book, sections| -> io::Result<_> {
            let pages = book.export(|pager, pages| -> io::Result<_> {
                pager.flush()?;
                Ok(pages.map(|(page_key, page_header)| {
                    PageHeader {
                        section_index: page_key.section_index,
                        section_page_index: page_key.section_page_index,
                        page_index: page_header.pager_page_index,
                    }
                }).collect())
            })??;

            let sections = sections.map(|(section_index, section_header)| {
                SectionHeader {
                    index: section_index,
                    end_offset: section_header.end_offset,
                }
            }).collect();

            Ok((sections, pages))
        })??;

        let header = Header {
            config: self.config.clone(),
            sections,
            pages,
        };

        let tmp_header_path = self.dir_path.join("header.json.tmp");
        let mut tmp_header_file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp_header_path)?;

        serde_json::to_writer_pretty(&mut tmp_header_file, &header)
            .map_err(|err| io::Error::new(io::ErrorKind::Other, format!("Failed to write metadata: {}", err)))?;

        tmp_header_file.flush()?;
        rename(&tmp_header_path, &self.dir_path.join("header.json"))?;

        Ok(())
    }
}

impl HashTable for ManagedHashTable {
    fn insert(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.hash_table.insert(key, value)
    }

    fn scan(&self, filter: hash_table::HashTableScanFilter) -> io::Result<impl hash_table::HashTableScanner> {
        self.hash_table.scan(filter)
    }
}
