use std::io::{self, Read};

pub mod book;
pub mod prefix_hasher;

pub trait HashTable {
    fn insert(&mut self, key: &[u8], value: &[u8]) -> io::Result<()>;
    fn scan_key(&self, key: &[u8]) -> io::Result<impl HashTableScanner>;
    fn scan_hash(&self, hash: Hash) -> io::Result<impl HashTableScanner>;
    fn scan_all(&self) -> io::Result<impl HashTableScanner>;
}

pub type Hash = u32;

pub trait SliceHasher {
    fn update(&mut self, data: &[u8]);
    fn finalize(self) -> Hash;
}

pub trait SliceHasherBuilder {
    type Hasher: SliceHasher;
    fn build(&self) -> Self::Hasher;
}

pub trait HashTableEntry {
    fn key_size(&self) -> u32;
    fn value_size(&self) -> u32;
    fn key(&mut self) -> io::Result<impl Read + '_>;
    fn value(&mut self) -> io::Result<impl Read + '_>;
}

pub trait HashTableScanner {
    fn next(&mut self) -> io::Result<Option<impl HashTableEntry + use<Self>>>;
}
