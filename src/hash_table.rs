use std::io::{self, Read};

pub mod book;
pub mod prefix_hasher;

pub enum HashTableScanFilter<'key> {
    Key(&'key [u8]),
    Hash(Hash),
    All,
}

pub trait HashTable {
    fn insert(&mut self, key: &[u8], value: &[u8]) -> io::Result<()>;
    fn scan(&self, filter: HashTableScanFilter) -> io::Result<impl HashTableScanner>;
}

pub type Hash = u32;

pub trait SliceHasher {
    fn update(&mut self, data: &[u8]);

    /// Returns `Some(true)` if the hash matches, `Some(false)` if it does not match,
    /// or `None` if not enough data has been provided to determine a match.
    /// 
    /// This allows for early rejection of non-matching keys without needing to
    /// compute the full hash.
    /// 
    /// It is not guaranteed that this method will be called before `finalize`.
    /// It may be called multiple times during the hashing process.
    /// 
    /// Also, it is not guaranteed that this method will be implemented to perform
    /// any meaningful comparison; it may always return `None`.
    fn try_compare(&self, _hash: Hash) -> Option<bool> {
        None
    }

    fn finalize(self) -> Hash;
}

pub trait SliceHasherBuilder {
    type Hasher: SliceHasher;
    fn build(&self) -> Self::Hasher;
}

impl<H: SliceHasherBuilder> SliceHasherBuilder for &H {
    type Hasher = H::Hasher;
    fn build(&self) -> Self::Hasher {
        (*self).build()
    }
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
