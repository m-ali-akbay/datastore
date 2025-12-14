use crate::hash_table::{Hash, SliceHasher};

use super::SliceHasherBuilder;

const HASH_BYTES: usize = Hash::BITS as usize / 8;

pub struct PrefixHasher {
    buffer: [u8; HASH_BYTES],
    offset: usize,
}

impl PrefixHasher {
    pub fn new() -> Self {
        Self {
            buffer: [0u8; HASH_BYTES],
            offset: 0,
        }
    }
}

impl SliceHasher for PrefixHasher {
    fn update(&mut self, data: &[u8]) {
        let len = data.len().min(HASH_BYTES - self.offset);
        if len == 0 {
            return;
        }
        self.buffer[self.offset..self.offset + len].copy_from_slice(&data[..len]);
        self.offset += len;
    }

    fn finalize(self) -> super::Hash {
        super::Hash::from_le_bytes(self.buffer)
    }
}

pub struct PrefixHasherBuilder;

impl SliceHasherBuilder for PrefixHasherBuilder {
    type Hasher = PrefixHasher;

    fn build(&self) -> Self::Hasher {
        PrefixHasher::new()
    }
}
