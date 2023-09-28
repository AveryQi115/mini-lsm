use super::Block;

const KEY_LEN_SIZE: usize = 2;
const VAL_LEN_SIZE: usize = 2;
const OFFSET_SIZE: usize = 2;

struct Entry {
    key: Vec<u8>,
    val: Vec<u8>,
    total_size: u16,
}

/// Builds a block.
pub struct BlockBuilder {
    kvs: Vec<Entry>,
    current_size: usize,
    target_size: usize,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            kvs: Vec::new(),
            current_size: 0,
            target_size: block_size,
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
        let pair_size = KEY_LEN_SIZE + VAL_LEN_SIZE + key.len() + value.len();
        if self.current_size + pair_size + OFFSET_SIZE > self.target_size {
            return false;
        }

        let entry = Entry {
            key: key.to_vec(),
            val: value.to_vec(),
            total_size: pair_size as u16,
        };

        self.kvs.push(entry);
        self.current_size += pair_size + OFFSET_SIZE;
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.kvs.is_empty()
    }

    pub fn size(&self) -> usize {
        self.current_size + 2 // for num of offsets
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        let mut offsets = vec![0u16; self.kvs.len()];
        let mut data: Vec<u8> = Vec::with_capacity(self.current_size - 2 * self.kvs.len());
        let mut cur = 0u16;
        for (i, kv) in self.kvs.iter().enumerate() {
            offsets[i] = cur;
            cur += kv.total_size;
            data.extend_from_slice(&(kv.key.len() as u16).to_be_bytes());
            data.extend_from_slice(kv.key.as_slice());
            data.extend_from_slice(&(kv.val.len() as u16).to_be_bytes());
            data.extend_from_slice(kv.val.as_slice());
        }

        Block { data, offsets }
    }
}
