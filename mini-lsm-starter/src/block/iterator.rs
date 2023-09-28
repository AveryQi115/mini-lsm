use std::sync::Arc;

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: Vec<u8>,
    /// The corresponding value, can be empty
    value: Vec<u8>,
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
}

impl BlockIterator {
    pub fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: Vec::new(),
            value: Vec::new(),
            idx: 0,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let offset = block.offsets[0];
        let key_len =
            (block.data[offset as usize] as u16) << 8 | block.data[1 + offset as usize] as u16;
        let key = block.data[offset as usize + 2..offset as usize + 2 + key_len as usize].to_vec();
        let val_offset = offset + 2 + key_len;
        let val_len = (block.data[val_offset as usize] as u16) << 8
            | block.data[1 + val_offset as usize] as u16;
        let value = block.data[val_offset as usize + 2..val_offset as usize + 2 + val_len as usize]
            .to_vec();
        Self {
            block,
            key,
            value,
            idx: 0,
        }
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: &[u8]) -> Self {
        let len = block.offsets.len();
        let mut low = 0;
        let mut high = len;
        while low < high {
            let mid = (low + high) / 2;
            let offset = block.offsets[mid];
            let key_len =
                (block.data[offset as usize] as u16) << 8 | block.data[1 + offset as usize] as u16;
            let mid_key = &block.data[offset as usize + 2..offset as usize + 2 + key_len as usize];
            if mid_key < key {
                low = mid + 1;
            } else if mid_key == key {
                let val_offset = offset + 2 + key_len;
                let val_len = (block.data[val_offset as usize] as u16) << 8
                    | block.data[1 + val_offset as usize] as u16;
                let value = block.data
                    [val_offset as usize + 2..val_offset as usize + 2 + val_len as usize]
                    .to_vec();
                let mid_key = mid_key.to_vec();
                return Self {
                    block,
                    key: mid_key,
                    value,
                    idx: mid,
                };
            } else {
                high = mid;
            }
        }

        if low == block.offsets.len() {
            return Self {
                block,
                key: Vec::new(),
                value: Vec::new(),
                idx: len,
            };
        }
        let offset = block.offsets[low];
        let key_len =
            (block.data[offset as usize] as u16) << 8 | block.data[1 + offset as usize] as u16;
        let key = block.data[offset as usize + 2..offset as usize + 2 + key_len as usize].to_vec();
        let val_offset = offset + 2 + key_len;
        let val_len = (block.data[val_offset as usize] as u16) << 8
            | block.data[1 + val_offset as usize] as u16;
        let value = block.data[val_offset as usize + 2..val_offset as usize + 2 + val_len as usize]
            .to_vec();
        Self {
            block,
            key,
            value,
            idx: low,
        }
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        self.key.len() != 0
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        let offset = self.block.offsets[0];
        let key_len = (self.block.data[offset as usize] as u16) << 8
            | self.block.data[1 + offset as usize] as u16;
        let key =
            self.block.data[offset as usize + 2..offset as usize + 2 + key_len as usize].to_vec();
        let val_offset = offset + 2 + key_len;
        let val_len = (self.block.data[val_offset as usize] as u16) << 8
            | self.block.data[1 + val_offset as usize] as u16;
        let value = self.block.data
            [val_offset as usize + 2..val_offset as usize + 2 + val_len as usize]
            .to_vec();
        self.key = key;
        self.value = value;
        self.idx = 0;
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx += 1;
        if self.idx == self.block.offsets.len() {
            self.key = Vec::new();
            self.value = Vec::new();
            return;
        }
        let offset = self.block.offsets[self.idx];
        let key_len = (self.block.data[offset as usize] as u16) << 8
            | self.block.data[1 + offset as usize] as u16;
        let key =
            self.block.data[offset as usize + 2..offset as usize + 2 + key_len as usize].to_vec();
        let val_offset = offset + 2 + key_len;
        let val_len = (self.block.data[val_offset as usize] as u16) << 8
            | self.block.data[1 + val_offset as usize] as u16;
        let value = self.block.data
            [val_offset as usize + 2..val_offset as usize + 2 + val_len as usize]
            .to_vec();
        self.key = key;
        self.value = value;
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by callers.
    pub fn seek_to_key(&mut self, key: &[u8]) {
        let len = self.block.offsets.len();
        let mut low = 0;
        let mut high = len;
        while low < high {
            let mid = (low + high) / 2;
            let offset = self.block.offsets[mid];
            let key_len = (self.block.data[offset as usize] as u16) << 8
                | self.block.data[1 + offset as usize] as u16;
            let mid_key =
                &self.block.data[offset as usize + 2..offset as usize + 2 + key_len as usize];
            if mid_key < key {
                low = mid + 1;
            } else if mid_key == key {
                let val_offset = offset + 2 + key_len;
                let val_len = (self.block.data[val_offset as usize] as u16) << 8
                    | self.block.data[1 + val_offset as usize] as u16;
                let value = self.block.data
                    [val_offset as usize + 2..val_offset as usize + 2 + val_len as usize]
                    .to_vec();

                self.key = mid_key.to_vec();
                self.value = value;
                self.idx = mid;
                return;
            } else {
                high = mid;
            }
        }

        if low == self.block.offsets.len() {
            self.key = Vec::new();
            self.value = Vec::new();
            self.idx = len;
            return;
        }
        let offset = self.block.offsets[low];
        let key_len = (self.block.data[offset as usize] as u16) << 8
            | self.block.data[1 + offset as usize] as u16;
        let key =
            self.block.data[offset as usize + 2..offset as usize + 2 + key_len as usize].to_vec();
        let val_offset = offset + 2 + key_len;
        let val_len = (self.block.data[val_offset as usize] as u16) << 8
            | self.block.data[1 + val_offset as usize] as u16;
        let value = self.block.data
            [val_offset as usize + 2..val_offset as usize + 2 + val_len as usize]
            .to_vec();
        self.key = key;
        self.value = value;
        self.idx = low;
        return;
    }
}
