#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use anyhow::Result;

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    block_idx: usize,
    cur_block_iterator: BlockIterator,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let block = table.read_block(0)?;
        let cur_block_iterator = BlockIterator::create_and_seek_to_first(block);
        Ok(Self {
            table,
            block_idx: 0,
            cur_block_iterator,
        })
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        let block = self.table.read_block(0)?;
        self.block_idx = 0;
        self.cur_block_iterator = BlockIterator::create_and_seek_to_first(block);
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: &[u8]) -> Result<Self> {
        let mut low = 0;
        let mut high = table.block_metas.len();
        while low < high {
            let mid = (low + high) / 2;
            if table.block_metas[mid].first_key.as_ref() > key {
                high = mid;
            } else {
                low = mid + 1;
            }
        }
        if low == 0 {
            return Self::create_and_seek_to_first(table);
        }
        let mut block = table.read_block(low - 1)?;
        let mut cur_block_iterator = BlockIterator::create_and_seek_to_key(block, key);
        let mut block_idx = low - 1;
        if !cur_block_iterator.is_valid() {
            if low >= table.block_metas.len() {
                return Ok(Self {
                    table,
                    block_idx: low,
                    cur_block_iterator,
                });
            }
            block_idx += 1;
            block = table.read_block(low)?;
            cur_block_iterator = BlockIterator::create_and_seek_to_first(block);
        }
        Ok(Self {
            table,
            block_idx,
            cur_block_iterator,
        })
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing this function.
    pub fn seek_to_key(&mut self, key: &[u8]) -> Result<()> {
        let mut low = 0;
        let mut high = self.table.block_metas.len();
        while low < high {
            let mid = (low + high) / 2;
            if self.table.block_metas[mid].first_key.as_ref() > key {
                high = mid;
            } else {
                low = mid + 1;
            }
        }
        if low == 0 {
            self.seek_to_first()?;
            return Ok(());
        }
        let mut block = self.table.read_block(low - 1)?;
        self.block_idx = low - 1;
        self.cur_block_iterator = BlockIterator::create_and_seek_to_key(block, key);
        if !self.cur_block_iterator.is_valid() {
            if low >= self.table.block_metas.len() {
                return Ok(());
            }
            block = self.table.read_block(low)?;
            self.block_idx += 1;
            self.cur_block_iterator = BlockIterator::create_and_seek_to_first(block);
        }
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> &[u8] {
        self.cur_block_iterator.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.cur_block_iterator.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.cur_block_iterator.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.cur_block_iterator.next();
        if !self.cur_block_iterator.is_valid() {
            if self.block_idx >= self.table.block_metas.len() - 1 {
                return Ok(());
            }
            let block = self.table.read_block(self.block_idx + 1)?;
            self.block_idx += 1;
            self.cur_block_iterator = BlockIterator::create_and_seek_to_first(block);
        }
        Ok(())
    }
}
