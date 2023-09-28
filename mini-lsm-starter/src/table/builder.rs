use std::path::Path;
use std::sync::Arc;

use anyhow::Result;

use super::FileObject;
use crate::block::Block;
use crate::block::BlockBuilder;
use bytes::Bytes;

use super::{BlockMeta, SsTable};
use crate::lsm_storage::BlockCache;

/// Builds an SSTable from key-value pairs.
/// The SSTable format uses 4KB alignment and the offset records the end byte of each data block
/// --------------------------------------------------------------------------------------------------------------------
/// | data block 1(0-2500B) | data block 2(4196-6696B) | ... | meta block1 (offset 2500) | meta block2 (offset 6696)|...
pub struct SsTableBuilder {
    pub(super) meta: Vec<BlockMeta>,
    data_blocks: Vec<Block>,
    cur_block: BlockBuilder,
    cur_start: u32,
    block_size: usize,
    first_key: Vec<u8>,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        assert!(block_size <= 4196);
        Self {
            meta: Vec::new(),
            data_blocks: Vec::new(),
            cur_block: BlockBuilder::new(block_size),
            cur_start: 0,
            block_size,
            first_key: Vec::new(),
        }
    }

    /// Adds a key-value pair to SSTable.
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may be of help here)
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        if self.cur_block.add(key, value) {
            if self.first_key.is_empty() {
                self.first_key = key.to_vec();
            }
            return;
        }
        let block_size = self.cur_block.size() as u32;
        // BlockBuider::new assign to self.cur_block, cur_block holds the old self.cur_block so neither is dropped
        let cur_block = std::mem::replace(&mut self.cur_block, BlockBuilder::new(self.block_size));
        self.data_blocks.push(BlockBuilder::build(cur_block));
        let first_key = std::mem::replace(&mut self.first_key, key.to_vec());
        self.meta.push(BlockMeta {
            offset: self.cur_start + block_size,
            key_len: first_key.len() as u16,
            first_key: Bytes::from(first_key),
        });
        self.cur_block = BlockBuilder::new(self.block_size);
        assert!(self.cur_block.add(key, value));
        self.cur_start += 4196;
    }

    /// Get the estimated size of the SSTable.
    /// Since the data blocks contain much more data than meta blocks, just return the size of data blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data_blocks.len() * 4196 + self.cur_block.is_empty() as usize * 4196
    }

    /// Builds the SSTable and writes it to the given path. No need to actually write to disk until
    /// chapter 4 block cache.
    pub fn build(
        self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        let mut data = Vec::new();
        for data_block in self.data_blocks {
            let data_bytes = data_block.encode();
            let padding_bytes = vec![0; 4196 - data_bytes.len()];
            data.extend_from_slice(&data_bytes);
            data.extend_from_slice(&padding_bytes);
        }
        let mut block_meta_offset = self.cur_start;
        let mut meta = self.meta;
        if !self.cur_block.is_empty() {
            let block_size = self.cur_block.size() as u32;
            let data_bytes = self.cur_block.build().encode();
            let padding_bytes = vec![0; 4196 - data_bytes.len()];
            data.extend_from_slice(&data_bytes);
            data.extend_from_slice(&padding_bytes);
            block_meta_offset += 4196;
            meta.push(BlockMeta {
                offset: self.cur_start + block_size,
                key_len: self.first_key.len() as u16,
                first_key: Bytes::from(self.first_key),
            });
        }

        BlockMeta::encode_block_meta(&meta, &mut data);

        data.extend_from_slice(block_meta_offset.to_be_bytes().as_ref());

        Ok(SsTable {
            file: FileObject::create(path.as_ref(), data)?,
            block_metas: meta,
            block_meta_offset: block_meta_offset,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
