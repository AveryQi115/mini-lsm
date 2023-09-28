#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
pub use builder::SsTableBuilder;
use bytes::{Buf, Bytes};
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::lsm_storage::BlockCache;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    /// It marks the end of the data block, as each data block is aligned to 4KB.
    pub offset: u32,
    key_len: u16,
    /// The first key of the data block, mainly used for index purpose.
    pub first_key: Bytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(
        block_meta: &[BlockMeta],
        #[allow(clippy::ptr_arg)] // remove this allow after you finish
        buf: &mut Vec<u8>,
    ) {
        for meta in block_meta {
            buf.extend_from_slice(&meta.offset.to_be_bytes());
            buf.extend_from_slice(&meta.key_len.to_be_bytes());
            buf.extend_from_slice(&meta.first_key);
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(buf: impl Buf) -> Vec<BlockMeta> {
        let mut block_metas = Vec::new();
        let mut buf = buf;
        while buf.has_remaining() {
            let offset = buf.get_u32();
            let key_len = buf.get_u16();
            let first_key = buf.copy_to_bytes(key_len as usize);
            block_metas.push(BlockMeta {
                offset,
                key_len,
                first_key,
            });
        }
        block_metas
    }
}

/// A file object.
pub struct FileObject(Bytes);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        Ok(self.0[offset as usize..(offset + len) as usize].to_vec())
    }

    pub fn size(&self) -> u64 {
        self.0.len() as u64
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        Ok(Self {
            0: Bytes::from(data),
        })
    }

    pub fn open(path: &Path) -> Result<Self> {
        Ok(Self {
            0: Bytes::from(std::fs::read(path)?),
        })
    }
}

/// -------------------------------------------------------------------------------------------------------
/// |              Data Block             |             Meta Block              |          Extra          |
/// -------------------------------------------------------------------------------------------------------
/// | Data Block #1 | ... | Data Block #N | Meta Block #1 | ... | Meta Block #N | Meta Block Offset (u32) |
/// -------------------------------------------------------------------------------------------------------
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    file: FileObject,
    /// The meta blocks that hold info for data blocks.
    block_metas: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    block_meta_offset: u32,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let block_meta_offset = file.read(file.size() - 4, 4)?;
        let block_meta_offset = u32::from_be_bytes(block_meta_offset[0..4].try_into().unwrap());
        let buf = file.read(
            block_meta_offset as u64,
            file.size() as u64 - 4 - block_meta_offset as u64,
        )?;
        let metas = BlockMeta::decode_block_meta(Bytes::from(buf));
        Ok(Self {
            file,
            block_metas: metas,
            block_meta_offset,
        })
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let block_offset = self.block_metas[block_idx].offset;
        let start = block_offset / 4196 * 4196;
        let block_data = self
            .file
            .read(start as u64, (block_offset - start) as u64)?;
        let block = Block::decode(&block_data);
        Ok(Arc::new(block))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        unimplemented!()
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: &[u8]) -> usize {
        unimplemented!()
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        unimplemented!()
    }
}

#[cfg(test)]
mod tests;
