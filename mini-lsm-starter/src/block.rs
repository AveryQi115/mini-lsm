mod builder;
mod iterator;

pub use builder::BlockBuilder;
/// You may want to check `bytes::BufMut` out when manipulating continuous chunks of memory
use bytes::Bytes;
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree.
/// It is a collection of sorted key-value pairs.
/// The `actual` storage format is as below (After `Block::encode`):
///
/// ----------------------------------------------------------------------------------------------------
/// |             Data Section             |              Offset Section             |      Extra      |
/// ----------------------------------------------------------------------------------------------------
/// | Entry #1 | Entry #2 | ... | Entry #N | Offset #1 | Offset #2 | ... | Offset #N | num_of_elements |
/// ----------------------------------------------------------------------------------------------------
pub struct Block {
    data: Vec<u8>,
    offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut bytes: Vec<u8> = Vec::with_capacity(self.data.len() + self.offsets.len() * 2 + 2);
        bytes.extend_from_slice(&self.data);
        for &offset in self.offsets.iter().rev() {
            bytes.push((offset >> 8) as u8);
            bytes.push(offset as u8);
        }
        let num_of_elements = self.offsets.len() as u16;
        bytes.push((num_of_elements >> 8) as u8);
        bytes.push(num_of_elements as u8);
        Bytes::from(bytes)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let size = data.len();
        let num_of_elements = (data[size - 2] as u16) << 8 | data[size - 1] as u16;

        let mut offsets: Vec<u16> = Vec::with_capacity(num_of_elements as usize);
        for i in 0..num_of_elements {
            let offset = (data[size - 4 - (i as usize) * 2] as u16) << 8
                | data[size - 3 - (i as usize) * 2] as u16;
            offsets.push(offset);
        }

        let data = data[0..size - 2 - (num_of_elements as usize) * 2].to_vec();
        Self { data, offsets }
    }
}

#[cfg(test)]
mod tests;
