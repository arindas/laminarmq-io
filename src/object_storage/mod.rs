use core::cmp::Ordering;
use serde::{Deserialize, Serialize};

#[cfg(feature = "aws_s3")]
pub mod aws_s3;

pub trait BlockMap {
    fn position_block_containing_offset(&self, offset: usize) -> Option<usize>;

    fn get_block_at_idx(&self, block_idx: usize) -> Option<Block>;

    fn get_block_containing_offset(&self, offset: usize) -> Option<Block> {
        self.position_block_containing_offset(offset)
            .and_then(|idx| self.get_block_at_idx(idx))
    }

    fn append_block_with_block_size(&mut self, block_size: usize) -> Block;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn truncate(&mut self, offset: usize) -> Option<(usize, usize, Block)>;

    fn size(&self) -> usize {
        self.get_block_at_idx(self.len() - 1)
            .map(|p| p.end())
            .unwrap_or(0)
    }

    fn clear(&mut self);
}

#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct Block {
    pub offset: usize,
    pub size: usize,
}

impl Block {
    pub fn end(&self) -> usize {
        self.offset + self.size
    }
}

impl BlockMap for Vec<Block> {
    fn position_block_containing_offset(&self, offset: usize) -> Option<usize> {
        self.binary_search_by(|block| match block.offset.cmp(&offset) {
            Ordering::Less if offset < block.end() => Ordering::Equal,
            Ordering::Less => Ordering::Less,
            Ordering::Equal => Ordering::Equal,
            Ordering::Greater => Ordering::Greater,
        })
        .ok()
    }

    fn get_block_at_idx(&self, block_idx: usize) -> Option<Block> {
        self.get(block_idx).cloned()
    }

    fn append_block_with_block_size(&mut self, block_size: usize) -> Block {
        let offset = self.last().map_or(0, |x| x.end());

        let block = Block {
            offset,
            size: block_size,
        };

        self.push(block);

        block
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn truncate(&mut self, offset: usize) -> Option<(usize, usize, Block)> {
        let idx = self.position_block_containing_offset(offset)?;

        let mut truncated_block = self.get_block_at_idx(idx)?;

        let _ = self.split_off(idx);

        let old_block_size = truncated_block.size;

        truncated_block.size -= truncated_block.end() - offset;

        self.push(truncated_block);

        Some((idx, old_block_size, truncated_block))
    }

    fn clear(&mut self) {
        self.clear()
    }
}

#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct FixedBlockSizeMap {
    block_size: usize,
    len: usize,
}

impl BlockMap for FixedBlockSizeMap {
    fn position_block_containing_offset(&self, offset: usize) -> Option<usize> {
        let block_idx = offset / self.block_size;

        (block_idx < self.len).then_some(block_idx)
    }

    fn get_block_at_idx(&self, block_idx: usize) -> Option<Block> {
        (block_idx < self.len).then_some(Block {
            offset: block_idx * self.block_size,
            size: self.block_size,
        })
    }

    fn get_block_containing_offset(&self, offset: usize) -> Option<Block> {
        let block_idx = offset / self.block_size;

        (block_idx < self.len).then_some(Block {
            offset: block_idx * self.block_size,
            size: self.block_size,
        })
    }

    fn append_block_with_block_size(&mut self, _: usize) -> Block {
        let block = Block {
            offset: self.len * self.block_size,
            size: self.block_size,
        };

        self.len += 1;

        block
    }

    fn len(&self) -> usize {
        self.len
    }

    fn truncate(&mut self, offset: usize) -> Option<(usize, usize, Block)> {
        let idx = self.position_block_containing_offset(offset)?;

        self.len = idx;

        self.get_block_at_idx(self.len() - 1)
            .map(|x| (self.len() - 1, self.block_size, x))
    }

    fn clear(&mut self) {
        self.len = 0
    }
}
