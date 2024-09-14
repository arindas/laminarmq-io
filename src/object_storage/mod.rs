use core::cmp::Ordering;
use serde::{Deserialize, Serialize};

#[cfg(feature = "aws_s3")]
pub mod aws_s3;

pub trait PartMap {
    fn position_part_containing_offset(&self, offset: usize) -> Option<usize>;

    fn get_part_at_idx(&self, part_idx: usize) -> Option<Part>;

    fn get_part_containing_offset(&self, offset: usize) -> Option<Part> {
        self.position_part_containing_offset(offset)
            .and_then(|idx| self.get_part_at_idx(idx))
    }

    fn append_part_with_part_size(&mut self, part_size: usize) -> Part;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn truncate(&mut self, offset: usize) -> Option<(usize, usize, Part)>;

    fn size(&self) -> usize {
        self.get_part_at_idx(self.len() - 1)
            .map(|p| p.end())
            .unwrap_or(0)
    }

    fn clear(&mut self);
}

#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct Part {
    pub offset: usize,
    pub size: usize,
}

impl Part {
    pub fn end(&self) -> usize {
        self.offset + self.size
    }
}

impl PartMap for Vec<Part> {
    fn position_part_containing_offset(&self, offset: usize) -> Option<usize> {
        self.binary_search_by(|part| match part.offset.cmp(&offset) {
            Ordering::Less if offset < part.end() => Ordering::Equal,
            Ordering::Less => Ordering::Less,
            Ordering::Equal => Ordering::Equal,
            Ordering::Greater => Ordering::Greater,
        })
        .ok()
    }

    fn get_part_at_idx(&self, part_idx: usize) -> Option<Part> {
        self.get(part_idx).cloned()
    }

    fn append_part_with_part_size(&mut self, part_size: usize) -> Part {
        let offset = self.last().map_or(0, |x| x.end());

        let part = Part {
            offset,
            size: part_size,
        };

        self.push(part);

        part
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn truncate(&mut self, offset: usize) -> Option<(usize, usize, Part)> {
        let idx = self.position_part_containing_offset(offset)?;

        let mut truncated_part = self.get_part_at_idx(idx)?;

        let _ = self.split_off(idx);

        let old_part_size = truncated_part.size;

        truncated_part.size -= truncated_part.end() - offset;

        self.push(truncated_part);

        Some((idx, old_part_size, truncated_part))
    }

    fn clear(&mut self) {
        self.clear()
    }
}

#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct FixedPartSizeMap {
    part_size: usize,
    len: usize,
}

impl PartMap for FixedPartSizeMap {
    fn position_part_containing_offset(&self, offset: usize) -> Option<usize> {
        let part_idx = offset / self.part_size;

        (part_idx < self.len).then_some(part_idx)
    }

    fn get_part_at_idx(&self, part_idx: usize) -> Option<Part> {
        (part_idx < self.len).then_some(Part {
            offset: part_idx * self.part_size,
            size: self.part_size,
        })
    }

    fn get_part_containing_offset(&self, offset: usize) -> Option<Part> {
        let part_idx = offset / self.part_size;

        (part_idx < self.len).then_some(Part {
            offset: part_idx * self.part_size,
            size: self.part_size,
        })
    }

    fn append_part_with_part_size(&mut self, _: usize) -> Part {
        let part = Part {
            offset: self.len * self.part_size,
            size: self.part_size,
        };

        self.len += 1;

        part
    }

    fn len(&self) -> usize {
        self.len
    }

    fn truncate(&mut self, offset: usize) -> Option<(usize, usize, Part)> {
        let idx = self.position_part_containing_offset(offset)?;

        self.len = idx;

        self.get_part_at_idx(self.len() - 1)
            .map(|x| (self.len() - 1, self.part_size, x))
    }

    fn clear(&mut self) {
        self.len = 0
    }
}
