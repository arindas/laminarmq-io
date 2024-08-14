use std::{
    cmp::min,
    mem,
    ops::{Bound, Deref, DerefMut, RangeBounds},
};

use bytes::{Bytes, BytesMut};

use crate::io_types::{Quantifier, ReadBytes};

pub enum BufferBytes {
    Mut { bytes_mut: BytesMut },
    Read { bytes: Bytes },
}

pub enum BufferError {
    IndexOutOfBounds,
    BadSliceRange,
    BufferOverflow,
    IntegerConversionError,
    BufferBeingRead,
    BufferBeingWrittenTo,
    InvalidOp,
}

#[allow(unused)]
pub struct Buffer {
    buffer_bytes: BufferBytes,
    len: usize,
}

impl Buffer {
    pub fn len(&self) -> usize {
        self.len
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn capacity(&self) -> usize {
        match &self.buffer_bytes {
            BufferBytes::Mut { bytes_mut } => bytes_mut.len(),
            BufferBytes::Read { bytes } => bytes.len(),
        }
    }

    pub fn avail_to_append(&self) -> usize {
        self.capacity() - self.len()
    }

    fn get_validated_slice_range_bounds<R>(&self, range: R) -> Result<(usize, usize), BufferError>
    where
        R: RangeBounds<usize>,
    {
        match match (range.start_bound(), range.end_bound()) {
            (_, Bound::Excluded(&0)) => Err(BufferError::BadSliceRange),
            (Bound::Included(&start), Bound::Included(&end)) => Ok((start, end)),
            (Bound::Included(&start), Bound::Excluded(&end)) => Ok((start, end - 1)),
            (Bound::Included(&start), Bound::Unbounded) => Ok((start, self.len - 1)),
            (Bound::Excluded(&start), Bound::Included(&end)) => Ok((start + 1, end)),
            (Bound::Excluded(&start), Bound::Excluded(&end)) => Ok((start + 1, end - 1)),
            (Bound::Excluded(&start), Bound::Unbounded) => Ok((start + 1, self.len - 1)),
            (Bound::Unbounded, Bound::Included(&end)) => Ok((0, end)),
            (Bound::Unbounded, Bound::Excluded(&end)) => Ok((0, end - 1)),
            (Bound::Unbounded, Bound::Unbounded) => Ok((0, self.len - 1)),
        } {
            Ok((start, end)) if start > end => Err(BufferError::BadSliceRange),
            Ok((start, end)) if start >= self.len || end >= self.len => {
                Err(BufferError::IndexOutOfBounds)
            }
            Ok((start, end)) => Ok((start, end)),
            Err(error) => Err(error),
        }
    }

    fn switch_to_read_mode(&mut self) {
        let inner_bytes = match &mut self.buffer_bytes {
            BufferBytes::Mut { bytes_mut } => mem::replace(bytes_mut, BytesMut::new()),
            BufferBytes::Read { bytes: _ } => return,
        };

        self.buffer_bytes = BufferBytes::Read {
            bytes: inner_bytes.freeze(),
        };
    }

    fn switch_to_write_mode(&mut self) -> Result<(), BufferError> {
        let inner_buffer = match &mut self.buffer_bytes {
            BufferBytes::Mut { bytes_mut: _ } => return Ok(()),
            BufferBytes::Read { bytes } => mem::replace(bytes, Bytes::new()),
        };

        match inner_buffer.try_into_mut() {
            Ok(bytes_mut) => {
                self.buffer_bytes = BufferBytes::Mut { bytes_mut };
                Ok(())
            }
            Err(bytes) => {
                self.buffer_bytes = BufferBytes::Read { bytes };
                Err(BufferError::BufferBeingRead)
            }
        }
    }

    fn get_read_buffer_ref(&self) -> Result<&Bytes, BufferError> {
        match &self.buffer_bytes {
            BufferBytes::Mut { bytes_mut: _ } => Err(BufferError::BufferBeingWrittenTo),
            BufferBytes::Read { bytes } => Ok(bytes),
        }
    }

    fn get_write_buffer_mut_ref(&mut self) -> Result<&mut BytesMut, BufferError> {
        match &mut self.buffer_bytes {
            BufferBytes::Mut { bytes_mut } => Ok(bytes_mut),
            BufferBytes::Read { bytes: _ } => Err(BufferError::BufferBeingRead),
        }
    }

    pub fn get_read_slice<R>(&mut self, range: R) -> Result<Bytes, BufferError>
    where
        R: RangeBounds<usize>,
    {
        let (start, end) = self.get_validated_slice_range_bounds(range)?;

        self.switch_to_read_mode();

        self.get_read_buffer_ref()
            .map(|bytes| bytes.slice(start..=end))
    }

    pub fn get_append_slice_mut(&mut self) -> Result<BytesMut, BufferError> {
        let len = self.len();

        self.switch_to_write_mode()?;

        let inner_bytes = self.get_write_buffer_mut_ref()?;

        Ok(inner_bytes.split_off(len))
    }

    pub fn unsplit_append_slice(
        &mut self,
        append_bytes_mut: BytesMut,
        bytes_written: usize,
    ) -> Result<(), BufferError> {
        if bytes_written > append_bytes_mut.len() {
            return Err(BufferError::BufferOverflow);
        }

        self.switch_to_write_mode()?;

        let inner_bytes = self.get_write_buffer_mut_ref()?;

        inner_bytes.unsplit(append_bytes_mut);
        self.len += bytes_written;

        Ok(())
    }

    pub fn clear(&mut self) {
        self.len = 0;
    }

    pub fn contains(&self, pos: usize) -> bool {
        pos < self.len()
    }

    pub fn contains_within_capacity(&self, pos: usize) -> bool {
        pos < self.capacity()
    }
}

pub struct AnchoredBuffer<P> {
    buffer: Buffer,
    anchor_position: P,
}

impl<P> DerefMut for AnchoredBuffer<P> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buffer
    }
}

impl<P> Deref for AnchoredBuffer<P> {
    type Target = Buffer;

    fn deref(&self) -> &Self::Target {
        &self.buffer
    }
}

impl<P> AnchoredBuffer<P>
where
    P: Quantifier,
{
    pub fn anchor_position(&self) -> P {
        self.anchor_position
    }

    pub fn end_position(&self) -> Result<P, BufferError> {
        P::from_usize(self.len())
            .map(|buf_len| self.anchor_position() + buf_len)
            .ok_or(BufferError::IntegerConversionError)
    }

    pub fn offset(&self, position: P) -> Option<usize> {
        position
            .checked_sub(&self.anchor_position())
            .and_then(|x| P::to_usize(&x))
    }

    pub fn avail_to_read_from_pos(&self, position: P) -> usize {
        if let Some(pos) = self.offset(position) {
            self.len() - pos
        } else {
            0
        }
    }

    pub fn contains_position(&self, position: P) -> bool {
        self.offset(position)
            .map(|pos| self.contains(pos))
            .unwrap_or(false)
    }

    pub fn contains_position_within_capacity(&self, position: P) -> bool {
        self.offset(position)
            .map(|pos| self.contains_within_capacity(pos))
            .unwrap_or(false)
    }

    pub fn re_anchor(&mut self, new_anchor_position: P) {
        self.clear();
        self.anchor_position = new_anchor_position;
    }

    pub fn read_at<S>(&mut self, position: P, size: S) -> Result<ReadBytes<Bytes, S>, BufferError>
    where
        S: Quantifier,
    {
        let start = self.offset(position).ok_or(BufferError::IndexOutOfBounds)?;

        let size = size.to_usize().ok_or(BufferError::IntegerConversionError)?;

        let size = min(size, self.avail_to_read_from_pos(position));

        let read_bytes = self.get_read_slice(start..(start + size))?;

        let read_len =
            S::from_usize(read_bytes.len()).ok_or(BufferError::IntegerConversionError)?;

        Ok(ReadBytes {
            read_bytes,
            read_len,
        })
    }
}
