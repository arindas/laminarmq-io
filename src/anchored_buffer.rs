#[allow(unused)]
use std::{
    cmp::{max, min},
    convert::Into,
    future::Future,
    marker::PhantomData,
    ops::{Add, AddAssign, Bound, Deref, DerefMut, Not, RangeBounds, Sub, SubAssign},
};

use bytes::{Bytes, BytesMut};

use crate::io_types::Quantifier;

pub struct _Buffer<T> {
    buffer: T,
    len: usize,
}

pub enum _BufferError {
    IndexOutOfBounds,
    BadSliceRange,
    BufferOverflow,
    IntegerConversionError,
}

impl<T, X> _Buffer<T>
where
    T: DerefMut<Target = [X]>,
    X: Copy,
{
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn capacity(&self) -> usize {
        self.buffer.len()
    }

    pub fn avail_to_append(&self) -> usize {
        self.capacity() - self.len()
    }

    pub fn get_read_slice<R>(&self, range: R) -> Result<&[X], _BufferError>
    where
        R: RangeBounds<usize>,
    {
        let (start, end) = match match (range.start_bound(), range.end_bound()) {
            (_, Bound::Excluded(&0)) => Err(_BufferError::BadSliceRange),
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
            Ok((start, end)) if start > end => Err(_BufferError::BadSliceRange),
            Ok((start, end)) if start >= self.len || end >= self.len => {
                Err(_BufferError::IndexOutOfBounds)
            }
            Ok((start, end)) => Ok((start, end)),
            Err(error) => Err(error),
        }?;

        Ok(&self.buffer[start..=end])
    }

    pub fn get_append_slice_mut(&mut self) -> &mut [X] {
        &mut self.buffer[self.len..]
    }

    pub fn advance_start_by(&mut self, n: usize) -> Result<usize, _BufferError> {
        if n > self.len() {
            return Err(_BufferError::IndexOutOfBounds);
        }

        self.buffer.copy_within(n.., 0);
        self.len -= n;

        Ok(n)
    }

    pub fn advance_end_by(&mut self, n: usize) -> Result<usize, _BufferError> {
        if n > self.avail_to_append() {
            Err(_BufferError::BufferOverflow)
        } else {
            self.len += n;
            Ok(n)
        }
    }

    pub fn append(&mut self, src: &[X]) -> Result<usize, _BufferError> {
        if src.len() > self.avail_to_append() {
            return Err(_BufferError::BufferOverflow);
        }

        self.get_append_slice_mut()[..src.len()].copy_from_slice(src);

        self.advance_end_by(src.len())
    }

    pub fn clear(&mut self) {
        self.len = 0
    }

    pub fn contains(&self, pos: usize) -> bool {
        pos < self.len()
    }

    pub fn contains_within_capacity(&self, pos: usize) -> bool {
        pos < self.capacity()
    }
}

pub struct _AnchoredBuffer<B, P> {
    buffer: _Buffer<B>,
    anchor_position: P,
}

impl<B, P> _AnchoredBuffer<B, P>
where
    B: DerefMut<Target = [u8]>,
    P: Quantifier,
{
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

    pub fn anchor_position(&self) -> P {
        self.anchor_position
    }

    pub fn re_anchor(&mut self, new_anchor_position: P) {
        self.clear();
        self.anchor_position = new_anchor_position;
    }

    pub fn advance_anchor_by<S>(&mut self, n: S) -> Result<S, _BufferError>
    where
        S: Quantifier + Into<P>,
    {
        self.advance_start_by(n.to_usize().ok_or(_BufferError::IntegerConversionError)?)?;

        self.anchor_position += n.into();

        Ok(n)
    }

    pub fn offset(&self, position: P) -> Option<usize> {
        position
            .checked_sub(&self.anchor_position())
            .and_then(|x| P::to_usize(&x))
    }

    pub fn read_at<S>(&self, position: P, size: S) -> Result<&[u8], _BufferError>
    where
        S: Quantifier,
    {
        let start = self
            .offset(position)
            .ok_or(_BufferError::IndexOutOfBounds)?;
        let end = start
            + size
                .to_usize()
                .ok_or(_BufferError::IntegerConversionError)?;

        self.get_read_slice(start..end)
    }

    pub fn end_position(&self) -> Result<P, _BufferError> {
        P::from_usize(self.len())
            .map(|buf_len| self.anchor_position() + buf_len)
            .ok_or(_BufferError::IntegerConversionError)
    }

    pub fn avail_to_read_from_pos(&self, position: P) -> usize {
        if let Some(pos) = self.offset(position) {
            self.len() - pos
        } else {
            0
        }
    }
}

impl<B, P> DerefMut for _AnchoredBuffer<B, P> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buffer
    }
}

impl<B, P> Deref for _AnchoredBuffer<B, P> {
    type Target = _Buffer<B>;

    fn deref(&self) -> &Self::Target {
        &self.buffer
    }
}

// ___________________________________________________________________________________

pub enum BufferBytes {
    Mut { bytes_mut: BytesMut },
    Read { bytes: Bytes },
}

pub enum BufferError {
    IndexOutOfBounds,
    BadSliceRange,
    BufferOverflow,
    IntegerConversionError,
    BufferStillBeingRead,
}
