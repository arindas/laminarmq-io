use std::{
    cmp::min,
    convert::Into,
    future::Future,
    ops::{Add, AddAssign, Bound, Deref, DerefMut, RangeBounds, Sub, SubAssign},
    usize,
};

use futures::{Stream, StreamExt};
use num::{CheckedSub, Unsigned};

pub trait Quantifier:
    Add<Output = Self>
    + Sub
    + AddAssign
    + SubAssign
    + Ord
    + From<usize>
    + Into<usize>
    + Unsigned
    + CheckedSub
    + Clone
    + Copy
{
}

pub trait SizedEntity {
    type Position: Quantifier + From<Self::Size>;
    type Size: Quantifier + From<Self::Position>;

    fn size(&self) -> Self::Size;

    fn contains(&self, position: Self::Position) -> bool {
        position < Self::Position::from(self.size())
    }
}

pub struct AppendLocation<P, S> {
    pub write_position: P,
    pub write_len: S,
}

pub trait AsyncTruncate: SizedEntity {
    type TruncateError;

    fn truncate(
        &mut self,
        position: Self::Position,
    ) -> impl Future<Output = Result<(), Self::TruncateError>>;
}

pub trait AsyncAppend: SizedEntity {
    type AppendError;

    fn append(
        &mut self,
        bytes: &[u8],
    ) -> impl Future<Output = Result<AppendLocation<Self::Position, Self::Size>, Self::AppendError>>;
}

pub enum AsyncStreamAppendError<AE, TE> {
    AppendOverflow,
    StreamReadError,
    TruncateError(TE),
    AppendError(AE),
}

pub struct AsyncStreamAppend<'a, A>(&'a mut A);

impl<'a, A> AsyncStreamAppend<'a, A>
where
    A: AsyncAppend + AsyncTruncate,
{
    pub async fn append_stream<XBuf, XE, X>(
        self,
        stream: &mut X,
        append_stream_threshold: Option<A::Size>,
    ) -> Result<
        AppendLocation<A::Position, A::Size>,
        AsyncStreamAppendError<A::AppendError, A::TruncateError>,
    >
    where
        XBuf: Deref<Target = [u8]>,
        X: Stream<Item = Result<XBuf, XE>> + Unpin,
    {
        let file = self.0;

        let (mut bytes_written, write_position) = (0.into(), file.size().into());

        while let Some(buf) = stream.next().await {
            match match match (buf, append_stream_threshold) {
                (Ok(buf), Some(thresh)) if (bytes_written + buf.len().into()) <= thresh => Ok(buf),
                (Ok(_), Some(_)) => Err(AsyncStreamAppendError::AppendOverflow),
                (Ok(buf), None) => Ok(buf),
                (Err(_), _) => Err(AsyncStreamAppendError::StreamReadError),
            } {
                Ok(buf) => file
                    .append(buf.deref())
                    .await
                    .map_err(AsyncStreamAppendError::AppendError),
                Err(error) => Err(error),
            } {
                Ok(AppendLocation {
                    write_position: _,
                    write_len,
                }) => bytes_written += write_len,

                Err(error) => {
                    return file
                        .truncate(write_position.into())
                        .await
                        .map_err(AsyncStreamAppendError::TruncateError)
                        .and_then(|_| Err(error))
                }
            }
        }

        Ok(AppendLocation {
            write_position: write_position.into(),
            write_len: bytes_written,
        })
    }
}

pub struct ReadBytes<T, S> {
    pub read_bytes: T,
    pub read_len: S,
}

impl<T, S> ReadBytes<T, S> {
    pub fn map<U, F>(self, map_fn: F) -> ReadBytes<U, S>
    where
        F: FnOnce(T) -> U,
    {
        ReadBytes {
            read_bytes: map_fn(self.read_bytes),
            read_len: self.read_len,
        }
    }
}

pub trait AsyncRead: SizedEntity {
    type ByteBuf<'a>: Deref<Target = [u8]> + 'a
    where
        Self: 'a;

    type ReadError;

    fn read_at(
        &mut self,
        position: Self::Position,
        size: Self::Size,
    ) -> impl Future<Output = Result<ReadBytes<Self::ByteBuf<'_>, Self::Size>, Self::ReadError>>;
}

pub struct ReadBytesLen<T> {
    read_len: T,
}

pub trait AsyncBufRead: SizedEntity {
    type BufReadError;

    fn read_at_buf(
        &mut self,
        position: Self::Position,
        size: Self::Size,
        buffer: &mut [u8],
    ) -> impl Future<Output = Result<ReadBytesLen<Self::Size>, Self::BufReadError>>;
}

pub trait ByteBufStream {
    type ByteBuf<'a>: Deref<Target = [u8]> + 'a
    where
        Self: 'a;

    type Error;

    fn next(&mut self) -> impl Future<Output = Option<Result<Self::ByteBuf<'_>, Self::Error>>>;
}

pub trait StreamRead: SizedEntity {
    type Stream<'a>: ByteBufStream + 'a
    where
        Self: 'a;

    fn read_stream_at(&mut self, position: Self::Position, size: Self::Size) -> Self::Stream<'_>;
}

#[allow(unused)]
pub struct AsyncReadByteBufStream<'a, R, P, S> {
    reader: &'a mut R,

    position: P,
    bytes_remaining: S,
}

impl<'a, R> ByteBufStream for AsyncReadByteBufStream<'a, R, R::Position, R::Size>
where
    R: AsyncRead,
{
    type ByteBuf<'x> = R::ByteBuf<'x>
    where
        Self: 'x;

    type Error = R::ReadError;

    async fn next(&mut self) -> Option<Result<Self::ByteBuf<'_>, Self::Error>> {
        if self.bytes_remaining == 0.into() {
            return None;
        }

        let read_bytes = self
            .reader
            .read_at(self.position, self.bytes_remaining)
            .await;

        if read_bytes.is_err() {
            return read_bytes.err().map(Err);
        }

        let read_bytes = read_bytes.ok()?;

        self.bytes_remaining -= read_bytes.read_len;

        Some(Ok(read_bytes.read_bytes))
    }
}

impl<R> StreamRead for R
where
    R: AsyncRead,
{
    type Stream<'x> = AsyncReadByteBufStream<'x, R, R::Position, R::Size>
    where
        Self: 'x;

    fn read_stream_at(&mut self, position: Self::Position, size: Self::Size) -> Self::Stream<'_> {
        AsyncReadByteBufStream {
            reader: self,
            position,
            bytes_remaining: size,
        }
    }
}

pub struct Buffer<T> {
    buffer: T,
    len: usize,
}

pub enum BufferError {
    IndexOutOfBounds,
    BadSliceRange,
    BufferOverflow,
}

impl<T, X> Buffer<T>
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

    pub fn remaining(&self) -> usize {
        self.capacity() - self.len()
    }

    pub fn get_read_slice<R>(&self, range: R) -> Result<&[X], BufferError>
    where
        R: RangeBounds<usize>,
    {
        let (start, end) = match match (range.start_bound(), range.end_bound()) {
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
        }?;

        Ok(&self.buffer[start..=end])
    }

    pub fn get_append_slice_mut(&mut self) -> &mut [X] {
        &mut self.buffer[self.len..]
    }

    pub fn advance_end_by(&mut self, n: usize) -> Result<usize, BufferError> {
        if n > self.remaining() {
            Err(BufferError::BufferOverflow)
        } else {
            self.len += n;
            Ok(n)
        }
    }

    pub fn append(&mut self, src: &[X]) -> Result<usize, BufferError> {
        if src.len() > self.remaining() {
            return Err(BufferError::BufferOverflow);
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

pub struct AnchoredBuffer<B, P> {
    buffer: Buffer<B>,
    anchor_position: P,
}

impl<B, P> AnchoredBuffer<B, P>
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

    pub fn advance_anchor_by<S>(&mut self, n: S)
    where
        S: Quantifier + Into<P>,
    {
        self.re_anchor(self.anchor_position() + n.into())
    }

    pub fn offset(&self, position: P) -> Option<usize> {
        position
            .checked_sub(&self.anchor_position())
            .map(Into::into)
    }

    pub fn read_at<S>(&self, position: P, size: S) -> Result<&[u8], BufferError>
    where
        S: Quantifier,
    {
        let start = self.offset(position).ok_or(BufferError::IndexOutOfBounds)?;
        let end = start + size.into();

        self.get_read_slice(start..end)
    }

    pub fn end(&self) -> P {
        self.anchor_position() + P::from(self.len())
    }

    pub fn avail_from_pos(&self, position: P) -> usize {
        if let Some(pos) = self.offset(position) {
            self.len() - pos
        } else {
            0
        }
    }
}

impl<B, P> DerefMut for AnchoredBuffer<B, P> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buffer
    }
}

impl<B, P> Deref for AnchoredBuffer<B, P> {
    type Target = Buffer<B>;

    fn deref(&self) -> &Self::Target {
        &self.buffer
    }
}

pub struct DirectReaderBufferedAppender<R, B, P, S> {
    inner: R,
    read_limit: Option<S>,

    append_buffer: AnchoredBuffer<B, P>,

    size: S,
}

pub enum DirectReaderBufferedAppenderError<RE, AE> {
    ReadError(RE),
    AppendError(AE),
    ReadBeyondWrittenArea,
    ReadBufferGap,
    ReadUnderflow,
    AppendBufferError(BufferError),
}

#[allow(unused)]
impl<R, B> DirectReaderBufferedAppender<R, B, R::Position, R::Size>
where
    R: AsyncRead + AsyncAppend,
    B: DerefMut<Target = [u8]>,
{
    async fn flush(
        &mut self,
    ) -> Result<(), DirectReaderBufferedAppenderError<R::ReadError, R::AppendError>> {
        let buffered_bytes = self
            .append_buffer
            .get_read_slice(..)
            .map_err(DirectReaderBufferedAppenderError::AppendBufferError)?;

        let AppendLocation {
            write_position: _,
            write_len,
        } = self
            .inner
            .append(buffered_bytes)
            .await
            .map_err(DirectReaderBufferedAppenderError::AppendError)?;

        self.append_buffer.advance_anchor_by(write_len);

        Ok(())
    }
}

impl<R, B> SizedEntity for DirectReaderBufferedAppender<R, B, R::Position, R::Size>
where
    R: SizedEntity,
{
    type Position = R::Position;

    type Size = R::Size;

    fn size(&self) -> Self::Size {
        self.size
    }
}

pub enum DirectReaderBufferedAppenderByteBuf<'a, T> {
    Buffered(&'a [u8]),
    Read(T),
}

impl<'a, T> Deref for DirectReaderBufferedAppenderByteBuf<'a, T>
where
    T: Deref<Target = [u8]> + 'a,
{
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match self {
            DirectReaderBufferedAppenderByteBuf::Buffered(buffered) => buffered,
            DirectReaderBufferedAppenderByteBuf::Read(read_bytes) => read_bytes,
        }
    }
}

impl<R, B> AsyncRead for DirectReaderBufferedAppender<R, B, R::Position, R::Size>
where
    R: AsyncRead + AsyncAppend,
    B: DerefMut<Target = [u8]>,
{
    type ByteBuf<'x> = DirectReaderBufferedAppenderByteBuf<'x, R::ByteBuf<'x>>
    where
        Self: 'x;

    type ReadError = DirectReaderBufferedAppenderError<R::ReadError, R::AppendError>;

    async fn read_at(
        &mut self,
        position: Self::Position,
        size: Self::Size,
    ) -> Result<ReadBytes<Self::ByteBuf<'_>, Self::Size>, Self::ReadError> {
        enum ReadStrategy {
            Inner,
            Buffered,
        }

        match match match position {
            pos if !self.contains(pos) => {
                Err(DirectReaderBufferedAppenderError::ReadBeyondWrittenArea)
            }
            pos if self.append_buffer.contains_position(pos) => Ok((
                self.append_buffer.avail_from_pos(pos),
                ReadStrategy::Buffered,
            )),
            pos => self
                .append_buffer
                .anchor_position()
                .checked_sub(&pos)
                .map(Into::into)
                .map(|avail| (avail, ReadStrategy::Inner))
                .ok_or(DirectReaderBufferedAppenderError::ReadBufferGap),
        } {
            Ok((0, _)) if size == 0.into() => {
                return Ok(ReadBytes {
                    read_bytes: DirectReaderBufferedAppenderByteBuf::Buffered(&[]),
                    read_len: 0.into(),
                })
            }
            Ok((0, _)) => Err(DirectReaderBufferedAppenderError::ReadUnderflow),
            Ok((avail, read_strategy)) => Ok((
                min(size, min(avail.into(), self.read_limit.unwrap_or(size))),
                read_strategy,
            )),
            Err(error) => Err(error),
        } {
            Ok((read_size, ReadStrategy::Buffered)) => self
                .append_buffer
                .read_at(position, read_size)
                .map(|x| ReadBytes {
                    read_bytes: DirectReaderBufferedAppenderByteBuf::Buffered(x),
                    read_len: x.len().into(),
                })
                .map_err(DirectReaderBufferedAppenderError::AppendBufferError),
            Ok((read_size, ReadStrategy::Inner)) => self
                .inner
                .read_at(position, read_size)
                .await
                .map(|x| x.map(DirectReaderBufferedAppenderByteBuf::Read))
                .map_err(DirectReaderBufferedAppenderError::ReadError),

            Err(error) => Err(error),
        }
    }
}

impl<R, B> AsyncAppend for DirectReaderBufferedAppender<R, B, R::Position, R::Size>
where
    R: AsyncRead + AsyncAppend,
    B: DerefMut<Target = [u8]>,
{
    type AppendError = DirectReaderBufferedAppenderError<R::ReadError, R::AppendError>;

    async fn append(
        &mut self,
        bytes: &[u8],
    ) -> Result<AppendLocation<Self::Position, Self::Size>, Self::AppendError> {
        enum AppendStrategy {
            Buffered,
            Direct,
            FlushedAndBuffered,
        }

        let append_strategy = match bytes.len() {
            n if n >= self.append_buffer.capacity() => AppendStrategy::Direct,
            n if n >= self.append_buffer.remaining() => AppendStrategy::FlushedAndBuffered,
            _ => AppendStrategy::Buffered,
        };

        let append_buffer_end = self.append_buffer.end();

        match match match match append_strategy {
            strat @ (AppendStrategy::FlushedAndBuffered | AppendStrategy::Direct) => {
                (strat, self.flush().await)
            }
            strat => (strat, Ok(())),
        } {
            (strat @ (AppendStrategy::Buffered | AppendStrategy::FlushedAndBuffered), Ok(_)) => (
                strat,
                self.append_buffer
                    .append(bytes)
                    .map_err(DirectReaderBufferedAppenderError::AppendBufferError)
                    .map(|x| AppendLocation {
                        write_position: append_buffer_end,
                        write_len: R::Size::from(x),
                    }),
            ),
            (strat @ AppendStrategy::Direct, Ok(_)) => (
                strat,
                self.inner
                    .append(bytes)
                    .await
                    .map_err(DirectReaderBufferedAppenderError::AppendError),
            ),
            (strat, Err(error)) => (strat, Err(error)),
        } {
            (AppendStrategy::Direct, Ok(append_location)) => {
                self.append_buffer
                    .advance_anchor_by(append_location.write_len);
                Ok(append_location)
            }
            (_, result) => result,
        } {
            Ok(append_location) => {
                self.size += append_location.write_len;
                Ok(append_location)
            }

            Err(error) => Err(error),
        }
    }
}

#[allow(unused)]
pub struct BufferedReaderBufferedAppender<R, RB, AB, P, S> {
    inner: R,
    read_limit: Option<S>,

    append_buffer: AnchoredBuffer<AB, P>,
    read_buffer: AnchoredBuffer<RB, P>,

    size: S,
}

impl<R, RB, AB> SizedEntity for BufferedReaderBufferedAppender<R, RB, AB, R::Position, R::Size>
where
    R: SizedEntity,
{
    type Position = R::Position;

    type Size = R::Size;

    fn size(&self) -> Self::Size {
        self.size
    }
}

pub enum BufferedReaderBufferedAppenderError<RE, AE> {
    AppendError(AE),
    ReadError(RE),

    AppendBufferError(BufferError),
    ReadBufferError(BufferError),

    ReadBeyondWrittenArea,

    ReadLimitUnknown,

    InvalidOp,
}

impl<R, RB, AB> AsyncRead for BufferedReaderBufferedAppender<R, RB, AB, R::Position, R::Size>
where
    R: AsyncBufRead + AsyncAppend,
    RB: DerefMut<Target = [u8]>,
    AB: DerefMut<Target = [u8]>,
{
    type ByteBuf<'x> = &'x [u8]
    where
        Self: 'x;

    type ReadError = BufferedReaderBufferedAppenderError<R::BufReadError, R::AppendError>;

    async fn read_at(
        &mut self,
        position: Self::Position,
        size: Self::Size,
    ) -> Result<ReadBytes<Self::ByteBuf<'_>, Self::Size>, Self::ReadError> {
        enum BuffferedReadStrategy {
            ReanchorAndRefillRead,
            FillRead,
            UseRead,
            UseAppend,
        }

        match match match match match match position {
            pos if !self.contains(pos) => Err(Self::ReadError::ReadBeyondWrittenArea),
            pos if self.append_buffer.contains_position(pos) => Ok((
                Some(self.append_buffer.avail_from_pos(pos)),
                BuffferedReadStrategy::UseAppend,
            )),
            pos if self.read_buffer.contains_position(pos) => Ok((
                Some(self.read_buffer.avail_from_pos(pos)),
                BuffferedReadStrategy::UseRead,
            )),
            pos if self.read_buffer.contains_position_within_capacity(pos)
                && pos == self.read_buffer.end() =>
            {
                Ok((None, BuffferedReadStrategy::FillRead))
            }
            _ => Ok((None, BuffferedReadStrategy::ReanchorAndRefillRead)),
        } {
            Ok((avail, strat @ BuffferedReadStrategy::ReanchorAndRefillRead)) => {
                self.read_buffer.re_anchor(position);
                Ok((avail, strat))
            }
            result => result,
        } {
            Ok((
                avail,
                strat @ (BuffferedReadStrategy::ReanchorAndRefillRead
                | BuffferedReadStrategy::FillRead),
            )) => self
                .inner
                .read_at_buf(
                    position,
                    min(
                        (self.append_buffer.anchor_position() - position)
                            .into()
                            .into(),
                        self.read_buffer.remaining().into(),
                    ),
                    self.read_buffer.get_append_slice_mut(),
                )
                .await
                .map_err(Self::ReadError::ReadError)
                .and_then(|ReadBytesLen { read_len }| {
                    self.read_buffer
                        .advance_end_by(read_len.into())
                        .map_err(Self::ReadError::ReadBufferError)
                })
                .map(|_| (avail, strat)),
            result => result,
        } {
            Ok((None, strat @ BuffferedReadStrategy::ReanchorAndRefillRead)) => {
                Ok((Some(self.read_buffer.len()), strat))
            }
            Ok((None, strat @ BuffferedReadStrategy::FillRead)) => {
                Ok((Some(self.read_buffer.avail_from_pos(position)), strat))
            }
            result => result,
        } {
            Ok((Some(avail), strat)) => Ok((
                min(size, min(avail.into(), self.read_limit.unwrap_or(size))),
                strat,
            )),
            Ok((None, _)) => Err(Self::ReadError::ReadLimitUnknown),
            Err(error) => Err(error),
        } {
            Ok((
                read_size,
                BuffferedReadStrategy::ReanchorAndRefillRead
                | BuffferedReadStrategy::FillRead
                | BuffferedReadStrategy::UseRead,
            )) => self
                .read_buffer
                .read_at(position, read_size)
                .map_err(Self::ReadError::ReadBufferError),
            Ok((read_size, BuffferedReadStrategy::UseAppend)) => self
                .append_buffer
                .read_at(position, read_size)
                .map_err(Self::ReadError::AppendBufferError),
            Err(error) => Err(error),
        }
        .map(|read_bytes| ReadBytes {
            read_bytes,
            read_len: read_bytes.len().into(),
        })
    }
}
