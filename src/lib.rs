use std::{
    cmp::min,
    convert::Into,
    future::Future,
    ops::{Add, AddAssign, Bound, Deref, DerefMut, RangeBounds, Sub, SubAssign},
};

use futures::{Stream, StreamExt};
use num::{zero, CheckedSub, FromPrimitive, ToPrimitive, Unsigned, Zero};

pub trait Quantifier:
    Add<Output = Self>
    + Sub
    + AddAssign
    + SubAssign
    + Ord
    + FromPrimitive
    + ToPrimitive
    + Unsigned
    + Zero
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
    IntegerConversionError,
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

        let (mut bytes_written, write_position) = (zero(), file.size().into());

        while let Some(buf) = stream.next().await {
            match match match (buf, append_stream_threshold) {
                (Ok(buf), Some(thresh))
                    if (bytes_written
                        + A::Size::from_usize(buf.len())
                            .ok_or(AsyncStreamAppendError::IntegerConversionError)?)
                        <= thresh =>
                {
                    Ok(buf)
                }
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
                        .truncate(write_position)
                        .await
                        .map_err(AsyncStreamAppendError::TruncateError)
                        .and_then(|_| Err(error))
                }
            }
        }

        Ok(AppendLocation {
            write_position,
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
        if self.bytes_remaining == zero() {
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
    IntegerConversionError,
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

    pub fn avail_to_append(&self) -> usize {
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
        if n > self.avail_to_append() {
            Err(BufferError::BufferOverflow)
        } else {
            self.len += n;
            Ok(n)
        }
    }

    pub fn append(&mut self, src: &[X]) -> Result<usize, BufferError> {
        if src.len() > self.avail_to_append() {
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
            .and_then(|x| P::to_usize(&x))
    }

    pub fn read_at<S>(&self, position: P, size: S) -> Result<&[u8], BufferError>
    where
        S: Quantifier,
    {
        let start = self.offset(position).ok_or(BufferError::IndexOutOfBounds)?;
        let end = start + size.to_usize().ok_or(BufferError::IntegerConversionError)?;

        self.get_read_slice(start..end)
    }

    pub fn end_position(&self) -> Result<P, BufferError> {
        P::from_usize(self.len())
            .map(|buf_len| self.anchor_position() + buf_len)
            .ok_or(BufferError::IntegerConversionError)
    }

    pub fn avail_to_read_from_pos(&self, position: P) -> usize {
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

pub struct DirectReaderBufferedAppender<R, AB, P, S> {
    inner: R,
    read_limit: Option<S>,

    append_buffer: AnchoredBuffer<AB, P>,

    size: S,
}

pub enum DirectReaderBufferedAppenderError<RE, AE> {
    ReadError(RE),
    AppendError(AE),
    ReadBeyondWrittenArea,
    ReadBufferGap,
    ReadUnderflow,
    AppendBufferError(BufferError),

    IntegerConversionError,
}

#[allow(unused)]
impl<R, AB> DirectReaderBufferedAppender<R, AB, R::Position, R::Size>
where
    R: AsyncRead + AsyncAppend,
    AB: DerefMut<Target = [u8]>,
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

impl<R, AB> SizedEntity for DirectReaderBufferedAppender<R, AB, R::Position, R::Size>
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

impl<R, AB> AsyncRead for DirectReaderBufferedAppender<R, AB, R::Position, R::Size>
where
    R: AsyncRead + AsyncAppend,
    AB: DerefMut<Target = [u8]>,
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
            pos if !self.contains(pos) => Err(Self::ReadError::ReadBeyondWrittenArea),
            pos if self.append_buffer.contains_position(pos) => Ok((
                self.append_buffer.avail_to_read_from_pos(pos),
                ReadStrategy::Buffered,
            )),
            pos => self
                .append_buffer
                .anchor_position()
                .checked_sub(&pos)
                .and_then(|x| R::Position::to_usize(&x))
                .map(|avail| (avail, ReadStrategy::Inner))
                .ok_or(Self::ReadError::ReadBufferGap),
        } {
            Ok((0, _)) if size == zero() => {
                return Ok(ReadBytes {
                    read_bytes: DirectReaderBufferedAppenderByteBuf::Buffered(&[]),
                    read_len: zero(),
                })
            }
            Ok((0, _)) => Err(Self::ReadError::ReadUnderflow),
            Ok((avail, read_strategy)) => Ok((
                min(
                    size,
                    min(
                        R::Size::from_usize(avail)
                            .ok_or(Self::ReadError::IntegerConversionError)?,
                        self.read_limit.unwrap_or(size),
                    ),
                ),
                read_strategy,
            )),
            Err(error) => Err(error),
        } {
            Ok((read_size, ReadStrategy::Buffered)) => self
                .append_buffer
                .read_at(position, read_size)
                .and_then(|x| {
                    Ok(ReadBytes {
                        read_bytes: DirectReaderBufferedAppenderByteBuf::Buffered(x),
                        read_len: R::Size::from_usize(x.len())
                            .ok_or(BufferError::IntegerConversionError)?,
                    })
                })
                .map_err(Self::ReadError::AppendBufferError),
            Ok((read_size, ReadStrategy::Inner)) => self
                .inner
                .read_at(position, read_size)
                .await
                .map(|x| x.map(DirectReaderBufferedAppenderByteBuf::Read))
                .map_err(Self::ReadError::ReadError),

            Err(error) => Err(error),
        }
    }
}

impl<R, AB> AsyncAppend for DirectReaderBufferedAppender<R, AB, R::Position, R::Size>
where
    R: AsyncRead + AsyncAppend,
    AB: DerefMut<Target = [u8]>,
{
    type AppendError = DirectReaderBufferedAppenderError<R::ReadError, R::AppendError>;

    async fn append(
        &mut self,
        bytes: &[u8],
    ) -> Result<AppendLocation<Self::Position, Self::Size>, Self::AppendError> {
        enum AppendStrategy {
            Inner,
            Buffered,
        }

        enum Action {
            Flush(AppendStrategy),
            AppendBuffered,
        }

        struct AdvanceAnchor;

        let append_strategy = match bytes.len() {
            n if n >= self.append_buffer.capacity() => Action::Flush(AppendStrategy::Inner),
            n if n >= self.append_buffer.avail_to_append() => {
                Action::Flush(AppendStrategy::Buffered)
            }
            _ => Action::AppendBuffered,
        };

        let append_buffer_end = self
            .append_buffer
            .end_position()
            .map_err(Self::AppendError::AppendBufferError)?;

        match match match match append_strategy {
            Action::AppendBuffered => (AppendStrategy::Buffered, Ok(())),
            Action::Flush(strat) => (strat, self.flush().await),
        } {
            (AppendStrategy::Buffered, Ok(_)) => (
                None,
                self.append_buffer
                    .append(bytes)
                    .and_then(|x| {
                        Ok(AppendLocation {
                            write_position: append_buffer_end,
                            write_len: R::Size::from_usize(x)
                                .ok_or(BufferError::IntegerConversionError)?,
                        })
                    })
                    .map_err(Self::AppendError::AppendBufferError),
            ),
            (AppendStrategy::Inner, Ok(_)) => (
                Some(AdvanceAnchor),
                self.inner
                    .append(bytes)
                    .await
                    .map_err(Self::AppendError::AppendError),
            ),
            (_, Err(error)) => (None, Err(error)),
        } {
            (Some(AdvanceAnchor), Ok(append_location)) => {
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

    IntegerConversionError,
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
        enum BufferedReadStrategy {
            UseAppend,
            UseRead,
        }

        enum ReadStrategy<P> {
            Buffered {
                strat: BufferedReadStrategy,
                avail: usize,
            },
            Fill(P),
        }

        enum Action<P> {
            Read(ReadStrategy<P>),
            Reanchor(P),
        }

        match match match match match position {
            pos if !self.contains(pos) => Err(Self::ReadError::ReadBeyondWrittenArea),
            pos if self.append_buffer.contains_position(pos) => {
                Ok(Action::Read(ReadStrategy::Buffered {
                    strat: BufferedReadStrategy::UseAppend,
                    avail: self.append_buffer.avail_to_read_from_pos(pos),
                }))
            }
            pos if self.read_buffer.contains_position(pos) => {
                Ok(Action::Read(ReadStrategy::Buffered {
                    strat: BufferedReadStrategy::UseRead,
                    avail: self.read_buffer.avail_to_read_from_pos(pos),
                }))
            }
            pos if self.read_buffer.contains_position_within_capacity(pos)
                && pos
                    >= self
                        .read_buffer
                        .end_position()
                        .map_err(Self::ReadError::ReadBufferError)? =>
            {
                Ok(Action::Read(ReadStrategy::Fill(
                    self.read_buffer
                        .end_position()
                        .map_err(Self::ReadError::ReadBufferError)?,
                )))
            }
            _ => Ok(Action::Reanchor(position)),
        } {
            Ok(Action::Reanchor(pos)) => {
                self.read_buffer.re_anchor(pos);
                Ok(ReadStrategy::Fill(pos))
            }
            Ok(Action::Read(strat)) => Ok(strat),
            Err(e) => Err(e),
        } {
            Ok(ReadStrategy::Buffered { strat, avail }) => Ok((strat, avail)),
            Ok(ReadStrategy::Fill(inner_read_pos)) => self
                .inner
                .read_at_buf(
                    inner_read_pos,
                    min(
                        R::Size::from(self.append_buffer.anchor_position() - inner_read_pos),
                        R::Size::from_usize(self.read_buffer.avail_to_append())
                            .ok_or(Self::ReadError::IntegerConversionError)?,
                    ),
                    self.read_buffer.get_append_slice_mut(),
                )
                .await
                .map_err(Self::ReadError::ReadError)
                .and_then(|ReadBytesLen { read_len }| {
                    self.read_buffer
                        .advance_end_by(
                            read_len
                                .to_usize()
                                .ok_or(Self::ReadError::IntegerConversionError)?,
                        )
                        .map_err(Self::ReadError::ReadBufferError)
                })
                .map(|_| {
                    (
                        BufferedReadStrategy::UseRead,
                        self.read_buffer.avail_to_read_from_pos(position),
                    )
                }),
            Err(err) => Err(err),
        } {
            Ok((strat, avail)) => Ok((
                strat,
                min(
                    size,
                    min(
                        R::Size::from_usize(avail)
                            .ok_or(Self::ReadError::IntegerConversionError)?,
                        self.read_limit.unwrap_or(size),
                    ),
                ),
            )),
            Err(error) => Err(error),
        } {
            Ok((BufferedReadStrategy::UseAppend, read_size)) => self
                .append_buffer
                .read_at(position, read_size)
                .map_err(Self::ReadError::AppendBufferError),
            Ok((BufferedReadStrategy::UseRead, read_size)) => self
                .read_buffer
                .read_at(position, read_size)
                .map_err(Self::ReadError::ReadBufferError),
            Err(error) => Err(error),
        }
        .and_then(|read_bytes| {
            Ok(ReadBytes {
                read_bytes,
                read_len: R::Size::from_usize(read_bytes.len())
                    .ok_or(Self::ReadError::IntegerConversionError)?,
            })
        })
    }
}

impl<R, RB, AB> BufferedReaderBufferedAppender<R, RB, AB, R::Position, R::Size>
where
    R: AsyncBufRead + AsyncAppend,
    AB: DerefMut<Target = [u8]>,
{
    async fn flush(
        &mut self,
    ) -> Result<(), BufferedReaderBufferedAppenderError<R::BufReadError, R::AppendError>> {
        let buffered_bytes = self
            .append_buffer
            .get_read_slice(..)
            .map_err(BufferedReaderBufferedAppenderError::AppendBufferError)?;

        let AppendLocation {
            write_position: _,
            write_len,
        } = self
            .inner
            .append(buffered_bytes)
            .await
            .map_err(BufferedReaderBufferedAppenderError::AppendError)?;

        self.append_buffer.advance_anchor_by(write_len);

        Ok(())
    }
}

impl<R, RB, AB> AsyncAppend for BufferedReaderBufferedAppender<R, RB, AB, R::Position, R::Size>
where
    R: AsyncBufRead + AsyncAppend,
    AB: DerefMut<Target = [u8]>,
{
    type AppendError = BufferedReaderBufferedAppenderError<R::BufReadError, R::AppendError>;

    async fn append(
        &mut self,
        bytes: &[u8],
    ) -> Result<AppendLocation<Self::Position, Self::Size>, Self::AppendError> {
        enum AppendStrategy {
            Inner,
            Buffered,
        }

        enum Action {
            Flush(AppendStrategy),
            AppendBuffered,
        }

        struct AdvanceAnchor;

        let append_strategy = match bytes.len() {
            n if n >= self.append_buffer.capacity() => Action::Flush(AppendStrategy::Inner),
            n if n >= self.append_buffer.avail_to_append() => {
                Action::Flush(AppendStrategy::Buffered)
            }
            _ => Action::AppendBuffered,
        };

        let append_buffer_end = self
            .append_buffer
            .end_position()
            .map_err(Self::AppendError::AppendBufferError)?;

        match match match match append_strategy {
            Action::AppendBuffered => (AppendStrategy::Buffered, Ok(())),
            Action::Flush(strat) => (strat, self.flush().await),
        } {
            (AppendStrategy::Buffered, Ok(_)) => (
                None,
                self.append_buffer
                    .append(bytes)
                    .and_then(|x| {
                        Ok(AppendLocation {
                            write_position: append_buffer_end,
                            write_len: R::Size::from_usize(x)
                                .ok_or(BufferError::IntegerConversionError)?,
                        })
                    })
                    .map_err(Self::AppendError::AppendBufferError),
            ),
            (AppendStrategy::Inner, Ok(_)) => (
                Some(AdvanceAnchor),
                self.inner
                    .append(bytes)
                    .await
                    .map_err(Self::AppendError::AppendError),
            ),
            (_, Err(error)) => (None, Err(error)),
        } {
            (Some(AdvanceAnchor), Ok(append_location)) => {
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
pub struct BufferedReaderDirectAppender<R, RB, P, S> {
    inner: R,
    read_limit: Option<S>,

    read_buffer: AnchoredBuffer<RB, P>,

    size: S,
}

impl<R, RB> SizedEntity for BufferedReaderDirectAppender<R, RB, R::Position, R::Size>
where
    R: SizedEntity,
{
    type Position = R::Position;

    type Size = R::Size;

    fn size(&self) -> Self::Size {
        self.size
    }
}

pub enum BufferedReaderDirectAppenderError<RE, AE> {
    AppendError(AE),
    ReadError(RE),

    ReadBufferError(BufferError),

    ReadBeyondWrittenArea,

    IntegerConversionError,
}

impl<R, RB> AsyncRead for BufferedReaderDirectAppender<R, RB, R::Position, R::Size>
where
    R: AsyncBufRead + AsyncAppend,
    RB: DerefMut<Target = [u8]>,
{
    type ByteBuf<'x> = &'x [u8]
    where
        Self: 'x;

    type ReadError = BufferedReaderDirectAppenderError<R::BufReadError, R::AppendError>;

    async fn read_at(
        &mut self,
        position: Self::Position,
        size: Self::Size,
    ) -> Result<ReadBytes<Self::ByteBuf<'_>, Self::Size>, Self::ReadError> {
        struct BufferedRead {
            avail: usize,
        }

        enum ReadStrategy<P> {
            Buffered(BufferedRead),
            Fill(P),
        }

        enum Action<P> {
            Read(ReadStrategy<P>),
            Reanchor(P),
        }

        match match match match match position {
            pos if !self.contains(pos) => Err(Self::ReadError::ReadBeyondWrittenArea),
            pos if self.read_buffer.contains_position(pos) => {
                Ok(Action::Read(ReadStrategy::Buffered(BufferedRead {
                    avail: self.read_buffer.avail_to_read_from_pos(pos),
                })))
            }
            pos if self.read_buffer.contains_position_within_capacity(pos)
                && pos
                    >= self
                        .read_buffer
                        .end_position()
                        .map_err(Self::ReadError::ReadBufferError)? =>
            {
                Ok(Action::Read(ReadStrategy::Fill(
                    self.read_buffer
                        .end_position()
                        .map_err(Self::ReadError::ReadBufferError)?,
                )))
            }
            _ => Ok(Action::Reanchor(position)),
        } {
            Ok(Action::Reanchor(pos)) => {
                self.read_buffer.re_anchor(pos);
                Ok(ReadStrategy::Fill(pos))
            }
            Ok(Action::Read(strat)) => Ok(strat),
            Err(e) => Err(e),
        } {
            Ok(ReadStrategy::Buffered(buffered_read)) => Ok(buffered_read),
            Ok(ReadStrategy::Fill(inner_read_pos)) => self
                .inner
                .read_at_buf(
                    inner_read_pos,
                    min(
                        self.size() - R::Size::from(inner_read_pos),
                        R::Size::from_usize(self.read_buffer.avail_to_append())
                            .ok_or(Self::ReadError::IntegerConversionError)?,
                    ),
                    self.read_buffer.get_append_slice_mut(),
                )
                .await
                .map_err(Self::ReadError::ReadError)
                .and_then(|ReadBytesLen { read_len }| {
                    self.read_buffer
                        .advance_end_by(
                            read_len
                                .to_usize()
                                .ok_or(Self::ReadError::IntegerConversionError)?,
                        )
                        .map_err(Self::ReadError::ReadBufferError)
                })
                .map(|_| BufferedRead {
                    avail: self.read_buffer.avail_to_read_from_pos(position),
                }),
            Err(err) => Err(err),
        } {
            Ok(BufferedRead { avail }) => Ok(min(
                size,
                min(
                    R::Size::from_usize(avail).ok_or(Self::ReadError::IntegerConversionError)?,
                    self.read_limit.unwrap_or(size),
                ),
            )),
            Err(error) => Err(error),
        } {
            Ok(read_size) => self
                .read_buffer
                .read_at(position, read_size)
                .map_err(Self::ReadError::ReadBufferError),
            Err(error) => Err(error),
        }
        .and_then(|read_bytes| {
            Ok(ReadBytes {
                read_bytes,
                read_len: R::Size::from_usize(read_bytes.len())
                    .ok_or(Self::ReadError::IntegerConversionError)?,
            })
        })
    }
}

impl<R, RB> AsyncAppend for BufferedReaderDirectAppender<R, RB, R::Position, R::Size>
where
    R: AsyncBufRead + AsyncAppend,
{
    type AppendError = BufferedReaderDirectAppenderError<R::BufReadError, R::AppendError>;

    async fn append(
        &mut self,
        bytes: &[u8],
    ) -> Result<AppendLocation<Self::Position, Self::Size>, Self::AppendError> {
        self.inner
            .append(bytes)
            .await
            .inspect(|append_location| {
                self.size += append_location.write_len;
            })
            .map_err(Self::AppendError::AppendError)
    }
}

#[allow(unused)]
pub struct BufferedReader<R, RB, P, S> {
    inner: R,
    read_limit: Option<S>,

    read_buffer: AnchoredBuffer<RB, P>,
}

pub enum BufferedReaderError<RE> {
    ReadError(RE),

    ReadBufferError(BufferError),

    ReadBeyondWrittenArea,

    IntegerConversionError,
}

impl<R, RB> SizedEntity for BufferedReader<R, RB, R::Position, R::Size>
where
    R: AsyncBufRead,
{
    type Position = R::Position;

    type Size = R::Size;

    fn size(&self) -> Self::Size {
        self.inner.size()
    }
}

impl<R, RB> AsyncRead for BufferedReader<R, RB, R::Position, R::Size>
where
    R: AsyncBufRead,
    RB: DerefMut<Target = [u8]>,
{
    type ByteBuf<'x> = &'x [u8]
    where
        Self: 'x;

    type ReadError = BufferedReaderError<R::BufReadError>;

    async fn read_at(
        &mut self,
        position: Self::Position,
        size: Self::Size,
    ) -> Result<ReadBytes<Self::ByteBuf<'_>, Self::Size>, Self::ReadError> {
        struct BufferedRead {
            avail: usize,
        }

        enum ReadStrategy<P> {
            Buffered(BufferedRead),
            Fill(P),
        }

        enum Action<P> {
            Read(ReadStrategy<P>),
            Reanchor(P),
        }

        match match match match match position {
            pos if !self.contains(pos) => Err(Self::ReadError::ReadBeyondWrittenArea),
            pos if self.read_buffer.contains_position(pos) => {
                Ok(Action::Read(ReadStrategy::Buffered(BufferedRead {
                    avail: self.read_buffer.avail_to_read_from_pos(pos),
                })))
            }
            pos if self.read_buffer.contains_position_within_capacity(pos)
                && pos
                    >= self
                        .read_buffer
                        .end_position()
                        .map_err(Self::ReadError::ReadBufferError)? =>
            {
                Ok(Action::Read(ReadStrategy::Fill(
                    self.read_buffer
                        .end_position()
                        .map_err(Self::ReadError::ReadBufferError)?,
                )))
            }
            _ => Ok(Action::Reanchor(position)),
        } {
            Ok(Action::Reanchor(pos)) => {
                self.read_buffer.re_anchor(pos);
                Ok(ReadStrategy::Fill(pos))
            }
            Ok(Action::Read(strat)) => Ok(strat),
            Err(e) => Err(e),
        } {
            Ok(ReadStrategy::Buffered(buffered_read)) => Ok(buffered_read),
            Ok(ReadStrategy::Fill(inner_read_pos)) => self
                .inner
                .read_at_buf(
                    inner_read_pos,
                    min(
                        self.size() - R::Size::from(inner_read_pos),
                        R::Size::from_usize(self.read_buffer.avail_to_append())
                            .ok_or(Self::ReadError::IntegerConversionError)?,
                    ),
                    self.read_buffer.get_append_slice_mut(),
                )
                .await
                .map_err(Self::ReadError::ReadError)
                .and_then(|ReadBytesLen { read_len }| {
                    self.read_buffer
                        .advance_end_by(
                            read_len
                                .to_usize()
                                .ok_or(Self::ReadError::IntegerConversionError)?,
                        )
                        .map_err(Self::ReadError::ReadBufferError)
                })
                .map(|_| BufferedRead {
                    avail: self.read_buffer.avail_to_read_from_pos(position),
                }),
            Err(err) => Err(err),
        } {
            Ok(BufferedRead { avail }) => Ok(min(
                size,
                min(
                    R::Size::from_usize(avail).ok_or(Self::ReadError::IntegerConversionError)?,
                    self.read_limit.unwrap_or(size),
                ),
            )),
            Err(error) => Err(error),
        } {
            Ok(read_size) => self
                .read_buffer
                .read_at(position, read_size)
                .map_err(Self::ReadError::ReadBufferError),
            Err(error) => Err(error),
        }
        .and_then(|read_bytes| {
            Ok(ReadBytes {
                read_bytes,
                read_len: R::Size::from_usize(read_bytes.len())
                    .ok_or(Self::ReadError::IntegerConversionError)?,
            })
        })
    }
}

#[allow(unused)]
pub struct BufferedAppender<R, AB, P, S> {
    inner: R,

    append_buffer: AnchoredBuffer<AB, P>,

    size: S,
}

pub enum BufferedAppenderError<AE> {
    AppendError(AE),
    AppendBufferError(BufferError),
}

impl<R, AB> SizedEntity for BufferedAppender<R, AB, R::Position, R::Size>
where
    R: AsyncAppend,
{
    type Position = R::Position;

    type Size = R::Size;

    fn size(&self) -> Self::Size {
        self.size
    }
}

#[allow(unused)]
impl<R, AB> BufferedAppender<R, AB, R::Position, R::Size>
where
    R: AsyncAppend,
    AB: DerefMut<Target = [u8]>,
{
    async fn flush(&mut self) -> Result<(), BufferedAppenderError<R::AppendError>> {
        let buffered_bytes = self
            .append_buffer
            .get_read_slice(..)
            .map_err(BufferedAppenderError::AppendBufferError)?;

        let AppendLocation {
            write_position: _,
            write_len,
        } = self
            .inner
            .append(buffered_bytes)
            .await
            .map_err(BufferedAppenderError::AppendError)?;

        self.append_buffer.advance_anchor_by(write_len);

        Ok(())
    }
}

impl<R, AB> AsyncAppend for BufferedAppender<R, AB, R::Position, R::Size>
where
    R: AsyncAppend,
    AB: DerefMut<Target = [u8]>,
{
    type AppendError = BufferedAppenderError<R::AppendError>;

    async fn append(
        &mut self,
        bytes: &[u8],
    ) -> Result<AppendLocation<Self::Position, Self::Size>, Self::AppendError> {
        enum AppendStrategy {
            Inner,
            Buffered,
        }

        enum Action {
            Flush(AppendStrategy),
            AppendBuffered,
        }

        struct AdvanceAnchor;

        let append_strategy = match bytes.len() {
            n if n >= self.append_buffer.capacity() => Action::Flush(AppendStrategy::Inner),
            n if n >= self.append_buffer.avail_to_append() => {
                Action::Flush(AppendStrategy::Buffered)
            }
            _ => Action::AppendBuffered,
        };

        let append_buffer_end = self
            .append_buffer
            .end_position()
            .map_err(Self::AppendError::AppendBufferError)?;

        match match match match append_strategy {
            Action::AppendBuffered => (AppendStrategy::Buffered, Ok(())),
            Action::Flush(strat) => (strat, self.flush().await),
        } {
            (AppendStrategy::Buffered, Ok(_)) => (
                None,
                self.append_buffer
                    .append(bytes)
                    .and_then(|x| {
                        Ok(AppendLocation {
                            write_position: append_buffer_end,
                            write_len: R::Size::from_usize(x)
                                .ok_or(BufferError::IntegerConversionError)?,
                        })
                    })
                    .map_err(Self::AppendError::AppendBufferError),
            ),
            (AppendStrategy::Inner, Ok(_)) => (
                Some(AdvanceAnchor),
                self.inner
                    .append(bytes)
                    .await
                    .map_err(Self::AppendError::AppendError),
            ),
            (_, Err(error)) => (None, Err(error)),
        } {
            (Some(AdvanceAnchor), Ok(append_location)) => {
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
