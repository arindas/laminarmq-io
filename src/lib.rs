pub mod fs;
pub mod object_storage;
pub mod stream;

use std::{
    cmp::{max, min},
    convert::Into,
    future::Future,
    marker::PhantomData,
    ops::{Add, AddAssign, Bound, Deref, DerefMut, Not, RangeBounds, Sub, SubAssign},
};

use futures::TryFutureExt;
use num::{zero, CheckedSub, FromPrimitive, ToPrimitive, Unsigned, Zero};
use stream::{Lender, OwnedLender, Stream};

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

impl<T> Quantifier for T where
    T: Add<Output = Self>
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

pub struct IntegerConversionError;

pub trait SizedEntity {
    type Position: Quantifier + From<Self::Size>;
    type Size: Quantifier + From<Self::Position>;

    fn size(&self) -> Self::Size;

    fn contains(&self, position: Self::Position) -> bool {
        position < Self::Position::from(self.size())
    }
}

pub trait FallibleEntity {
    type Error: From<IntegerConversionError>;
}

pub struct AppendLocation<P, S> {
    pub write_position: P,
    pub write_len: S,
}

pub trait AsyncTruncate: SizedEntity + FallibleEntity {
    fn truncate(
        &mut self,
        position: Self::Position,
    ) -> impl Future<Output = Result<(), Self::Error>>;
}

pub trait AsyncRemove: FallibleEntity {
    fn remove(self) -> impl Future<Output = Result<(), Self::Error>>;
}

pub trait AsyncClose: FallibleEntity {
    fn close(self) -> impl Future<Output = Result<(), Self::Error>>;
}

pub trait AsyncAppend: SizedEntity + FallibleEntity {
    fn append(
        &mut self,
        bytes: &[u8],
    ) -> impl Future<Output = Result<AppendLocation<Self::Position, Self::Size>, Self::Error>>;
}

pub enum StreamAppendError<E, XE> {
    AppendOverflow,
    StreamReadError(XE),
    InnerError(E),
}

pub trait StreamAppend: SizedEntity + FallibleEntity {
    fn append_stream<XBuf, XE, X>(
        &mut self,
        stream: &mut X,
        append_threshold: Option<Self::Size>,
    ) -> impl Future<
        Output = Result<
            AppendLocation<Self::Position, Self::Size>,
            StreamAppendError<Self::Error, XE>,
        >,
    >
    where
        XBuf: Deref<Target = [u8]>,
        X: Stream<OwnedLender<Result<XBuf, XE>>>,
        X: Unpin;
}

impl<A> StreamAppend for A
where
    A: AsyncAppend + AsyncTruncate,
{
    async fn append_stream<XBuf, XE, X>(
        &mut self,
        stream: &mut X,
        append_threshold: Option<Self::Size>,
    ) -> Result<AppendLocation<Self::Position, Self::Size>, StreamAppendError<Self::Error, XE>>
    where
        XBuf: Deref<Target = [u8]>,
        X: Stream<OwnedLender<Result<XBuf, XE>>> + Unpin,
    {
        let (mut bytes_written, write_position) = (zero(), self.size().into());
        while let Some(buf) = stream.next().await {
            match match match (buf, append_threshold) {
                (Ok(buf), Some(thresh))
                    if (bytes_written
                        + A::Size::from_usize(buf.len()).ok_or(
                            StreamAppendError::InnerError(IntegerConversionError.into()),
                        )?)
                        <= thresh =>
                {
                    Ok(buf)
                }
                (Ok(_), Some(_)) => Err(StreamAppendError::AppendOverflow),
                (Ok(buf), None) => Ok(buf),
                (Err(err), _) => Err(StreamAppendError::StreamReadError(err)),
            } {
                Ok(buf) => self
                    .append(buf.deref())
                    .await
                    .map_err(StreamAppendError::InnerError),
                Err(error) => Err(error),
            } {
                Ok(AppendLocation {
                    write_position: _,
                    write_len,
                }) => bytes_written += write_len,

                Err(error) => {
                    return self
                        .truncate(write_position)
                        .await
                        .map_err(StreamAppendError::InnerError)
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

pub struct ReadBytesLen<T> {
    read_len: T,
}

pub trait AsyncBufRead: SizedEntity + FallibleEntity {
    fn read_at_buf(
        &mut self,
        position: Self::Position,
        buffer: &mut [u8],
    ) -> impl Future<Output = Result<ReadBytesLen<Self::Size>, Self::Error>>;

    fn read_at_buf_sized(
        &mut self,
        position: Self::Position,
        size: Self::Size,
        buffer: &mut [u8],
    ) -> impl Future<Output = Result<ReadBytesLen<Self::Size>, Self::Error>> {
        async move {
            let size = size
                .to_usize()
                .map(|size| min(size, buffer.len()))
                .ok_or(IntegerConversionError)
                .map_err(Into::into)?;

            self.read_at_buf(position, &mut buffer[..size]).await
        }
    }
}

pub trait ByteLender {
    type ByteBuf<'a>: Deref<Target = [u8]> + 'a
    where
        Self: 'a;
}

pub trait AsyncRead<B: ByteLender>: SizedEntity + FallibleEntity {
    fn read_at<'a>(
        &'a mut self,
        position: Self::Position,
        size: Self::Size,
    ) -> impl Future<Output = Result<ReadBytes<B::ByteBuf<'a>, Self::Size>, Self::Error>> + 'a
    where
        B: 'a;
}

pub struct ReadLimitedAsyncRead<R, S> {
    inner: R,
    read_limit: S,
}

impl<R, S> FallibleEntity for ReadLimitedAsyncRead<R, S>
where
    R: FallibleEntity,
{
    type Error = R::Error;
}

impl<R> SizedEntity for ReadLimitedAsyncRead<R, R::Size>
where
    R: SizedEntity,
{
    type Position = R::Position;

    type Size = R::Size;

    fn size(&self) -> Self::Size {
        self.inner.size()
    }
}

impl<B, R> AsyncRead<B> for ReadLimitedAsyncRead<R, R::Size>
where
    R: AsyncRead<B>,
    B: ByteLender,
{
    async fn read_at<'a>(
        &'a mut self,
        position: Self::Position,
        size: Self::Size,
    ) -> Result<ReadBytes<<B as ByteLender>::ByteBuf<'a>, Self::Size>, Self::Error>
    where
        B: 'a,
    {
        self.inner
            .read_at(position, min(self.read_limit, size))
            .await
    }
}

pub struct FallibleByteLender<B, E>(PhantomData<(B, E)>);

impl<B, E> Lender for FallibleByteLender<B, E>
where
    B: ByteLender,
{
    type Item<'a> = Result<B::ByteBuf<'a>, E>
    where
        Self: 'a;
}

pub trait StreamRead<B: ByteLender>: SizedEntity + FallibleEntity {
    fn read_stream_at<'a>(
        &'a mut self,
        position: Self::Position,
        size: Self::Size,
    ) -> impl Stream<FallibleByteLender<B, Self::Error>> + 'a
    where
        B: 'a;
}

pub struct AsyncReadStreamer<'a, R, B, P, SZ> {
    reader: &'a mut R,
    position: P,
    bytes_to_read: SZ,

    _phantom_data: PhantomData<B>,
}

impl<'x, R, B> Stream<FallibleByteLender<B, R::Error>>
    for AsyncReadStreamer<'x, R, B, R::Position, R::Size>
where
    R: AsyncRead<B>,
    B: ByteLender,
{
    async fn next<'a>(&'a mut self) -> Option<<FallibleByteLender<B, R::Error> as Lender>::Item<'a>>
    where
        FallibleByteLender<B, R::Error>: 'a,
    {
        if self.bytes_to_read == zero() {
            return None;
        }

        let read_bytes = self.reader.read_at(self.position, self.bytes_to_read).await;

        if read_bytes.is_err() {
            return read_bytes.err().map(Err);
        }

        let read_bytes = read_bytes.ok()?;

        self.position += read_bytes.read_len.into();

        self.bytes_to_read -= read_bytes.read_len;

        Some(Ok(read_bytes.read_bytes))
    }
}

impl<R, B> StreamRead<B> for R
where
    R: AsyncRead<B>,
    B: ByteLender,
{
    fn read_stream_at<'a>(
        &'a mut self,
        position: Self::Position,
        size: Self::Size,
    ) -> impl Stream<FallibleByteLender<B, Self::Error>> + 'a
    where
        B: 'a,
    {
        AsyncReadStreamer {
            reader: self,
            position,
            bytes_to_read: size,
            _phantom_data: PhantomData,
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

    pub fn advance_start_by(&mut self, n: usize) -> Result<usize, BufferError> {
        if n > self.len() {
            return Err(BufferError::IndexOutOfBounds);
        }

        self.buffer.copy_within(n.., 0);
        self.len -= n;

        Ok(n)
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

    pub fn advance_anchor_by<S>(&mut self, n: S) -> Result<S, BufferError>
    where
        S: Quantifier + Into<P>,
    {
        self.advance_start_by(n.to_usize().ok_or(BufferError::IntegerConversionError)?)?;

        self.anchor_position += n.into();

        Ok(n)
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

pub enum DirectReaderBufferedAppenderError<E> {
    ReadError(E),
    AppendError(E),
    RemoveError(E),
    CloseError(E),

    ReadBeyondWrittenArea,
    ReadBufferGap,
    ReadUnderflow,
    AppendBufferError(BufferError),

    FlushIncomplete,

    IntegerConversionError,
}

impl<E> From<IntegerConversionError> for DirectReaderBufferedAppenderError<E> {
    fn from(_: IntegerConversionError) -> Self {
        Self::IntegerConversionError
    }
}

impl<R, AB> DirectReaderBufferedAppender<R, AB, R::Position, R::Size>
where
    R: AsyncAppend,
    AB: DerefMut<Target = [u8]>,
{
    async fn flush(&mut self) -> Result<(), DirectReaderBufferedAppenderError<R::Error>> {
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

        self.append_buffer
            .advance_anchor_by(write_len)
            .map_err(DirectReaderBufferedAppenderError::AppendBufferError)?;

        self.append_buffer
            .is_empty()
            .not()
            .then_some(())
            .ok_or(DirectReaderBufferedAppenderError::FlushIncomplete)?;

        Ok(())
    }
}

impl<R, AB, P, S> AsyncRemove for DirectReaderBufferedAppender<R, AB, P, S>
where
    R: AsyncRemove,
{
    fn remove(self) -> impl Future<Output = Result<(), Self::Error>> {
        self.inner.remove().map_err(Self::Error::RemoveError)
    }
}

impl<R, AB> AsyncClose for DirectReaderBufferedAppender<R, AB, R::Position, R::Size>
where
    R: AsyncClose + AsyncAppend,
    AB: DerefMut<Target = [u8]>,
{
    async fn close(mut self) -> Result<(), Self::Error> {
        self.flush().await?;

        self.inner.close().await.map_err(Self::Error::CloseError)
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

impl<R, AB, P, S> FallibleEntity for DirectReaderBufferedAppender<R, AB, P, S>
where
    R: FallibleEntity,
{
    type Error = DirectReaderBufferedAppenderError<R::Error>;
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

pub struct DirectReaderBufferedAppenderByteLender<BL>(BL);

impl<BL> ByteLender for DirectReaderBufferedAppenderByteLender<BL>
where
    BL: ByteLender,
{
    type ByteBuf<'a> = DirectReaderBufferedAppenderByteBuf<'a, BL::ByteBuf<'a>>
    where
        Self: 'a;
}

impl<R, RBL, AB> AsyncRead<DirectReaderBufferedAppenderByteLender<RBL>>
    for DirectReaderBufferedAppender<R, AB, R::Position, R::Size>
where
    R: AsyncRead<RBL>,
    RBL: ByteLender,
    AB: DerefMut<Target = [u8]>,
{
    async fn read_at<'a>(
        &'a mut self,
        position: Self::Position,
        size: Self::Size,
    ) -> Result<
        ReadBytes<
            <DirectReaderBufferedAppenderByteLender<RBL> as ByteLender>::ByteBuf<'a>,
            Self::Size,
        >,
        Self::Error,
    >
    where
        DirectReaderBufferedAppenderByteLender<RBL>: 'a,
    {
        enum ReadStrategy {
            Inner,
            Buffered,
        }

        match match match position {
            pos if !self.contains(pos) => Err(Self::Error::ReadBeyondWrittenArea),
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
                .ok_or(Self::Error::ReadBufferGap),
        } {
            Ok((0, _)) if size == zero() => {
                return Ok(ReadBytes {
                    read_bytes: DirectReaderBufferedAppenderByteBuf::Buffered(&[]),
                    read_len: zero(),
                })
            }
            Ok((0, _)) => Err(Self::Error::ReadUnderflow),
            Ok((avail, read_strategy)) => Ok((
                min(
                    size,
                    min(
                        R::Size::from_usize(avail).ok_or(Self::Error::IntegerConversionError)?,
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
                .map_err(Self::Error::AppendBufferError),
            Ok((read_size, ReadStrategy::Inner)) => self
                .inner
                .read_at(position, read_size)
                .await
                .map(|x| x.map(DirectReaderBufferedAppenderByteBuf::Read))
                .map_err(Self::Error::ReadError),

            Err(error) => Err(error),
        }
    }
}

impl<R, AB> AsyncAppend for DirectReaderBufferedAppender<R, AB, R::Position, R::Size>
where
    R: AsyncAppend,
    AB: DerefMut<Target = [u8]>,
{
    async fn append(
        &mut self,
        bytes: &[u8],
    ) -> Result<AppendLocation<Self::Position, Self::Size>, Self::Error> {
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
            .map_err(Self::Error::AppendBufferError)?;

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
                    .map_err(Self::Error::AppendBufferError),
            ),
            (AppendStrategy::Inner, Ok(_)) => (
                Some(AdvanceAnchor),
                self.inner
                    .append(bytes)
                    .await
                    .map_err(Self::Error::AppendError),
            ),
            (_, Err(error)) => (None, Err(error)),
        } {
            (Some(AdvanceAnchor), Ok(append_location)) => self
                .append_buffer
                .advance_anchor_by(append_location.write_len)
                .map_err(Self::Error::AppendBufferError)
                .map(|_| append_location),
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

pub enum BufferedReaderBufferedAppenderError<E> {
    AppendError(E),
    ReadError(E),
    RemoveError(E),
    CloseError(E),

    AppendBufferError(BufferError),
    ReadBufferError(BufferError),

    ReadBeyondWrittenArea,
    FlushIncomplete,

    IntegerConversionError,
}

impl<E> From<IntegerConversionError> for BufferedReaderBufferedAppenderError<E> {
    fn from(_: IntegerConversionError) -> Self {
        Self::IntegerConversionError
    }
}

impl<R, RB, AB, P, S> FallibleEntity for BufferedReaderBufferedAppender<R, RB, AB, P, S>
where
    R: FallibleEntity,
{
    type Error = BufferedReaderBufferedAppenderError<R::Error>;
}

pub struct ByteSliceRefLender;

impl ByteLender for ByteSliceRefLender {
    type ByteBuf<'a> = &'a [u8]
    where
        Self: 'a;
}

impl<R, RB, AB> AsyncRead<ByteSliceRefLender>
    for BufferedReaderBufferedAppender<R, RB, AB, R::Position, R::Size>
where
    R: AsyncBufRead,
    RB: DerefMut<Target = [u8]>,
    AB: DerefMut<Target = [u8]>,
{
    async fn read_at<'a>(
        &'a mut self,
        position: Self::Position,
        size: Self::Size,
    ) -> Result<ReadBytes<<ByteSliceRefLender as ByteLender>::ByteBuf<'a>, Self::Size>, Self::Error>
    where
        ByteSliceRefLender: 'a,
    {
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
            pos if !self.contains(pos) => Err(Self::Error::ReadBeyondWrittenArea),
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
                        .map_err(Self::Error::ReadBufferError)? =>
            {
                Ok(Action::Read(ReadStrategy::Fill(
                    self.read_buffer
                        .end_position()
                        .map_err(Self::Error::ReadBufferError)?,
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
                .read_at_buf_sized(
                    inner_read_pos,
                    min(
                        R::Size::from(self.append_buffer.anchor_position() - inner_read_pos),
                        R::Size::from_usize(self.read_buffer.avail_to_append())
                            .ok_or(Self::Error::IntegerConversionError)?,
                    ),
                    self.read_buffer.get_append_slice_mut(),
                )
                .await
                .map_err(Self::Error::ReadError)
                .and_then(|ReadBytesLen { read_len }| {
                    self.read_buffer
                        .advance_end_by(
                            read_len
                                .to_usize()
                                .ok_or(Self::Error::IntegerConversionError)?,
                        )
                        .map_err(Self::Error::ReadBufferError)
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
                        R::Size::from_usize(avail).ok_or(Self::Error::IntegerConversionError)?,
                        self.read_limit.unwrap_or(size),
                    ),
                ),
            )),
            Err(error) => Err(error),
        } {
            Ok((BufferedReadStrategy::UseAppend, read_size)) => self
                .append_buffer
                .read_at(position, read_size)
                .map_err(Self::Error::AppendBufferError),
            Ok((BufferedReadStrategy::UseRead, read_size)) => self
                .read_buffer
                .read_at(position, read_size)
                .map_err(Self::Error::ReadBufferError),
            Err(error) => Err(error),
        }
        .and_then(|read_bytes| {
            Ok(ReadBytes {
                read_bytes,
                read_len: R::Size::from_usize(read_bytes.len())
                    .ok_or(Self::Error::IntegerConversionError)?,
            })
        })
    }
}

impl<R, RB, AB> BufferedReaderBufferedAppender<R, RB, AB, R::Position, R::Size>
where
    R: AsyncAppend,
    AB: DerefMut<Target = [u8]>,
{
    async fn flush(&mut self) -> Result<(), BufferedReaderBufferedAppenderError<R::Error>> {
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

        self.append_buffer
            .advance_anchor_by(write_len)
            .map_err(BufferedReaderBufferedAppenderError::AppendBufferError)?;

        self.append_buffer
            .is_empty()
            .not()
            .then_some(())
            .ok_or(BufferedReaderBufferedAppenderError::FlushIncomplete)?;

        Ok(())
    }
}

impl<R, RB, AB, P, S> AsyncRemove for BufferedReaderBufferedAppender<R, RB, AB, P, S>
where
    R: AsyncRemove,
{
    fn remove(self) -> impl Future<Output = Result<(), Self::Error>> {
        self.inner.remove().map_err(Self::Error::RemoveError)
    }
}

impl<R, RB, AB> AsyncClose for BufferedReaderBufferedAppender<R, RB, AB, R::Position, R::Size>
where
    R: AsyncClose + AsyncAppend,
    AB: DerefMut<Target = [u8]>,
{
    async fn close(mut self) -> Result<(), Self::Error> {
        self.flush().await?;

        self.inner.close().await.map_err(Self::Error::CloseError)
    }
}

impl<R, RB, AB> AsyncAppend for BufferedReaderBufferedAppender<R, RB, AB, R::Position, R::Size>
where
    R: AsyncAppend,
    AB: DerefMut<Target = [u8]>,
{
    async fn append(
        &mut self,
        bytes: &[u8],
    ) -> Result<AppendLocation<Self::Position, Self::Size>, Self::Error> {
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
            .map_err(Self::Error::AppendBufferError)?;

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
                    .map_err(Self::Error::AppendBufferError),
            ),
            (AppendStrategy::Inner, Ok(_)) => (
                Some(AdvanceAnchor),
                self.inner
                    .append(bytes)
                    .await
                    .map_err(Self::Error::AppendError),
            ),
            (_, Err(error)) => (None, Err(error)),
        } {
            (Some(AdvanceAnchor), Ok(append_location)) => self
                .append_buffer
                .advance_anchor_by(append_location.write_len)
                .map_err(Self::Error::AppendBufferError)
                .map(|_| append_location),
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

pub enum BufferedReaderDirectAppenderError<E> {
    AppendError(E),
    ReadError(E),

    ReadBufferError(BufferError),

    ReadBeyondWrittenArea,

    IntegerConversionError,
}

impl<E> From<IntegerConversionError> for BufferedReaderDirectAppenderError<E> {
    fn from(_: IntegerConversionError) -> Self {
        Self::IntegerConversionError
    }
}

impl<R, RB, P, S> FallibleEntity for BufferedReaderDirectAppender<R, RB, P, S>
where
    R: FallibleEntity,
{
    type Error = BufferedReaderDirectAppenderError<R::Error>;
}

impl<R, RB> AsyncRead<ByteSliceRefLender>
    for BufferedReaderDirectAppender<R, RB, R::Position, R::Size>
where
    R: AsyncBufRead,
    RB: DerefMut<Target = [u8]>,
{
    async fn read_at<'a>(
        &'a mut self,
        position: Self::Position,
        size: Self::Size,
    ) -> Result<ReadBytes<<ByteSliceRefLender as ByteLender>::ByteBuf<'a>, Self::Size>, Self::Error>
    where
        ByteSliceRefLender: 'a,
    {
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
            pos if !self.contains(pos) => Err(Self::Error::ReadBeyondWrittenArea),
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
                        .map_err(Self::Error::ReadBufferError)? =>
            {
                Ok(Action::Read(ReadStrategy::Fill(
                    self.read_buffer
                        .end_position()
                        .map_err(Self::Error::ReadBufferError)?,
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
                .read_at_buf_sized(
                    inner_read_pos,
                    min(
                        self.size() - R::Size::from(inner_read_pos),
                        R::Size::from_usize(self.read_buffer.avail_to_append())
                            .ok_or(Self::Error::IntegerConversionError)?,
                    ),
                    self.read_buffer.get_append_slice_mut(),
                )
                .await
                .map_err(Self::Error::ReadError)
                .and_then(|ReadBytesLen { read_len }| {
                    self.read_buffer
                        .advance_end_by(
                            read_len
                                .to_usize()
                                .ok_or(Self::Error::IntegerConversionError)?,
                        )
                        .map_err(Self::Error::ReadBufferError)
                })
                .map(|_| BufferedRead {
                    avail: self.read_buffer.avail_to_read_from_pos(position),
                }),
            Err(err) => Err(err),
        } {
            Ok(BufferedRead { avail }) => Ok(min(
                size,
                min(
                    R::Size::from_usize(avail).ok_or(Self::Error::IntegerConversionError)?,
                    self.read_limit.unwrap_or(size),
                ),
            )),
            Err(error) => Err(error),
        } {
            Ok(read_size) => self
                .read_buffer
                .read_at(position, read_size)
                .map_err(Self::Error::ReadBufferError),
            Err(error) => Err(error),
        }
        .and_then(|read_bytes| {
            Ok(ReadBytes {
                read_bytes,
                read_len: R::Size::from_usize(read_bytes.len())
                    .ok_or(Self::Error::IntegerConversionError)?,
            })
        })
    }
}

impl<R, RB> AsyncAppend for BufferedReaderDirectAppender<R, RB, R::Position, R::Size>
where
    R: AsyncBufRead + AsyncAppend,
{
    async fn append(
        &mut self,
        bytes: &[u8],
    ) -> Result<AppendLocation<Self::Position, Self::Size>, Self::Error> {
        self.inner
            .append(bytes)
            .await
            .inspect(|append_location| {
                self.size += append_location.write_len;
            })
            .map_err(Self::Error::AppendError)
    }
}

pub struct BufferedReader<R, RB, P, S> {
    inner: R,
    read_limit: Option<S>,

    read_buffer: AnchoredBuffer<RB, P>,
}

pub enum BufferedReaderError<E> {
    ReadError(E),

    ReadBufferError(BufferError),

    ReadBeyondWrittenArea,

    IntegerConversionError,
}

impl<E> From<IntegerConversionError> for BufferedReaderError<E> {
    fn from(_: IntegerConversionError) -> Self {
        Self::IntegerConversionError
    }
}

impl<R, RB, P, S> FallibleEntity for BufferedReader<R, RB, P, S>
where
    R: FallibleEntity,
{
    type Error = BufferedReaderError<R::Error>;
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

impl<R, RB> AsyncRead<ByteSliceRefLender> for BufferedReader<R, RB, R::Position, R::Size>
where
    R: AsyncBufRead,
    RB: DerefMut<Target = [u8]>,
{
    async fn read_at<'a>(
        &'a mut self,
        position: Self::Position,
        size: Self::Size,
    ) -> Result<ReadBytes<<ByteSliceRefLender as ByteLender>::ByteBuf<'a>, Self::Size>, Self::Error>
    where
        ByteSliceRefLender: 'a,
    {
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
            pos if !self.contains(pos) => Err(Self::Error::ReadBeyondWrittenArea),
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
                        .map_err(Self::Error::ReadBufferError)? =>
            {
                Ok(Action::Read(ReadStrategy::Fill(
                    self.read_buffer
                        .end_position()
                        .map_err(Self::Error::ReadBufferError)?,
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
                .read_at_buf_sized(
                    inner_read_pos,
                    min(
                        self.size() - R::Size::from(inner_read_pos),
                        R::Size::from_usize(self.read_buffer.avail_to_append())
                            .ok_or(Self::Error::IntegerConversionError)?,
                    ),
                    self.read_buffer.get_append_slice_mut(),
                )
                .await
                .map_err(Self::Error::ReadError)
                .and_then(|ReadBytesLen { read_len }| {
                    self.read_buffer
                        .advance_end_by(
                            read_len
                                .to_usize()
                                .ok_or(Self::Error::IntegerConversionError)?,
                        )
                        .map_err(Self::Error::ReadBufferError)
                })
                .map(|_| BufferedRead {
                    avail: self.read_buffer.avail_to_read_from_pos(position),
                }),
            Err(err) => Err(err),
        } {
            Ok(BufferedRead { avail }) => Ok(min(
                size,
                min(
                    R::Size::from_usize(avail).ok_or(Self::Error::IntegerConversionError)?,
                    self.read_limit.unwrap_or(size),
                ),
            )),
            Err(error) => Err(error),
        } {
            Ok(read_size) => self
                .read_buffer
                .read_at(position, read_size)
                .map_err(Self::Error::ReadBufferError),
            Err(error) => Err(error),
        }
        .and_then(|read_bytes| {
            Ok(ReadBytes {
                read_bytes,
                read_len: R::Size::from_usize(read_bytes.len())
                    .ok_or(Self::Error::IntegerConversionError)?,
            })
        })
    }
}

pub struct BufferedAppender<R, AB, P, S> {
    inner: R,

    append_buffer: AnchoredBuffer<AB, P>,

    size: S,
}

pub enum BufferedAppenderError<AE> {
    AppendError(AE),
    RemoveError(AE),
    CloseError(AE),
    AppendBufferError(BufferError),
    FlushIncomplete,
    IntegerConversionError,
}

impl<E> From<IntegerConversionError> for BufferedAppenderError<E> {
    fn from(_: IntegerConversionError) -> Self {
        Self::IntegerConversionError
    }
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

impl<R, AB, P, S> FallibleEntity for BufferedAppender<R, AB, P, S>
where
    R: FallibleEntity,
{
    type Error = BufferedAppenderError<R::Error>;
}

impl<R, AB> BufferedAppender<R, AB, R::Position, R::Size>
where
    R: AsyncAppend,
    AB: DerefMut<Target = [u8]>,
{
    async fn flush(&mut self) -> Result<(), BufferedAppenderError<R::Error>> {
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

        self.append_buffer
            .advance_anchor_by(write_len)
            .map_err(BufferedAppenderError::AppendBufferError)?;

        self.append_buffer
            .is_empty()
            .not()
            .then_some(())
            .ok_or(BufferedAppenderError::FlushIncomplete)?;

        Ok(())
    }
}

impl<R, AB, P, S> AsyncRemove for BufferedAppender<R, AB, P, S>
where
    R: AsyncRemove,
{
    fn remove(self) -> impl Future<Output = Result<(), Self::Error>> {
        self.inner.remove().map_err(Self::Error::RemoveError)
    }
}

impl<R, AB> AsyncClose for BufferedAppender<R, AB, R::Position, R::Size>
where
    R: AsyncClose + AsyncAppend,
    AB: DerefMut<Target = [u8]>,
{
    async fn close(mut self) -> Result<(), Self::Error> {
        self.flush().await?;

        self.inner.close().await.map_err(Self::Error::CloseError)
    }
}

impl<R, AB> AsyncAppend for BufferedAppender<R, AB, R::Position, R::Size>
where
    R: AsyncAppend,
    AB: DerefMut<Target = [u8]>,
{
    async fn append(
        &mut self,
        bytes: &[u8],
    ) -> Result<AppendLocation<Self::Position, Self::Size>, Self::Error> {
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
            .map_err(Self::Error::AppendBufferError)?;

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
                    .map_err(Self::Error::AppendBufferError),
            ),
            (AppendStrategy::Inner, Ok(_)) => (
                Some(AdvanceAnchor),
                self.inner
                    .append(bytes)
                    .await
                    .map_err(Self::Error::AppendError),
            ),
            (_, Err(error)) => (None, Err(error)),
        } {
            (Some(AdvanceAnchor), Ok(append_location)) => self
                .append_buffer
                .advance_anchor_by(append_location.write_len)
                .map_err(Self::Error::AppendBufferError)
                .map(|_| append_location),
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
pub struct StreamReaderBufferedAppender<R, AB, P, S> {
    inner: R,
    read_limit: Option<S>,

    append_buffer: AnchoredBuffer<AB, P>,

    size: S,
}

pub enum StreamReaderBufferedAppenderError<E> {
    ReadError(E),
    AppendError(E),
    RemoveError(E),
    CloseError(E),

    ReadBeyondWrittenArea,
    AppendBufferError(BufferError),

    FlushIncomplete,

    IntegerConversionError,
}

impl<E> From<IntegerConversionError> for StreamReaderBufferedAppenderError<E> {
    fn from(_: IntegerConversionError) -> Self {
        Self::IntegerConversionError
    }
}

impl<R, AB> StreamReaderBufferedAppender<R, AB, R::Position, R::Size>
where
    R: AsyncAppend,
    AB: DerefMut<Target = [u8]>,
{
    async fn flush(&mut self) -> Result<(), StreamReaderBufferedAppenderError<R::Error>> {
        let buffered_bytes = self
            .append_buffer
            .get_read_slice(..)
            .map_err(StreamReaderBufferedAppenderError::AppendBufferError)?;

        let AppendLocation {
            write_position: _,
            write_len,
        } = self
            .inner
            .append(buffered_bytes)
            .await
            .map_err(StreamReaderBufferedAppenderError::AppendError)?;

        self.append_buffer
            .advance_anchor_by(write_len)
            .map_err(StreamReaderBufferedAppenderError::AppendBufferError)?;

        self.append_buffer
            .is_empty()
            .not()
            .then_some(())
            .ok_or(StreamReaderBufferedAppenderError::FlushIncomplete)?;

        Ok(())
    }
}

impl<R, AB, P, S> AsyncRemove for StreamReaderBufferedAppender<R, AB, P, S>
where
    R: AsyncRemove,
{
    fn remove(self) -> impl Future<Output = Result<(), Self::Error>> {
        self.inner.remove().map_err(Self::Error::RemoveError)
    }
}

impl<R, AB> AsyncClose for StreamReaderBufferedAppender<R, AB, R::Position, R::Size>
where
    R: AsyncClose + AsyncAppend,
    AB: DerefMut<Target = [u8]>,
{
    async fn close(mut self) -> Result<(), Self::Error> {
        self.flush().await?;

        self.inner.close().await.map_err(Self::Error::CloseError)
    }
}

impl<R, AB> SizedEntity for StreamReaderBufferedAppender<R, AB, R::Position, R::Size>
where
    R: SizedEntity,
{
    type Position = R::Position;

    type Size = R::Size;

    fn size(&self) -> Self::Size {
        self.size
    }
}

impl<R, AB, P, S> FallibleEntity for StreamReaderBufferedAppender<R, AB, P, S>
where
    R: FallibleEntity,
{
    type Error = StreamReaderBufferedAppenderError<R::Error>;
}

pub enum StreamReaderBufferedAppenderByteBuf<'a, T> {
    Buffered(&'a [u8]),
    Read(T),
}

impl<'a, T> Deref for StreamReaderBufferedAppenderByteBuf<'a, T>
where
    T: Deref<Target = [u8]> + 'a,
{
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Buffered(buffered) => buffered,
            Self::Read(read_bytes) => read_bytes,
        }
    }
}

pub struct StreamReaderBufferedAppenderByteLender<BL>(PhantomData<BL>);

impl<BL> ByteLender for StreamReaderBufferedAppenderByteLender<BL>
where
    BL: ByteLender,
{
    type ByteBuf<'a> = StreamReaderBufferedAppenderByteBuf<'a, BL::ByteBuf<'a>>
    where
        Self: 'a;
}

struct StreamReaderBufferedAppenderBufferedBytesStream<'a> {
    buffered_bytes: &'a [u8],
    consumed: bool,
}

impl<'a> StreamReaderBufferedAppenderBufferedBytesStream<'a> {
    fn new(buffered_bytes: &'a [u8]) -> Self {
        Self {
            buffered_bytes,
            consumed: false,
        }
    }
}

impl<'x, RBL, E> Stream<FallibleByteLender<StreamReaderBufferedAppenderByteLender<RBL>, E>>
    for StreamReaderBufferedAppenderBufferedBytesStream<'x>
where
    RBL: ByteLender,
{
    async fn next<'a>(
        &'a mut self,
    ) -> Option<
        <FallibleByteLender<StreamReaderBufferedAppenderByteLender<RBL>, E> as Lender>::Item<'a>,
    >
    where
        FallibleByteLender<StreamReaderBufferedAppenderByteLender<RBL>, E>: 'a,
    {
        if self.consumed {
            None
        } else {
            self.consumed = true;
            Some(Ok(StreamReaderBufferedAppenderByteBuf::Buffered(
                self.buffered_bytes,
            )))
        }
    }
}

impl<R, RBL, AB> StreamRead<StreamReaderBufferedAppenderByteLender<RBL>>
    for StreamReaderBufferedAppender<R, AB, R::Position, R::Size>
where
    R: StreamRead<RBL>,
    R::Error: 'static,
    RBL: ByteLender + 'static,
    AB: DerefMut<Target = [u8]>,
{
    fn read_stream_at<'a>(
        &'a mut self,
        position: Self::Position,
        size: Self::Size,
    ) -> impl Stream<FallibleByteLender<StreamReaderBufferedAppenderByteLender<RBL>, Self::Error>> + 'a
    where
        StreamReaderBufferedAppenderByteLender<RBL>: 'a,
    {
        let append_buffer_anchor_position = self.append_buffer.anchor_position();

        let append_buffer_read_start = max(append_buffer_anchor_position, position);
        let append_buffer_read_end = min(position + size.into(), self.size().into());
        let append_buffer_read_size = append_buffer_read_end
            .checked_sub(&append_buffer_read_start)
            .unwrap_or(zero());

        let inner_read_start = position;
        let inner_read_end = min(self.inner.size().into(), position + size.into());
        let inner_read_size = inner_read_end
            .checked_sub(&inner_read_start)
            .unwrap_or(zero());

        let inner_stream = stream::latch(
            stream::map(
                self.inner
                    .read_stream_at(inner_read_start, inner_read_size.into()),
                |_: &(), stream_item_bytebuf_result| {
                    stream_item_bytebuf_result
                        .map_err(StreamReaderBufferedAppenderError::ReadError)
                        .map(StreamReaderBufferedAppenderByteBuf::Read)
                },
            ),
            inner_read_size > zero() && position < append_buffer_anchor_position,
        );

        let append_buffer_read_bytes = self
            .append_buffer
            .read_at(append_buffer_read_start, append_buffer_read_size)
            .unwrap_or(&[]);
        let buffered_bytes =
            StreamReaderBufferedAppenderBufferedBytesStream::new(append_buffer_read_bytes);

        stream::chain(inner_stream, buffered_bytes)
    }
}

impl<R, AB> AsyncAppend for StreamReaderBufferedAppender<R, AB, R::Position, R::Size>
where
    R: AsyncAppend,
    AB: DerefMut<Target = [u8]>,
{
    async fn append(
        &mut self,
        bytes: &[u8],
    ) -> Result<AppendLocation<Self::Position, Self::Size>, Self::Error> {
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
            .map_err(Self::Error::AppendBufferError)?;

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
                    .map_err(Self::Error::AppendBufferError),
            ),
            (AppendStrategy::Inner, Ok(_)) => (
                Some(AdvanceAnchor),
                self.inner
                    .append(bytes)
                    .await
                    .map_err(Self::Error::AppendError),
            ),
            (_, Err(error)) => (None, Err(error)),
        } {
            (Some(AdvanceAnchor), Ok(append_location)) => self
                .append_buffer
                .advance_anchor_by(append_location.write_len)
                .map_err(Self::Error::AppendBufferError)
                .map(|_| append_location),
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
pub struct BufferedStreamReader<R, RB, P, S> {
    inner: R,
    read_buffer: AnchoredBuffer<RB, P>,

    _phantom_data: PhantomData<S>,
}

pub enum BufferedStreamReaderError<E> {
    ReadError(E),
    RemoveError(E),
    CloseError(E),

    ReadBeyondWrittenArea,
    ReadBufferError(BufferError),

    IntegerConversionError,
}

impl<E> From<IntegerConversionError> for BufferedStreamReaderError<E> {
    fn from(_: IntegerConversionError) -> Self {
        Self::IntegerConversionError
    }
}
impl<R, RB, P, S> AsyncRemove for BufferedStreamReader<R, RB, P, S>
where
    R: AsyncRemove,
{
    fn remove(self) -> impl Future<Output = Result<(), Self::Error>> {
        self.inner.remove().map_err(Self::Error::RemoveError)
    }
}

impl<R, RB, P, S> AsyncClose for BufferedStreamReader<R, RB, P, S>
where
    R: AsyncClose,
    RB: DerefMut<Target = [u8]>,
{
    async fn close(self) -> Result<(), Self::Error> {
        self.inner.close().await.map_err(Self::Error::CloseError)
    }
}

impl<R, RB> SizedEntity for BufferedStreamReader<R, RB, R::Position, R::Size>
where
    R: SizedEntity,
{
    type Position = R::Position;

    type Size = R::Size;

    fn size(&self) -> Self::Size {
        self.inner.size()
    }
}

impl<R, RB, P, S> FallibleEntity for BufferedStreamReader<R, RB, P, S>
where
    R: FallibleEntity,
{
    type Error = BufferedStreamReaderError<R::Error>;
}

pub enum BufferedStreamReaderByteBuf<'a, T> {
    Buffered(&'a [u8]),
    Read(T),
    Delim,
}

impl<'a, T> Deref for BufferedStreamReaderByteBuf<'a, T>
where
    T: Deref<Target = [u8]> + 'a,
{
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Buffered(buffered) => buffered,
            Self::Read(read_bytes) => read_bytes,
            Self::Delim => &[],
        }
    }
}

pub struct BufferedStreamReaderByteLender<BL>(PhantomData<BL>);

impl<BL> ByteLender for BufferedStreamReaderByteLender<BL>
where
    BL: ByteLender,
{
    type ByteBuf<'a> = BufferedStreamReaderByteBuf<'a, BL::ByteBuf<'a>>
    where
        Self: 'a;
}

pub enum BufferedStreamReaderReadStreamState<P, S> {
    ConsumeBuffer {
        read_position: P,
        read_size: S,
        remainder: S,
    },
    ReanchorBuffer {
        read_position: P,
        remainder: S,
    },
    FillBuffer {
        read_position: P,
        remainder: S,
    },
    Done,
}

#[allow(unused)]
pub struct BufferedStreamReaderReadStream<'a, R, RS, RB, P, S> {
    state: BufferedStreamReaderReadStreamState<P, S>,

    inner_stream: RS,
    read_buffer: &'a mut AnchoredBuffer<RB, P>,

    inner_size: S,

    _phantom_data: PhantomData<R>,
}

impl<'x, R, RBL, RS, RB>
    Stream<
        FallibleByteLender<
            BufferedStreamReaderByteLender<RBL>,
            BufferedStreamReaderError<R::Error>,
        >,
    > for BufferedStreamReaderReadStream<'x, R, RS, RB, R::Position, R::Size>
where
    R: StreamRead<RBL>,
    R::Error: 'static,
    RBL: ByteLender + 'static,
    RB: DerefMut<Target = [u8]>,
    RS: Stream<FallibleByteLender<RBL, R::Error>>,
{
    async fn next<'a>(
        &'a mut self,
    ) -> Option<
        <FallibleByteLender<
            BufferedStreamReaderByteLender<RBL>,
            BufferedStreamReaderError<R::Error>,
        > as Lender>::Item<'a>,
    >
    where
        FallibleByteLender<
            BufferedStreamReaderByteLender<RBL>,
            BufferedStreamReaderError<R::Error>,
        >: 'a,
    {
        let (item, next_state) = match self.state {
            BufferedStreamReaderReadStreamState::ConsumeBuffer {
                read_position,
                read_size,
                remainder,
            } => (
                Some(
                    self.read_buffer
                        .read_at(read_position, read_size)
                        .map(BufferedStreamReaderByteBuf::Buffered)
                        .map_err(BufferedStreamReaderError::ReadBufferError),
                ),
                BufferedStreamReaderReadStreamState::FillBuffer {
                    read_position: read_position + read_size.into(),
                    remainder,
                },
            ),

            BufferedStreamReaderReadStreamState::ReanchorBuffer {
                read_position,
                remainder,
            } => {
                self.read_buffer.re_anchor(read_position);
                (
                    Some(Ok(BufferedStreamReaderByteBuf::Delim)),
                    BufferedStreamReaderReadStreamState::FillBuffer {
                        read_position,
                        remainder,
                    },
                )
            }

            BufferedStreamReaderReadStreamState::FillBuffer {
                read_position,
                remainder,
            } if remainder == zero() || read_position >= self.inner_size.into() => {
                (None, BufferedStreamReaderReadStreamState::Done)
            }

            BufferedStreamReaderReadStreamState::FillBuffer {
                read_position,
                remainder,
            } if self.read_buffer.len() == self.read_buffer.capacity()
                || read_position != self.read_buffer.end_position().ok()? =>
            {
                (
                    Some(Ok(BufferedStreamReaderByteBuf::Delim)),
                    BufferedStreamReaderReadStreamState::ReanchorBuffer {
                        read_position,
                        remainder,
                    },
                )
            }

            BufferedStreamReaderReadStreamState::FillBuffer {
                read_position,
                remainder,
            } => {
                let read_bytes = self
                    .inner_stream
                    .next()
                    .await?
                    .map_err(BufferedStreamReaderError::ReadError)
                    .and_then(|x| {
                        let avail_to_append = self.read_buffer.avail_to_append();

                        self.read_buffer
                            .append(&x.deref()[..avail_to_append])
                            .map_err(BufferedStreamReaderError::ReadBufferError)
                            .and(Ok(BufferedStreamReaderByteBuf::Read(x)))
                    });

                let read_bytes_len =
                    R::Size::from_usize(read_bytes.as_ref().map(|x| x.len()).unwrap_or(0))?;

                let remainder = remainder.checked_sub(&read_bytes_len).unwrap_or(zero());
                let read_position = read_position + read_bytes_len.into();

                (
                    Some(read_bytes),
                    BufferedStreamReaderReadStreamState::FillBuffer {
                        read_position,
                        remainder,
                    },
                )
            }

            BufferedStreamReaderReadStreamState::Done => {
                (None, BufferedStreamReaderReadStreamState::Done)
            }
        };

        self.state = next_state;

        item
    }
}

impl<R, RBL, AB> StreamRead<BufferedStreamReaderByteLender<RBL>>
    for BufferedStreamReader<R, AB, R::Position, R::Size>
where
    R: StreamRead<RBL>,
    R::Error: 'static,
    RBL: ByteLender + 'static,
    AB: DerefMut<Target = [u8]>,
{
    fn read_stream_at<'a>(
        &'a mut self,
        position: Self::Position,
        size: Self::Size,
    ) -> impl Stream<FallibleByteLender<BufferedStreamReaderByteLender<RBL>, Self::Error>> + 'a
    where
        BufferedStreamReaderByteLender<RBL>: 'a,
    {
        let inner_size = self.inner.size();

        let (initial_state, inner_read_pos, inner_read_size) =
            if self.read_buffer.contains_position(position) {
                let avail_to_read_from_pos = self.read_buffer.avail_to_read_from_pos(position);
                let avail_to_read_from_pos =
                    R::Size::from_usize(avail_to_read_from_pos).unwrap_or(zero());

                let remainder = size.checked_sub(&avail_to_read_from_pos).unwrap_or(zero());

                (
                    BufferedStreamReaderReadStreamState::ConsumeBuffer {
                        read_position: position,
                        read_size: avail_to_read_from_pos,
                        remainder,
                    },
                    position + avail_to_read_from_pos.into(),
                    remainder,
                )
            } else {
                (
                    BufferedStreamReaderReadStreamState::FillBuffer {
                        read_position: position,
                        remainder: size,
                    },
                    position,
                    size,
                )
            };

        BufferedStreamReaderReadStream {
            state: initial_state,
            inner_stream: self.inner.read_stream_at(inner_read_pos, inner_read_size),
            read_buffer: &mut self.read_buffer,
            inner_size,
            _phantom_data: PhantomData::<R>,
        }
    }
}

#[allow(unused)]
pub struct BufferedStreamReaderBufferedAppender<R, RB, AB, P, S> {
    inner: R,

    append_buffer: AnchoredBuffer<AB, P>,
    read_buffer: AnchoredBuffer<RB, P>,

    _phantom_data: PhantomData<S>,
}

pub enum BufferedStreamReaderBufferedAppenderError<E> {
    AppendError(E),
    ReadError(E),
    RemoveError(E),
    CloseError(E),

    AppendBufferError(BufferError),
    ReadBufferError(BufferError),

    ReadBeyondWrittenArea,
    FlushIncomplete,

    IntegerConversionError,
}

impl<E> From<IntegerConversionError> for BufferedStreamReaderBufferedAppenderError<E> {
    fn from(_: IntegerConversionError) -> Self {
        Self::IntegerConversionError
    }
}
impl<R, RB, AB, P, S> AsyncRemove for BufferedStreamReaderBufferedAppender<R, RB, AB, P, S>
where
    R: AsyncRemove,
{
    fn remove(self) -> impl Future<Output = Result<(), Self::Error>> {
        self.inner.remove().map_err(Self::Error::RemoveError)
    }
}

impl<R, RB, AB, P, S> AsyncClose for BufferedStreamReaderBufferedAppender<R, RB, AB, P, S>
where
    R: AsyncClose,
    RB: DerefMut<Target = [u8]>,
{
    async fn close(self) -> Result<(), Self::Error> {
        self.inner.close().await.map_err(Self::Error::CloseError)
    }
}

impl<R, RB, AB> SizedEntity
    for BufferedStreamReaderBufferedAppender<R, RB, AB, R::Position, R::Size>
where
    R: SizedEntity,
{
    type Position = R::Position;

    type Size = R::Size;

    fn size(&self) -> Self::Size {
        self.inner.size()
    }
}

impl<R, RB, AB, P, S> FallibleEntity for BufferedStreamReaderBufferedAppender<R, RB, AB, P, S>
where
    R: FallibleEntity,
{
    type Error = BufferedStreamReaderBufferedAppenderError<R::Error>;
}
pub enum BufferedStreamReaderBufferedAppenderByteBuf<'a, T> {
    Buffered(&'a [u8]),
    Read(T),
    Delim,
}

impl<'a, T> Deref for BufferedStreamReaderBufferedAppenderByteBuf<'a, T>
where
    T: Deref<Target = [u8]> + 'a,
{
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Buffered(buffered) => buffered,
            Self::Read(read_bytes) => read_bytes,
            Self::Delim => &[],
        }
    }
}

pub struct BufferedStreamReaderBufferedAppenderByteLender<BL>(PhantomData<BL>);

impl<BL> ByteLender for BufferedStreamReaderBufferedAppenderByteLender<BL>
where
    BL: ByteLender,
{
    type ByteBuf<'a> = BufferedStreamReaderBufferedAppenderByteBuf<'a, BL::ByteBuf<'a>>
    where
        Self: 'a;
}

pub enum BufferedStreamReaderBufferedAppenderReadStreamState<P, S> {
    ConsumeReadBuffer {
        read_position: P,
        read_size: S,
        remainder: S,
    },
    ReanchorReadBuffer {
        read_position: P,
        remainder: S,
    },
    FillReadBuffer {
        read_position: P,
        remainder: S,
    },
    ConsumeAppendBuffer {
        read_position: P,
        read_size: S,
        remainder: S,
    },
    Done,
}

#[allow(unused)]
pub struct BufferedStreamReaderBufferedAppenderReadStream<'a, R, RS, RB, AB, P, S> {
    state: BufferedStreamReaderBufferedAppenderReadStreamState<P, S>,

    inner_stream: RS,

    read_buffer: &'a mut AnchoredBuffer<RB, P>,
    append_buffer: &'a AnchoredBuffer<AB, P>,

    inner_size: S,

    _phantom_data: PhantomData<R>,
}
