use std::{
    cmp::min,
    convert::Into,
    future::Future,
    ops::{Add, AddAssign, Bound, Deref, DerefMut, RangeBounds, Sub, SubAssign},
};

use futures::{Stream, StreamExt};
use num::{CheckedSub, Unsigned};

pub trait Quantifier:
    Add<Output = Self>
    + Sub
    + AddAssign
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

                Err(error) => file
                    .truncate(write_position.into())
                    .await
                    .map_err(AsyncStreamAppendError::TruncateError)
                    .and_then(|_| Err(error))?,
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

pub struct ReadBytesLen<T>(T);

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
    R::Size: SubAssign,
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
    R::Size: SubAssign,
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
    end: usize,
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
        self.end
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
            (Bound::Included(&start), Bound::Unbounded) => Ok((start, self.end - 1)),
            (Bound::Excluded(&start), Bound::Included(&end)) => Ok((start + 1, end)),
            (Bound::Excluded(&start), Bound::Excluded(&end)) => Ok((start + 1, end - 1)),
            (Bound::Excluded(&start), Bound::Unbounded) => Ok((start + 1, self.end - 1)),
            (Bound::Unbounded, Bound::Included(&end)) => Ok((0, end)),
            (Bound::Unbounded, Bound::Excluded(&end)) => Ok((0, end - 1)),
            (Bound::Unbounded, Bound::Unbounded) => Ok((0, self.end - 1)),
        } {
            Ok((start, end)) if start > end => Err(BufferError::BadSliceRange),
            Ok((start, end)) if start >= self.end || end >= self.end => {
                Err(BufferError::IndexOutOfBounds)
            }
            Ok((start, end)) => Ok((start, end)),
            Err(e) => Err(e),
        }?;

        Ok(&self.buffer[start..=end])
    }

    pub fn get_append_slice_mut(&mut self) -> &mut [X] {
        &mut self.buffer[self.end..]
    }

    pub fn advance(&mut self, n: usize) -> Result<usize, BufferError> {
        if n > self.remaining() {
            Err(BufferError::BufferOverflow)
        } else {
            self.end += n;
            Ok(n)
        }
    }

    pub fn append(&mut self, src: &[X]) -> Result<usize, BufferError> {
        if src.len() > self.remaining() {
            return Err(BufferError::BufferOverflow);
        }

        self.get_append_slice_mut()[..src.len()].copy_from_slice(src);

        self.advance(src.len())
    }

    pub fn clear(&mut self) {
        self.end = 0
    }
}

pub struct DirectReaderBufferedWriter<'a, R, S> {
    inner: R,
    read_limit: S,

    write_buffer: Buffer<&'a mut [u8]>,

    flushed_size: S,
    current_size: S,
}

pub enum DirectReaderBufferedWriterError<RE, AE> {
    ReadError(RE),
    AppendError(AE),
    ReadBeyondWrittenArea,
    WriterBufferError(BufferError),
}

#[allow(unused)]
impl<'a, R> DirectReaderBufferedWriter<'a, R, R::Size>
where
    R: AsyncRead + AsyncAppend,
{
    async fn flush(
        &mut self,
    ) -> Result<(), DirectReaderBufferedWriterError<R::ReadError, R::AppendError>> {
        let buffered_bytes = self
            .write_buffer
            .get_read_slice(..)
            .map_err(DirectReaderBufferedWriterError::WriterBufferError)?;

        self.inner
            .append(buffered_bytes)
            .await
            .map_err(DirectReaderBufferedWriterError::AppendError)?;

        self.flushed_size += buffered_bytes.len().into();

        self.write_buffer.clear();

        Ok(())
    }
}

impl<'a, R> SizedEntity for DirectReaderBufferedWriter<'a, R, R::Size>
where
    R: SizedEntity,
{
    type Position = R::Position;

    type Size = R::Size;

    fn size(&self) -> Self::Size {
        self.current_size
    }
}

pub enum DirectReaderBufferedWriterByteBuf<'a, T> {
    Buffered(&'a [u8]),
    Read(T),
}

impl<'a, T> Deref for DirectReaderBufferedWriterByteBuf<'a, T>
where
    T: Deref<Target = [u8]> + 'a,
{
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match self {
            DirectReaderBufferedWriterByteBuf::Buffered(buffered) => buffered,
            DirectReaderBufferedWriterByteBuf::Read(read_bytes) => read_bytes,
        }
    }
}

impl<'a, R> AsyncRead for DirectReaderBufferedWriter<'a, R, R::Size>
where
    R: AsyncRead + AsyncAppend,
{
    type ByteBuf<'x> = DirectReaderBufferedWriterByteBuf<'x, R::ByteBuf<'x>>
    where
        Self: 'x;

    type ReadError = DirectReaderBufferedWriterError<R::ReadError, R::AppendError>;

    async fn read_at(
        &mut self,
        position: Self::Position,
        size: Self::Size,
    ) -> Result<ReadBytes<Self::ByteBuf<'_>, Self::Size>, Self::ReadError> {
        enum ReadStrategy {
            Inner,
            Buffered,
        }

        enum ReadRequest<P, S> {
            Buffrered { offset: usize, end: usize },
            Inner { position: P, size: S },
        }

        match match match position {
            pos if pos >= Into::<R::Position>::into(self.size()) => {
                Err(DirectReaderBufferedWriterError::ReadBeyondWrittenArea)
            }
            pos if pos >= Into::<R::Position>::into(self.flushed_size) => {
                Ok((self.size(), ReadStrategy::Buffered))
            }
            _ => Ok((self.flushed_size, ReadStrategy::Inner)),
        }
        .map(|(end, read_strategy)| ((end - Into::<R::Size>::into(position)), read_strategy))
        .map(|(available_bytes, read_strategy)| {
            (
                min(size, min(available_bytes, self.read_limit)),
                read_strategy,
            )
        }) {
            Ok((allowed_read_size, ReadStrategy::Buffered)) => {
                let offset: usize = position.into() - self.flushed_size.into();

                Ok(ReadRequest::Buffrered {
                    offset,
                    end: offset + allowed_read_size.into(),
                })
            }
            Ok((allowed_read_size, ReadStrategy::Inner)) => Ok(ReadRequest::Inner {
                position,
                size: allowed_read_size,
            }),

            Err(e) => Err(e),
        } {
            Ok(ReadRequest::Buffrered { offset, end }) => self
                .write_buffer
                .get_read_slice(offset..end)
                .map(|x| ReadBytes {
                    read_bytes: DirectReaderBufferedWriterByteBuf::Buffered(x),
                    read_len: x.len().into(),
                })
                .map_err(DirectReaderBufferedWriterError::WriterBufferError),
            Ok(ReadRequest::Inner { position, size }) => self
                .inner
                .read_at(position, size)
                .await
                .map(|x| x.map(DirectReaderBufferedWriterByteBuf::Read))
                .map_err(DirectReaderBufferedWriterError::ReadError),

            Err(e) => Err(e),
        }
    }
}

impl<'a, R> AsyncAppend for DirectReaderBufferedWriter<'a, R, R::Size>
where
    R: AsyncRead + AsyncAppend,
{
    type AppendError = DirectReaderBufferedWriterError<R::ReadError, R::AppendError>;

    async fn append(
        &mut self,
        bytes: &[u8],
    ) -> Result<AppendLocation<Self::Position, Self::Size>, Self::AppendError> {
        enum AppendStrategy {
            Buffered,
            Direct,
            Flush,
        }

        let append_strategy = match bytes.len() {
            n if n >= self.write_buffer.capacity() => AppendStrategy::Direct,
            n if n >= self.write_buffer.remaining() => AppendStrategy::Flush,
            _ => AppendStrategy::Buffered,
        };

        match match append_strategy {
            strat @ (AppendStrategy::Flush | AppendStrategy::Direct) => {
                self.flush().await?;
                strat
            }
            strat => strat,
        } {
            AppendStrategy::Buffered | AppendStrategy::Flush => {
                let write_position = self.flushed_size + self.write_buffer.len().into();

                let append_location = AppendLocation {
                    write_position: Into::<R::Position>::into(write_position),
                    write_len: self
                        .write_buffer
                        .append(bytes)
                        .map_err(DirectReaderBufferedWriterError::WriterBufferError)?
                        .into(),
                };

                self.current_size += append_location.write_len;

                Ok(append_location)
            }
            AppendStrategy::Direct => {
                let append_location = self
                    .inner
                    .append(bytes)
                    .await
                    .map_err(DirectReaderBufferedWriterError::AppendError)?;

                self.flushed_size += append_location.write_len;
                self.current_size += append_location.write_len;

                Ok(append_location)
            }
        }
    }
}
