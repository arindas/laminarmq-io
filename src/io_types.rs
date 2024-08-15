#[allow(unused)]
use std::{
    cmp::{max, min},
    convert::Into,
    future::Future,
    marker::PhantomData,
    ops::{Add, AddAssign, Bound, Deref, DerefMut, Not, RangeBounds, Sub, SubAssign},
};

use bytes::{Bytes, BytesMut};
#[allow(unused)]
use futures::TryFutureExt;
#[allow(unused)]
use num::{zero, CheckedSub, FromPrimitive, ToPrimitive, Unsigned, Zero};

#[allow(unused)]
use crate::stream::{Lender, OwnedLender, Stream};

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

pub trait SizedEntity {
    type Position: Quantifier + From<Self::Size>;
    type Size: Quantifier + From<Self::Position>;

    fn size(&self) -> Self::Size;

    fn contains(&self, position: Self::Position) -> bool {
        position < Self::Position::from(self.size())
    }
}

pub struct IntegerConversionError;

pub trait FallibleEntity {
    type Error: From<IntegerConversionError>;
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

pub struct AppendLocation<P, S> {
    pub write_position: P,
    pub write_len: S,
}

pub struct UnwrittenError<E> {
    pub unwritten: Bytes,
    pub err: E,
}

pub trait AsyncAppend: SizedEntity + FallibleEntity {
    fn append(
        &mut self,
        bytes: Bytes,
    ) -> impl Future<
        Output = Result<AppendLocation<Self::Position, Self::Size>, UnwrittenError<Self::Error>>,
    >;
}

pub enum StreamAppendError<E, XE> {
    AppendOverflow,
    StreamReadError(XE),
    InnerError(E),
}

pub trait StreamAppend: SizedEntity + FallibleEntity {
    fn append_stream<XE, X>(
        &mut self,
        stream: &mut X,
        append_threshold: Option<Self::Size>,
        rollback: bool,
    ) -> impl Future<Output = StreamAppendResult<Self::Position, Self::Size, Self::Error, XE>>
    where
        X: Stream<OwnedLender<Result<Bytes, XE>>>,
        X: Unpin;
}

pub type StreamAppendResult<P, S, E, XE> =
    Result<AppendLocation<P, S>, UnwrittenError<StreamAppendError<E, XE>>>;

impl<A> StreamAppend for A
where
    A: AsyncAppend + AsyncTruncate,
{
    async fn append_stream<XE, X>(
        &mut self,
        stream: &mut X,
        append_threshold: Option<Self::Size>,
        rollback: bool,
    ) -> StreamAppendResult<Self::Position, Self::Size, Self::Error, XE>
    where
        X: Stream<OwnedLender<Result<Bytes, XE>>> + Unpin,
    {
        let (mut bytes_written, write_position) = (zero(), self.size().into());
        while let Some(buf) = stream.next().await {
            match match match (buf, append_threshold) {
                (Ok(buf), Some(thresh))
                    if (bytes_written
                        + A::Size::from_usize(buf.len()).ok_or(UnwrittenError {
                            err: StreamAppendError::InnerError(IntegerConversionError.into()),
                            unwritten: buf.clone(),
                        })?)
                        <= thresh =>
                {
                    Ok(buf)
                }
                (Ok(buf), Some(_)) => Err(UnwrittenError {
                    err: StreamAppendError::AppendOverflow,
                    unwritten: buf,
                }),
                (Ok(buf), None) => Ok(buf),
                (Err(err), _) => Err(UnwrittenError {
                    err: StreamAppendError::StreamReadError(err),
                    unwritten: Bytes::new(),
                }),
            } {
                Ok(buf) => self
                    .append(buf)
                    .await
                    .map_err(|UnwrittenError { unwritten, err }| UnwrittenError {
                        unwritten,
                        err: StreamAppendError::InnerError(err),
                    }),
                Err(error) => Err(error),
            } {
                Ok(AppendLocation {
                    write_position: _,
                    write_len,
                }) => bytes_written += write_len,

                Err(UnwrittenError { unwritten, err }) if rollback => {
                    self.truncate(write_position)
                        .await
                        .map_err(|err| UnwrittenError {
                            err: StreamAppendError::InnerError(err),
                            unwritten: unwritten.clone(),
                        })?;

                    return Err(UnwrittenError { unwritten, err });
                }

                Err(error) => {
                    return Err(error);
                }
            }
        }

        Ok(AppendLocation {
            write_position,
            write_len: bytes_written,
        })
    }
}

pub trait AsyncFlush: FallibleEntity {
    fn flush(&mut self) -> impl Future<Output = Result<(), Self::Error>>;
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
    pub read_len: T,
}

pub struct UnreadError<E> {
    pub unread: BytesMut,
    pub err: E,
}

pub trait AsyncBufRead: SizedEntity + FallibleEntity {
    fn read_at_buf(
        &mut self,
        position: Self::Position,
        buffer: BytesMut,
    ) -> impl Future<Output = Result<ReadBytes<BytesMut, Self::Size>, UnreadError<Self::Error>>>;

    fn read_at_buf_sized(
        &mut self,
        position: Self::Position,
        size: Self::Size,
        mut buffer: BytesMut,
    ) -> impl Future<Output = Result<ReadBytes<BytesMut, Self::Size>, UnreadError<Self::Error>>>
    {
        async move {
            let size = size.to_usize().map(|size| min(size, buffer.len()));

            if size.is_none() {
                return Err(UnreadError {
                    unread: buffer,
                    err: IntegerConversionError.into(),
                });
            }

            let size = unsafe { size.unwrap_unchecked() }; // SAFETY: see none check above

            let remainder = buffer.split_off(size);

            let result = self.read_at_buf(position, buffer).await;

            match result {
                Ok(ReadBytes {
                    mut read_bytes,
                    read_len,
                }) => {
                    read_bytes.unsplit(remainder);
                    Ok(ReadBytes {
                        read_bytes,
                        read_len,
                    })
                }
                Err(UnreadError { mut unread, err }) => {
                    unread.unsplit(remainder);
                    Err(UnreadError { unread, err })
                }
            }
        }
    }
}

pub trait ByteLender {
    type ByteBuf<'a>: Deref<Target = [u8]> + 'a
    where
        Self: 'a;
}

pub struct OwnedByteLender<T>(PhantomData<T>);

impl<T> ByteLender for OwnedByteLender<T>
where
    T: Deref<Target = [u8]>,
{
    type ByteBuf<'a> = T
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
