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

#[derive(Clone, Copy, Debug)]
pub struct WriteLocation<P, S> {
    pub position: P,
    pub len: S,
}

impl<P, S> WriteLocation<P, S>
where
    P: Quantifier,
    S: Quantifier + Into<P>,
{
    #[inline]
    pub fn end_position(&self) -> P {
        self.position + self.len.into()
    }
}

#[derive(Clone, Debug)]
pub struct WriteOutcome<P, S> {
    pub location: WriteLocation<P, S>,
    pub written: Bytes,
}

pub struct Unwritten<E> {
    pub unwritten: Bytes,
    pub err: E,
}

impl<E> Unwritten<E> {
    pub fn map_err<U, F: FnOnce(E) -> U>(self, op: F) -> Unwritten<U> {
        Unwritten {
            unwritten: self.unwritten,
            err: op(self.err),
        }
    }
}

pub trait AsyncWrite: SizedEntity + FallibleEntity {
    fn write(
        &mut self,
        bytes: Bytes,
    ) -> impl Future<Output = Result<WriteOutcome<Self::Position, Self::Size>, Unwritten<Self::Error>>>;

    fn write_all(
        &mut self,
        bytes: Bytes,
    ) -> impl Future<Output = Result<WriteOutcome<Self::Position, Self::Size>, Unwritten<Self::Error>>>
    {
        async {
            let write_position = self.size().into();
            let bytes_len = Self::Size::from_usize(bytes.len()).unwrap_or(zero());
            let mut written: Self::Size = zero();

            while written < bytes_len {
                let num_bytes_written = written.to_usize().unwrap_or(0);
                let bytes_to_write = bytes.slice(num_bytes_written..);
                let WriteOutcome {
                    location:
                        WriteLocation {
                            position: _,
                            len: write_len,
                        },
                    written: _,
                } = self.write(bytes_to_write).await?;

                written += write_len;
            }

            Ok(WriteOutcome {
                location: WriteLocation {
                    position: write_position,
                    len: written,
                },
                written: bytes,
            })
        }
    }
}

pub enum StreamWriteError<E, XE> {
    WriteOverflow,
    StreamReadError(XE),
    InnerError(E),
}

pub trait StreamWrite: SizedEntity + FallibleEntity {
    fn write_stream<XE, X>(
        &mut self,
        stream: &mut X,
        opts: StreamWriteOpts<Self::Size>,
    ) -> impl Future<Output = StreamWriteResult<Self::Position, Self::Size, Self::Error, XE>>
    where
        X: Stream<OwnedLender<Result<Bytes, XE>>>,
        X: Unpin;
}

pub type StreamWriteResult<P, S, E, XE> =
    Result<WriteLocation<P, S>, Unwritten<StreamWriteError<E, XE>>>;

#[derive(Clone, Copy, Debug)]
pub struct StreamWriteOpts<S> {
    pub write_threshold: Option<S>,
    pub rollback: bool,
}

impl<A> StreamWrite for A
where
    A: AsyncWrite + AsyncTruncate,
{
    async fn write_stream<XE, X>(
        &mut self,
        stream: &mut X,
        opts: StreamWriteOpts<Self::Size>,
    ) -> StreamWriteResult<Self::Position, Self::Size, Self::Error, XE>
    where
        X: Stream<OwnedLender<Result<Bytes, XE>>> + Unpin,
    {
        let (mut bytes_written, write_position) = (zero(), self.size().into());

        while let Some(buf) = stream.next().await {
            let buf_len_opt = buf
                .as_ref()
                .ok()
                .and_then(|x| Self::Size::from_usize(x.len()));

            match match match (buf, buf_len_opt, opts.write_threshold) {
                (Ok(buf), Some(buf_len), Some(thresh)) if bytes_written + buf_len <= thresh => {
                    Ok(buf)
                }
                (Ok(buf), _, Some(_)) => Err(Unwritten {
                    err: StreamWriteError::WriteOverflow,
                    unwritten: buf,
                }),
                (Ok(buf), _, None) => Ok(buf),
                (Err(err), _, _) => Err(Unwritten {
                    err: StreamWriteError::StreamReadError(err),
                    unwritten: Bytes::new(),
                }),
            } {
                Ok(buf) => self
                    .write_all(buf)
                    .await
                    .map_err(|x| x.map_err(StreamWriteError::InnerError)),
                Err(error) => Err(error),
            } {
                Ok(WriteOutcome {
                    written: _,
                    location:
                        WriteLocation {
                            position: _,
                            len: write_len,
                        },
                }) => bytes_written += write_len,

                Err(Unwritten { unwritten, err }) if opts.rollback => {
                    self.truncate(write_position)
                        .await
                        .map_err(|err| Unwritten {
                            err: StreamWriteError::InnerError(err),
                            unwritten: unwritten.slice(..),
                        })?;

                    return Err(Unwritten { unwritten, err });
                }

                Err(error) => {
                    return Err(error);
                }
            }
        }

        Ok(WriteLocation {
            position: write_position,
            len: bytes_written,
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

pub struct AsyncReadStreamReadStream<'a, R, B, P, SZ> {
    reader: &'a mut R,
    position: P,
    bytes_to_read: SZ,

    _phantom_data: PhantomData<B>,
}

impl<'x, R, B> Stream<FallibleByteLender<B, R::Error>>
    for AsyncReadStreamReadStream<'x, R, B, R::Position, R::Size>
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

        if read_bytes.read_len == zero() {
            return None;
        }

        self.position += read_bytes.read_len.into();

        self.bytes_to_read -= read_bytes.read_len;

        Some(Ok(read_bytes.read_bytes))
    }
}

pub struct AsyncReadStreamRead<R>(R);

impl<R> DerefMut for AsyncReadStreamRead<R> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<R> Deref for AsyncReadStreamRead<R> {
    type Target = R;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<R> AsyncReadStreamRead<R> {
    pub fn into_inner(self) -> R {
        self.0
    }
}

impl<R> SizedEntity for AsyncReadStreamRead<R>
where
    R: SizedEntity,
{
    type Position = R::Position;

    type Size = R::Size;

    fn size(&self) -> Self::Size {
        self.0.size()
    }
}

impl<R> FallibleEntity for AsyncReadStreamRead<R>
where
    R: FallibleEntity,
{
    type Error = R::Error;
}

impl<R, B> StreamRead<B> for AsyncReadStreamRead<R>
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
        AsyncReadStreamReadStream {
            reader: &mut self.0,
            position,
            bytes_to_read: size,
            _phantom_data: PhantomData,
        }
    }
}
