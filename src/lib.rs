use std::{
    fmt::Debug,
    future::Future,
    ops::{Add, Deref},
};

use futures::{Stream, StreamExt};

pub struct AppendLocation<P, S> {
    pub write_position: P,
    pub write_len: S,
}

pub trait SizedStorage {
    type Position: Add<Output = Self::Position> + Ord + From<usize> + From<Self::Size>;
    type Size: Add<Output = Self::Size> + Ord + From<usize>;

    fn size(&self) -> Self::Size;
}

pub trait AsyncTruncate: SizedStorage {
    type TruncateError;

    fn truncate(
        &mut self,
        position: Self::Position,
    ) -> impl Future<Output = Result<(), Self::TruncateError>> + Send;
}

pub trait AsyncAppend: SizedStorage {
    type AppendError;

    fn append(
        &mut self,
        bytes: &[u8],
    ) -> impl Future<Output = Result<AppendLocation<Self::Position, Self::Size>, Self::AppendError>> + Send;
}

pub enum StreamAppendError<AE, TE> {
    AppendOverflow,
    StreamReadError,
    TruncateError(TE),
    AppendError(AE),
}

pub type StreamAsyncAppendError<AA> =
    StreamAppendError<<AA as AsyncAppend>::AppendError, <AA as AsyncTruncate>::TruncateError>;

pub type StreamAppendResult<AA> = Result<
    AppendLocation<<AA as SizedStorage>::Position, <AA as SizedStorage>::Size>,
    StreamAsyncAppendError<AA>,
>;

pub struct AsyncStreamAppend<'a, AA>(&'a mut AA);

impl<'a, AA> AsyncStreamAppend<'a, AA>
where
    AA: AsyncAppend + AsyncTruncate,
    AA::Size: Copy,
{
    pub async fn append_stream<XBuf, XE, X>(
        self,
        stream: &mut X,
        append_threshold: Option<AA::Size>,
    ) -> StreamAppendResult<AA>
    where
        XBuf: Deref<Target = [u8]>,
        X: Stream<Item = Result<XBuf, XE>> + Unpin,
    {
        let file = self.0;

        let (mut bytes_written, write_position) = (0.into(), file.size().into());

        while let Some(buf) = stream.next().await {
            match match match (buf, append_threshold) {
                (Ok(buf), Some(thresh)) if (bytes_written + buf.len().into()) <= thresh => Ok(buf),
                (Ok(_), Some(_)) => Err(StreamAppendError::AppendOverflow),
                (Ok(buf), None) => Ok(buf),
                (Err(_), _) => Err(StreamAppendError::StreamReadError),
            } {
                Ok(buf) => file
                    .append(buf.deref())
                    .await
                    .map_err(StreamAppendError::AppendError),
                Err(error) => Err(error),
            } {
                Ok(AppendLocation {
                    write_position: _,
                    write_len: buf_bytes_w,
                }) => {
                    bytes_written = bytes_written + buf_bytes_w;
                }
                Err(error) => {
                    file.truncate(write_position)
                        .await
                        .map_err(StreamAppendError::TruncateError)?;
                    return Err(error);
                }
            };
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

pub trait AsyncRead: SizedStorage {
    type ByteBuf<'a>: Deref<Target = [u8]> + 'a
    where
        Self: 'a;

    type ReadError;

    fn read_at<'a>(
        &'a mut self,
        position: Self::Position,
        size: Self::Size,
    ) -> impl Future<Output = Result<ReadBytes<Self::ByteBuf<'a>, Self::Size>, Self::ReadError>>;
}

pub struct ReadBytesLen<T>(T);

pub trait AsyncBufRead: SizedStorage {
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

    fn next<'a>(
        &'a mut self,
    ) -> impl Future<Output = Option<Result<Self::ByteBuf<'a>, Self::Error>>>;
}

pub trait StreamRead: SizedStorage {
    type Stream<'a>: ByteBufStream + 'a
    where
        Self: 'a;

    fn read_at<'a>(&'a mut self, position: Self::Position, size: Self::Size) -> Self::Stream<'a>;
}
