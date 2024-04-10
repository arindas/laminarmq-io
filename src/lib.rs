use std::{
    fmt::Debug,
    future::Future,
    ops::{Add, Deref, DerefMut},
};

use futures::{Stream, StreamExt};

pub struct AppendLocation<P, S> {
    pub write_position: P,
    pub bytes_written: S,
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

pub type StreamAppendResult<AA> = Result<
    AppendLocation<<AA as SizedStorage>::Position, <AA as SizedStorage>::Size>,
    StreamAppendError<<AA as AsyncAppend>::AppendError, <AA as AsyncTruncate>::TruncateError>,
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
                    bytes_written: buf_bytes_w,
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
            bytes_written,
        })
    }
}

pub struct ReadBytes<T, S> {
    pub bytes: T,
    pub bytes_read: S,
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

pub trait AsyncBufRead: SizedStorage {
    type BufReadError: Debug;

    fn read_at_buf<T>(
        &mut self,
        position: Self::Position,
        size: Self::Size,
        buffer: T,
    ) -> impl Future<Output = Result<ReadBytes<T, Self::Size>, Self::BufReadError>> + Send
    where
        T: DerefMut<Target = [u8]>;
}

pub trait ByteBufStream {
    type ByteBuf<'a>: Deref<Target = [u8]> + 'a
    where
        Self: 'a;

    type Error;

    fn next<'a>(
        &'a mut self,
    ) -> impl Future<Output = Option<Result<Self::ByteBuf<'a>, Self::Error>>> + Send;
}
