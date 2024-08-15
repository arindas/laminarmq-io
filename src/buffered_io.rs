use std::{marker::PhantomData, ops::Deref};

use bytes::Bytes;
use num::ToPrimitive;

use crate::{
    anchored_buffer::{AnchoredBuffer, BufferError},
    io_types::{
        AppendLocation, AsyncAppend, AsyncFlush, AsyncRead, ByteLender, FallibleEntity,
        IntegerConversionError, ReadBytes, SizedEntity, UnwrittenError,
    },
};

pub enum BufferedReadByteBuf<T> {
    Buffered(Bytes),
    Read(T),
}

impl<T> Deref for BufferedReadByteBuf<T>
where
    T: Deref<Target = [u8]>,
{
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match self {
            BufferedReadByteBuf::Buffered(buffered) => buffered.deref(),
            BufferedReadByteBuf::Read(read) => read.deref(),
        }
    }
}

pub struct BufferedByteLender<BL>(PhantomData<BL>);

impl<BL> ByteLender for BufferedByteLender<BL>
where
    BL: ByteLender,
{
    type ByteBuf<'a> = BufferedReadByteBuf<BL::ByteBuf<'a>>
    where
        Self: 'a;
}

#[allow(unused)]
pub struct BufferedAppender<R, P, S> {
    inner: R,
    buffer: AnchoredBuffer<P>,
    flush_state: FlushState,
    size: S,
}

pub enum BufferedAppenderError<E> {
    AppendError(E),
    ReadError(E),
    RemoveError(E),
    CloseError(E),

    AppendBufferError(BufferError),

    InvalidWriteSize,

    ReadBeyondWrittenArea,
    ReadGapEncountered,
    FlushIncomplete,

    IntegerConversionError,
}

impl<E> From<IntegerConversionError> for BufferedAppenderError<E> {
    fn from(_: IntegerConversionError) -> Self {
        Self::IntegerConversionError
    }
}

impl<R, P, S> FallibleEntity for BufferedAppender<R, P, S>
where
    R: FallibleEntity,
{
    type Error = BufferedAppenderError<R::Error>;
}

impl<R> SizedEntity for BufferedAppender<R, R::Position, R::Size>
where
    R: SizedEntity,
{
    type Position = R::Position;

    type Size = R::Size;

    fn size(&self) -> Self::Size {
        self.size
    }
}

impl<R, BL> AsyncRead<BufferedByteLender<BL>> for BufferedAppender<R, R::Position, R::Size>
where
    R: AsyncRead<BL>,
    BL: ByteLender,
{
    async fn read_at<'a>(
        &'a mut self,
        position: Self::Position,
        size: Self::Size,
    ) -> Result<
        ReadBytes<<BufferedByteLender<BL> as ByteLender>::ByteBuf<'a>, Self::Size>,
        Self::Error,
    >
    where
        BufferedByteLender<BL>: 'a,
    {
        match position {
            pos if !self.contains(pos) => Err(Self::Error::ReadBeyondWrittenArea),
            pos if self.buffer.contains_position(pos) => self
                .buffer
                .read_at(pos, size)
                .map(|x| x.map(BufferedReadByteBuf::Buffered))
                .map_err(Self::Error::AppendBufferError),
            pos if self.inner.contains(pos) => self
                .inner
                .read_at(pos, size)
                .await
                .map(|x| x.map(BufferedReadByteBuf::Read))
                .map_err(Self::Error::ReadError),
            _ => Err(Self::Error::ReadGapEncountered),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum FlushState {
    Incomplete { retry_flush_buffer_offset: usize },
    Clean,
}

impl<R> AsyncFlush for BufferedAppender<R, R::Position, R::Size>
where
    R: SizedEntity + AsyncFlush + AsyncAppend,
{
    async fn flush(&mut self) -> Result<(), Self::Error> {
        let flush_buffer_offset = match self.flush_state {
            FlushState::Incomplete {
                retry_flush_buffer_offset,
            } => retry_flush_buffer_offset,
            FlushState::Clean => 0,
        };

        let bytes = self
            .buffer
            .get_read_slice(flush_buffer_offset..)
            .map_err(Self::Error::AppendBufferError)?;

        let bytes_len = bytes.len();

        let inner_append_result = self.inner.append(bytes).await;

        match inner_append_result {
            Ok(AppendLocation {
                write_position,
                write_len,
            }) if write_len
                .to_usize()
                .ok_or(Self::Error::IntegerConversionError)?
                == bytes_len =>
            {
                self.flush_state = FlushState::Clean;
                self.buffer.re_anchor(write_position + write_len.into());

                self.inner.flush().await.map_err(Self::Error::AppendError)?;

                Ok(())
            }

            Ok(AppendLocation {
                write_position,
                write_len,
            }) => {
                self.flush_state = FlushState::Incomplete {
                    retry_flush_buffer_offset: self
                        .buffer
                        .offset(write_position + write_len.into())
                        .ok_or(Self::Error::InvalidWriteSize)?,
                };

                self.inner.flush().await.map_err(Self::Error::AppendError)?;

                Err(Self::Error::FlushIncomplete)
            }

            Err(UnwrittenError { unwritten: _, err }) => Err(Self::Error::AppendError(err)),
        }
    }
}
