use std::{marker::PhantomData, ops::Deref};

use bytes::Bytes;
use num::{FromPrimitive, ToPrimitive};

use crate::{
    anchored_buffer::{AnchoredBuffer, BufferError},
    io_types::{
        AppendLocation, AsyncAppend, AsyncClose, AsyncFlush, AsyncRead, AsyncRemove, ByteLender,
        FallibleEntity, IntegerConversionError, ReadBytes, SizedEntity, UnwrittenError,
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
    R: AsyncFlush + AsyncAppend,
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

impl<R> AsyncAppend for BufferedAppender<R, R::Position, R::Size>
where
    R: AsyncAppend + AsyncFlush,
{
    async fn append(
        &mut self,
        bytes: Bytes,
    ) -> Result<AppendLocation<Self::Position, Self::Size>, UnwrittenError<Self::Error>> {
        enum AppendDest {
            Buffer,
            Inner,
        }

        enum Action {
            Flush { dest_after_flush: AppendDest },
            AppendToBuffer,
        }

        struct ReanchorBufferAfterFlushAndInnerAppend;

        let buffer_end_position = self.buffer.end_position().map_err(|err| UnwrittenError {
            unwritten: bytes.clone(),
            err: Self::Error::AppendBufferError(err),
        })?;

        let bytes_len = R::Size::from_usize(bytes.len()).ok_or(UnwrittenError {
            unwritten: bytes.clone(),
            err: Self::Error::IntegerConversionError,
        })?;

        match match match match match bytes.len() {
            n if n >= self.buffer.capacity() => Action::Flush {
                dest_after_flush: AppendDest::Inner,
            },
            n if n >= self.buffer.avail_to_append() => Action::Flush {
                dest_after_flush: AppendDest::Buffer,
            },
            _ => Action::AppendToBuffer,
        } {
            Action::Flush { dest_after_flush } => (
                dest_after_flush,
                self.flush().await.map_err(|err| UnwrittenError {
                    unwritten: bytes.clone(),
                    err,
                }),
            ),
            Action::AppendToBuffer => (AppendDest::Buffer, Ok(())),
        } {
            (AppendDest::Buffer, Ok(_)) => {
                let mut buffer_append_slice_mut =
                    self.buffer
                        .get_append_slice_mut()
                        .map_err(|err| UnwrittenError {
                            unwritten: bytes.clone(),
                            err: Self::Error::AppendBufferError(err),
                        })?;

                buffer_append_slice_mut.copy_from_slice(&bytes);

                self.buffer
                    .unsplit_append_slice(buffer_append_slice_mut, bytes.len())
                    .map_err(|err| UnwrittenError {
                        unwritten: bytes.clone(),
                        err: Self::Error::AppendBufferError(err),
                    })?;

                (
                    None,
                    Ok(AppendLocation {
                        write_position: buffer_end_position,
                        write_len: bytes_len,
                    }),
                )
            }
            (AppendDest::Inner, Ok(_)) => (
                Some(ReanchorBufferAfterFlushAndInnerAppend),
                self.inner
                    .append(bytes)
                    .await
                    .map_err(|UnwrittenError { unwritten, err }| UnwrittenError {
                        unwritten,
                        err: Self::Error::AppendError(err),
                    }),
            ),
            (_, Err(err)) => (None, Err(err)),
        } {
            (
                Some(ReanchorBufferAfterFlushAndInnerAppend),
                Ok(
                    append_location @ AppendLocation {
                        write_position,
                        write_len,
                    },
                ),
            ) => {
                self.buffer.re_anchor(write_position + write_len.into());
                Ok(append_location)
            }
            (_, result) => result,
        } {
            Ok(
                append_location @ AppendLocation {
                    write_position: _,
                    write_len,
                },
            ) => {
                self.size += write_len;
                Ok(append_location)
            }
            Err(err) => Err(err),
        }
    }
}

impl<R> AsyncClose for BufferedAppender<R, R::Position, R::Size>
where
    R: AsyncAppend + AsyncFlush + AsyncClose,
{
    async fn close(mut self) -> Result<(), Self::Error> {
        self.flush().await?;

        self.inner.close().await.map_err(Self::Error::CloseError)
    }
}

impl<R, P, S> AsyncRemove for BufferedAppender<R, P, S>
where
    R: AsyncRemove,
{
    async fn remove(self) -> Result<(), Self::Error> {
        self.inner.remove().await.map_err(Self::Error::RemoveError)
    }
}
