use std::{
    cmp::{max, min},
    marker::PhantomData,
    ops::Deref,
};

use bytes::{Bytes, BytesMut};
use num::{zero, CheckedSub, FromPrimitive, ToPrimitive};

use crate::{
    anchored_buffer::{AnchoredBuffer, BufferError},
    io_types::{
        AppendLocation, AsyncAppend, AsyncBufRead, AsyncClose, AsyncFlush, AsyncRead, AsyncRemove,
        AsyncTruncate, ByteLender, FallibleByteLender, FallibleEntity, IntegerConversionError,
        OwnedByteLender, ReadBytes, SizedEntity, StreamRead, UnreadError, UnwrittenError,
    },
    stream::{self, Lender, Stream},
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

pub struct OnceBufferedReadResult<E>(Option<Result<Bytes, E>>);

impl<RBL, E> Stream<FallibleByteLender<BufferedByteLender<RBL>, E>> for OnceBufferedReadResult<E>
where
    RBL: ByteLender,
{
    async fn next<'a>(
        &'a mut self,
    ) -> Option<<FallibleByteLender<BufferedByteLender<RBL>, E> as Lender>::Item<'a>>
    where
        FallibleByteLender<BufferedByteLender<RBL>, E>: 'a,
    {
        self.0
            .take()
            .map(|bytes| bytes.map(BufferedReadByteBuf::Buffered))
    }
}

#[allow(unused)]
pub struct BufferedAppender<R, P, S> {
    inner: R,
    buffer: AnchoredBuffer<P>,
    flush_state: FlushState,
    size: S,
}

pub enum BufferedAppenderError<E> {
    InnerError(E),

    BufferError(BufferError),

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
                .map_err(Self::Error::BufferError),
            pos if self.inner.contains(pos) => self
                .inner
                .read_at(pos, size)
                .await
                .map(|x| x.map(BufferedReadByteBuf::Read))
                .map_err(Self::Error::InnerError),
            _ => Err(Self::Error::ReadGapEncountered),
        }
    }
}

impl<R> AsyncBufRead for BufferedAppender<R, R::Position, R::Size>
where
    R: AsyncBufRead,
{
    async fn read_at_buf(
        &mut self,
        position: Self::Position,
        mut buffer: BytesMut,
    ) -> Result<ReadBytes<BytesMut, Self::Size>, UnreadError<Self::Error>> {
        let provided_buffer_len =
            R::Size::from_usize(buffer.len()).ok_or(Self::Error::IntegerConversionError);

        #[allow(clippy::blocks_in_conditions)]
        match match (provided_buffer_len, position) {
            (Ok(_), pos) if !self.contains(pos) => Err(Self::Error::ReadBeyondWrittenArea),
            (Ok(provided_buffer_len), pos) if self.buffer.contains_position(pos) => self
                .buffer
                .read_at(pos, provided_buffer_len)
                .map_err(Self::Error::BufferError)
                .map(
                    |ReadBytes {
                         read_bytes,
                         read_len,
                     }| {
                        buffer.copy_from_slice(&read_bytes);
                        read_len
                    },
                ),
            (Ok(_), pos) if self.inner.contains(pos) => {
                match self.inner.read_at_buf(pos, buffer).await {
                    Ok(ReadBytes {
                        read_bytes,
                        read_len,
                    }) => {
                        buffer = read_bytes;
                        Ok(read_len)
                    }
                    Err(UnreadError { unread, err }) => {
                        buffer = unread;
                        Err(Self::Error::InnerError(err))
                    }
                }
            }
            _ => Err(Self::Error::ReadGapEncountered),
        } {
            Ok(read_len) => Ok(ReadBytes {
                read_bytes: buffer,
                read_len,
            }),
            Err(err) => Err(UnreadError {
                unread: buffer,
                err,
            }),
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
            .map_err(Self::Error::BufferError)?;

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

                self.inner.flush().await.map_err(Self::Error::InnerError)?;

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

                self.inner.flush().await.map_err(Self::Error::InnerError)?;

                Err(Self::Error::FlushIncomplete)
            }

            Err(UnwrittenError { unwritten: _, err }) => Err(Self::Error::InnerError(err)),
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
            err: Self::Error::BufferError(err),
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
                            err: Self::Error::BufferError(err),
                        })?;

                buffer_append_slice_mut.copy_from_slice(&bytes);

                self.buffer
                    .unsplit_append_slice(buffer_append_slice_mut, bytes.len())
                    .map_err(|err| UnwrittenError {
                        unwritten: bytes.clone(),
                        err: Self::Error::BufferError(err),
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
                        err: Self::Error::InnerError(err),
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

        self.inner.close().await.map_err(Self::Error::InnerError)
    }
}

impl<R, P, S> AsyncRemove for BufferedAppender<R, P, S>
where
    R: AsyncRemove,
{
    async fn remove(self) -> Result<(), Self::Error> {
        self.inner.remove().await.map_err(Self::Error::InnerError)
    }
}

impl<R> AsyncTruncate for BufferedAppender<R, R::Position, R::Size>
where
    R: AsyncTruncate,
{
    async fn truncate(&mut self, position: Self::Position) -> Result<(), Self::Error> {
        if self.buffer.contains_position(position) {
            self.buffer
                .truncate(position)
                .map_err(Self::Error::BufferError)?;
        } else {
            self.inner
                .truncate(position)
                .await
                .map_err(Self::Error::InnerError)?;

            self.buffer.re_anchor(self.inner.size().into());
        }

        self.size = self.inner.size()
            + R::Size::from_usize(self.buffer.len()).ok_or(Self::Error::IntegerConversionError)?;

        Ok(())
    }
}

impl<R, RBL> StreamRead<BufferedByteLender<RBL>> for BufferedAppender<R, R::Position, R::Size>
where
    R: StreamRead<RBL>,
    R::Error: 'static,
    RBL: ByteLender + 'static,
{
    fn read_stream_at<'a>(
        &'a mut self,
        position: Self::Position,
        size: Self::Size,
    ) -> impl Stream<FallibleByteLender<BufferedByteLender<RBL>, Self::Error>> + 'a
    where
        BufferedByteLender<R::Error>: 'a,
    {
        let buffer_anchor_position = self.buffer.anchor_position();

        let inner_read_start = position;
        let inner_read_end = min(self.inner.size().into(), position + size.into());
        let expected_inner_read_size = inner_read_end
            .checked_sub(&inner_read_start)
            .unwrap_or(zero());

        let inner_read_necessary =
            expected_inner_read_size > zero() && position < buffer_anchor_position;

        let buffer_read_start = max(buffer_anchor_position, position);
        let buffer_read_end = min(position + size.into(), self.size().into());
        let expected_buffer_read_size = buffer_read_end
            .checked_sub(&buffer_read_start)
            .unwrap_or(zero());

        let buffered_read_necessary = expected_buffer_read_size > zero();

        stream::chain(
            stream::latch(
                stream::map(
                    self.inner
                        .read_stream_at(inner_read_start, expected_inner_read_size.into()),
                    |_: &(), stream_item_bytebuf_result| {
                        stream_item_bytebuf_result
                            .map_err(Self::Error::InnerError)
                            .map(BufferedReadByteBuf::Read)
                    },
                ),
                inner_read_necessary,
            ),
            OnceBufferedReadResult(buffered_read_necessary.then(|| {
                self.buffer
                    .read_at(buffer_read_start, expected_buffer_read_size)
                    .map(
                        |ReadBytes {
                             read_bytes,
                             read_len: _,
                         }| read_bytes,
                    )
                    .map_err(Self::Error::BufferError)
            })),
        )
    }
}

#[allow(unused)]
pub struct BufferedReader<R, P, S> {
    inner: R,
    buffer: AnchoredBuffer<P>,
    size: S,
}

pub enum BufferedReaderError<E> {
    InnerError(E),
    BufferError(BufferError),
    ReadBeyondWrittenArea,
    IntegerConversionError,
}

impl<E> From<IntegerConversionError> for BufferedReaderError<E> {
    fn from(_: IntegerConversionError) -> Self {
        Self::IntegerConversionError
    }
}

impl<R, P, S> FallibleEntity for BufferedReader<R, P, S>
where
    R: FallibleEntity,
{
    type Error = BufferedReaderError<R::Error>;
}

impl<R> SizedEntity for BufferedReader<R, R::Position, R::Size>
where
    R: SizedEntity,
{
    type Position = R::Position;

    type Size = R::Size;

    fn size(&self) -> Self::Size {
        self.size
    }
}

impl<R> AsyncRead<OwnedByteLender<Bytes>> for BufferedReader<R, R::Position, R::Size>
where
    R: AsyncBufRead,
{
    async fn read_at<'a>(
        &'a mut self,
        position: Self::Position,
        size: Self::Size,
    ) -> Result<
        ReadBytes<<OwnedByteLender<Bytes> as ByteLender>::ByteBuf<'a>, Self::Size>,
        Self::Error,
    >
    where
        OwnedByteLender<Bytes>: 'a,
    {
        struct Reanchor<P>(P);

        struct Fill<P>(P);

        enum ReadSource {
            Buffer,
        }

        let buffer_end_position = self
            .buffer
            .end_position()
            .map_err(Self::Error::BufferError)?;

        match match match match position {
            pos if !self.contains(pos) => Err(Self::Error::ReadBeyondWrittenArea),
            pos if self.buffer.contains_position(pos) => Ok((None, None, ReadSource::Buffer)),
            pos if self.buffer.contains_position_within_capacity(pos)
                && pos >= buffer_end_position =>
            {
                Ok((None, Some(Fill(pos)), ReadSource::Buffer))
            }
            pos => Ok((Some(Reanchor(pos)), Some(Fill(pos)), ReadSource::Buffer)),
        } {
            Ok((Some(Reanchor(pos)), fill, source)) => {
                self.buffer.re_anchor(pos);

                Ok((fill, source))
            }
            Ok((None, fill, source)) => Ok((fill, source)),
            Err(err) => Err(err),
        } {
            Ok((Some(Fill(pos)), source)) => {
                let append_buffer = self
                    .buffer
                    .get_append_slice_mut()
                    .map_err(Self::Error::BufferError)?;

                let (append_bytes_mut, bytes_written) =
                    match self.inner.read_at_buf(pos, append_buffer).await {
                        Ok(ReadBytes {
                            read_bytes,
                            read_len,
                        }) => (read_bytes, read_len.to_usize().unwrap_or(0)),
                        Err(UnreadError { unread, err: _ }) => (unread, 0),
                    };

                self.buffer
                    .unsplit_append_slice(append_bytes_mut, bytes_written)
                    .map_err(Self::Error::BufferError)?;

                Ok(source)
            }
            Ok((None, source)) => Ok(source),
            Err(err) => Err(err),
        } {
            Ok(ReadSource::Buffer) => self
                .buffer
                .read_at(position, size)
                .map_err(Self::Error::BufferError),
            Err(err) => Err(err),
        }
    }
}

impl<R, BL> AsyncRead<BufferedByteLender<BL>> for BufferedReader<R, R::Position, R::Size>
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
        struct Reanchor<P>(P);

        struct Fill<P>(P);

        enum ReadSource {
            Buffer,
            Inner,
        }

        let buffer_end_position = self
            .buffer
            .end_position()
            .map_err(Self::Error::BufferError)?;

        let buffer_capacitty = R::Size::from_usize(self.buffer.capacity())
            .ok_or(Self::Error::IntegerConversionError)?;

        match match match match position {
            pos if !self.contains(pos) => Err(Self::Error::ReadBeyondWrittenArea),
            pos if self.buffer.contains_position(pos) => Ok((None, None, ReadSource::Buffer)),
            pos if self.buffer.contains_position_within_capacity(pos)
                && pos >= buffer_end_position =>
            {
                Ok((None, Some(Fill(pos)), ReadSource::Buffer))
            }
            pos if size <= buffer_capacitty => {
                Ok((Some(Reanchor(pos)), Some(Fill(pos)), ReadSource::Buffer))
            }
            _ => Ok((None, None, ReadSource::Inner)),
        } {
            Ok((Some(Reanchor(pos)), fill, source)) => {
                self.buffer.re_anchor(pos);

                Ok((fill, source))
            }
            Ok((None, fill, source)) => Ok((fill, source)),
            Err(err) => Err(err),
        } {
            Ok((Some(Fill(pos)), source)) => {
                let mut append_buffer = self
                    .buffer
                    .get_append_slice_mut()
                    .map_err(Self::Error::BufferError)?;

                let ReadBytes {
                    read_bytes,
                    read_len,
                } = self
                    .inner
                    .read_at(pos, size)
                    .await
                    .map_err(Self::Error::InnerError)?;

                append_buffer.copy_from_slice(&read_bytes);

                self.buffer
                    .unsplit_append_slice(append_buffer, read_len.to_usize().unwrap_or(0))
                    .map_err(Self::Error::BufferError)?;

                Ok(source)
            }
            Ok((None, source)) => Ok(source),
            Err(err) => Err(err),
        } {
            Ok(ReadSource::Buffer) => self
                .buffer
                .read_at(position, size)
                .map(|x| x.map(BufferedReadByteBuf::Buffered))
                .map_err(Self::Error::BufferError),
            Ok(ReadSource::Inner) => self
                .inner
                .read_at(position, size)
                .await
                .map(|x| x.map(BufferedReadByteBuf::Read))
                .map_err(Self::Error::InnerError),
            Err(err) => Err(err),
        }
    }
}

#[allow(unused)]
enum BufferedReaderStreamReadState<P, S> {
    ConsumeBuffer {
        buffer_read_position: P,
        buffer_read_size: S,

        remainder: S,
    },

    FillBuffer {
        inner_read_position: P,
        remainder: S,
    },

    ReanchorBuffer {
        anchor_position: P,
        remainder: S,
    },

    Done,
}

#[allow(unused)]
pub struct BufferedReaderStreamReadStream<'a, R, RS, P, S> {
    state: BufferedReaderStreamReadState<P, S>,

    inner_stream_read_stream: RS,

    buffer: &'a mut AnchoredBuffer<P>,

    inner_size: S,

    _phantom_data: PhantomData<R>,
}

pub type FallibleBufferedReaderByteLender<RBL, E> =
    FallibleByteLender<BufferedByteLender<RBL>, BufferedReaderError<E>>;

impl<'x, R, RBL, RS> Stream<FallibleBufferedReaderByteLender<RBL, R::Error>>
    for BufferedReaderStreamReadStream<'x, R, RS, R::Position, R::Size>
where
    R: StreamRead<RBL>,
    R::Error: 'static,
    RBL: ByteLender + 'static,
    RS: Stream<FallibleByteLender<RBL, R::Error>>,
{
    async fn next<'a>(
        &'a mut self,
    ) -> Option<<FallibleBufferedReaderByteLender<RBL, R::Error> as Lender>::Item<'a>>
    where
        FallibleBufferedReaderByteLender<RBL, R::Error>: 'a,
    {
        let (item, next_state) = match self.state {
            BufferedReaderStreamReadState::ConsumeBuffer {
                buffer_read_position,
                buffer_read_size,
                remainder,
            } => (
                Some(
                    self.buffer
                        .read_at(buffer_read_position, buffer_read_size)
                        .map_err(BufferedReaderError::BufferError)
                        .map(
                            |ReadBytes {
                                 read_bytes,
                                 read_len: _,
                             }| {
                                BufferedReadByteBuf::Buffered(read_bytes)
                            },
                        ),
                ),
                BufferedReaderStreamReadState::FillBuffer {
                    inner_read_position: buffer_read_position + buffer_read_size.into(),
                    remainder,
                },
            ),

            BufferedReaderStreamReadState::FillBuffer {
                inner_read_position,
                remainder,
            } if remainder == zero() || inner_read_position >= self.inner_size.into() => {
                (None, BufferedReaderStreamReadState::Done)
            }

            BufferedReaderStreamReadState::FillBuffer {
                inner_read_position,
                remainder,
            } => match self.inner_stream_read_stream.next().await {
                Some(Ok(read_bytes)) if read_bytes.len() > self.buffer.avail_to_append() => {
                    let read_bytes_len = read_bytes.len();
                    let new_anchor_position =
                        R::Position::from_usize(read_bytes_len).map(|x| x + inner_read_position);

                    let remainder = R::Size::from_usize(read_bytes_len)
                        .and_then(|read_bytes_len| remainder.checked_sub(&read_bytes_len));

                    match (new_anchor_position, remainder) {
                        (Some(anchor_position), Some(rem)) => (
                            Some(Ok(BufferedReadByteBuf::Read(read_bytes))),
                            BufferedReaderStreamReadState::ReanchorBuffer {
                                anchor_position,
                                remainder: rem,
                            },
                        ),
                        _ => (
                            Some(Err(BufferedReaderError::IntegerConversionError)),
                            BufferedReaderStreamReadState::Done,
                        ),
                    }
                }

                Some(Ok(read_bytes)) => {
                    let read_bytes_len = read_bytes.len();
                    let new_inner_read_position =
                        R::Position::from_usize(read_bytes_len).map(|x| x + inner_read_position);

                    let remainder = R::Size::from_usize(read_bytes_len)
                        .and_then(|read_bytes_len| remainder.checked_sub(&read_bytes_len));

                    let buffer_append_result = self
                        .buffer
                        .get_append_slice_mut()
                        .map(|mut x| {
                            x.copy_from_slice(&read_bytes);
                            x
                        })
                        .and_then(|append_bytes_mut| {
                            self.buffer
                                .unsplit_append_slice(append_bytes_mut, read_bytes.len())
                        });

                    match (new_inner_read_position, remainder, buffer_append_result) {
                        (_, _, Err(err)) => (
                            Some(Err(BufferedReaderError::BufferError(err))),
                            BufferedReaderStreamReadState::Done,
                        ),
                        (Some(pos), Some(rem), Ok(())) => (
                            Some(Ok(BufferedReadByteBuf::Read(read_bytes))),
                            BufferedReaderStreamReadState::FillBuffer {
                                inner_read_position: pos,
                                remainder: rem,
                            },
                        ),

                        _ => (
                            Some(Err(BufferedReaderError::IntegerConversionError)),
                            BufferedReaderStreamReadState::Done,
                        ),
                    }
                }

                Some(Err(err)) => (
                    Some(Err(BufferedReaderError::InnerError(err))),
                    BufferedReaderStreamReadState::Done,
                ),

                None => (None, BufferedReaderStreamReadState::Done),
            },

            BufferedReaderStreamReadState::ReanchorBuffer {
                anchor_position,
                remainder,
            } => {
                self.buffer.re_anchor(anchor_position);

                (
                    Some(Ok(BufferedReadByteBuf::Buffered(Bytes::new()))),
                    BufferedReaderStreamReadState::FillBuffer {
                        inner_read_position: anchor_position,
                        remainder,
                    },
                )
            }

            BufferedReaderStreamReadState::Done => (None, BufferedReaderStreamReadState::Done),
        };

        self.state = next_state;

        item
    }
}

impl<R, RBL> StreamRead<BufferedByteLender<RBL>> for BufferedReader<R, R::Position, R::Size>
where
    R: StreamRead<RBL>,
    R::Error: 'static,
    RBL: ByteLender + 'static,
{
    fn read_stream_at<'a>(
        &'a mut self,
        position: Self::Position,
        size: Self::Size,
    ) -> impl Stream<FallibleByteLender<BufferedByteLender<RBL>, Self::Error>> + 'a
    where
        BufferedByteLender<RBL>: 'a,
    {
        let inner_size = self.inner.size();

        let (initial_state, inner_read_pos, inner_read_size) = match position {
            pos if self.buffer.contains_position(pos) => {
                let avail_to_read_from_pos = self.buffer.avail_to_read_from_pos(position);
                let avail_to_read_from_pos =
                    R::Size::from_usize(avail_to_read_from_pos).unwrap_or(zero());

                let remainder = size.checked_sub(&avail_to_read_from_pos).unwrap_or(zero());

                (
                    BufferedReaderStreamReadState::ConsumeBuffer {
                        buffer_read_position: position,
                        buffer_read_size: avail_to_read_from_pos,
                        remainder,
                    },
                    position + avail_to_read_from_pos.into(),
                    remainder,
                )
            }

            pos => (
                BufferedReaderStreamReadState::FillBuffer {
                    inner_read_position: pos,
                    remainder: size,
                },
                pos,
                size,
            ),
        };

        BufferedReaderStreamReadStream {
            state: initial_state,
            inner_stream_read_stream: self.inner.read_stream_at(inner_read_pos, inner_read_size),
            buffer: &mut self.buffer,
            inner_size,
            _phantom_data: PhantomData::<R>,
        }
    }
}

impl<R> AsyncTruncate for BufferedReader<R, R::Position, R::Size>
where
    R: AsyncTruncate,
{
    async fn truncate(&mut self, position: Self::Position) -> Result<(), Self::Error> {
        self.inner
            .truncate(position)
            .await
            .map_err(Self::Error::InnerError)?;

        match position {
            pos if pos < self.buffer.anchor_position() => {
                self.buffer.re_anchor(zero());
            }
            pos if self.buffer.contains_position(pos) => {
                self.buffer
                    .truncate(pos)
                    .map_err(Self::Error::BufferError)?;
            }
            _ => {}
        }

        Ok(())
    }
}

impl<R, P, S> AsyncRemove for BufferedReader<R, P, S>
where
    R: AsyncRemove,
{
    async fn remove(self) -> Result<(), Self::Error> {
        self.inner.remove().await.map_err(Self::Error::InnerError)
    }
}

impl<R, P, S> AsyncClose for BufferedReader<R, P, S>
where
    R: AsyncClose,
{
    async fn close(self) -> Result<(), Self::Error> {
        self.inner.close().await.map_err(Self::Error::InnerError)
    }
}
