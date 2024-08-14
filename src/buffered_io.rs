use std::{marker::PhantomData, ops::Deref};

use bytes::Bytes;

use crate::{
    anchored_buffer::{AnchoredBuffer, BufferError},
    io_types::{
        AsyncRead, ByteLender, FallibleEntity, IntegerConversionError, ReadBytes, SizedEntity,
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
    size: S,
}

pub enum BufferedAppenderError<E> {
    AppendError(E),
    ReadError(E),
    RemoveError(E),
    CloseError(E),

    AppendBufferError(BufferError),

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
