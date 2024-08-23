use bytes::Bytes;
use glommio::{
    io::{BufferedFile as BufferedFile_, ReadResult},
    GlommioError,
};

use crate::io_types::{
    AppendLocation, AsyncAppend, AsyncClose, AsyncRead, AsyncRemove, AsyncTruncate, ByteLender,
    FallibleEntity, IntegerConversionError, OwnedByteLender, ReadBytes, SizedEntity,
    UnwrittenError,
};

pub enum BufferedFileError {
    InnerError(GlommioError<()>),
    IntegerConversionError,
}

impl From<IntegerConversionError> for BufferedFileError {
    fn from(_: IntegerConversionError) -> Self {
        Self::IntegerConversionError
    }
}

#[allow(unused)]
pub struct BufferedFile {
    inner: BufferedFile_,
    size: u64,
}

impl FallibleEntity for BufferedFile {
    type Error = BufferedFileError;
}

impl SizedEntity for BufferedFile {
    type Position = u64;

    type Size = u64;

    fn size(&self) -> Self::Size {
        self.size
    }
}

impl AsyncTruncate for BufferedFile {
    async fn truncate(&mut self, position: Self::Position) -> Result<(), Self::Error> {
        self.inner
            .fdatasync()
            .await
            .map_err(BufferedFileError::InnerError)?;

        self.inner
            .truncate(position)
            .await
            .map(|_| {
                self.size = position;
            })
            .map_err(BufferedFileError::InnerError)
    }
}

impl AsyncAppend for BufferedFile {
    async fn append(
        &mut self,
        bytes: Bytes,
    ) -> Result<AppendLocation<Self::Position, Self::Size>, UnwrittenError<Self::Error>> {
        let write_position: Self::Position = self.size;

        let write_len: Self::Size = self
            .inner
            .write_at(bytes.clone().into(), write_position)
            .await
            .map_err(|err| UnwrittenError {
                unwritten: bytes.clone(),
                err: BufferedFileError::InnerError(err),
            })?
            .try_into()
            .map_err(|_| UnwrittenError {
                unwritten: bytes,
                err: BufferedFileError::IntegerConversionError,
            })?;

        self.size += write_len;

        Ok(AppendLocation {
            write_position,
            write_len,
        })
    }
}

impl AsyncRead<OwnedByteLender<ReadResult>> for BufferedFile {
    async fn read_at<'a>(
        &'a mut self,
        position: Self::Position,
        size: Self::Size,
    ) -> Result<
        ReadBytes<<OwnedByteLender<ReadResult> as ByteLender>::ByteBuf<'a>, Self::Size>,
        Self::Error,
    >
    where
        OwnedByteLender<ReadResult>: 'a,
    {
        let size: usize = size
            .try_into()
            .map_err(|_| Self::Error::IntegerConversionError)?;

        self.inner
            .read_at(position, size)
            .await
            .map_err(Self::Error::InnerError)
            .and_then(|read_result| {
                read_result
                    .len()
                    .try_into()
                    .map_err(|_| Self::Error::IntegerConversionError)
                    .map(|read_len| ReadBytes {
                        read_bytes: read_result,
                        read_len,
                    })
            })
    }
}

impl AsyncRemove for BufferedFile {
    async fn remove(self) -> Result<(), Self::Error> {
        self.inner.remove().await.map_err(Self::Error::InnerError)
    }
}

impl AsyncClose for BufferedFile {
    async fn close(self) -> Result<(), Self::Error> {
        self.inner.close().await.map_err(Self::Error::InnerError)
    }
}
