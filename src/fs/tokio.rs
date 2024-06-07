use num::ToPrimitive;
use std::io;
use tokio::{
    fs::File as TokioFile,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

use crate::{
    AppendLocation, AsyncAppend, AsyncBufRead, AsyncTruncate, FallibleEntity,
    IntegerConversionError, ReadBytesLen, SizedEntity,
};

#[allow(unused)]
pub struct File {
    inner: TokioFile,
    size: u64,
}

impl SizedEntity for File {
    type Position = u64;

    type Size = u64;

    fn size(&self) -> Self::Size {
        self.size
    }
}

pub enum TokioFileError {
    IoError(io::Error),

    InvalidOp,

    IntegerConversionError,
}

impl From<IntegerConversionError> for TokioFileError {
    fn from(_: IntegerConversionError) -> Self {
        Self::IntegerConversionError
    }
}

impl FallibleEntity for File {
    type Error = TokioFileError;
}

impl AsyncTruncate for File {
    async fn truncate(&mut self, position: Self::Position) -> Result<(), Self::Error> {
        self.inner.flush().await.map_err(Self::Error::IoError)?;

        self.inner
            .set_len(position)
            .await
            .map_err(Self::Error::IoError)?;

        self.size = position;

        Ok(())
    }
}

impl AsyncAppend for File {
    async fn append(
        &mut self,
        bytes: &[u8],
    ) -> Result<AppendLocation<Self::Position, Self::Size>, Self::Error> {
        let write_position = self.size();

        let write_len = self
            .inner
            .write(bytes)
            .await
            .map_err(Self::Error::IoError)?
            .to_u64()
            .ok_or(Self::Error::IntegerConversionError)?;

        self.inner.flush().await.map_err(Self::Error::IoError)?;

        self.size += write_len;

        Ok(AppendLocation {
            write_position,
            write_len,
        })
    }
}

impl AsyncBufRead for File {
    async fn read_at_buf(
        &mut self,
        position: Self::Position,
        buffer: &mut [u8],
    ) -> Result<ReadBytesLen<Self::Size>, Self::Error> {
        self.inner
            .seek(io::SeekFrom::Start(position))
            .await
            .map_err(Self::Error::IoError)?;

        let read_len = self
            .inner
            .read(buffer)
            .await
            .map_err(Self::Error::IoError)?
            .to_u64()
            .ok_or(Self::Error::IntegerConversionError)?;

        self.inner
            .seek(io::SeekFrom::Start(self.size))
            .await
            .map_err(Self::Error::IoError)?;

        Ok(ReadBytesLen { read_len })
    }
}
