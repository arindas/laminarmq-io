use std::io;

use num::ToPrimitive;
use tokio::{
    fs::File as TokioFile,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

use crate::{
    AppendLocation, AsyncAppend, AsyncBufRead, AsyncTruncate, IntegerConversionError, ReadBytesLen,
    SizedEntity,
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

impl AsyncTruncate for File {
    type TruncateError = TokioFileError;

    async fn truncate(&mut self, position: Self::Position) -> Result<(), Self::TruncateError> {
        self.inner
            .flush()
            .await
            .map_err(Self::TruncateError::IoError)?;

        self.inner
            .set_len(position)
            .await
            .map_err(Self::TruncateError::IoError)?;

        self.size = position;

        Ok(())
    }
}

impl AsyncAppend for File {
    type AppendError = TokioFileError;

    async fn append(
        &mut self,
        bytes: &[u8],
    ) -> Result<AppendLocation<Self::Position, Self::Size>, Self::AppendError> {
        let write_position = self.size();

        let write_len = self
            .inner
            .write(bytes)
            .await
            .map_err(Self::AppendError::IoError)?
            .to_u64()
            .ok_or(Self::AppendError::IntegerConversionError)?;

        self.inner
            .flush()
            .await
            .map_err(Self::AppendError::IoError)?;

        self.size += write_len;

        Ok(AppendLocation {
            write_position,
            write_len,
        })
    }
}

impl From<IntegerConversionError> for TokioFileError {
    fn from(_: IntegerConversionError) -> Self {
        TokioFileError::IntegerConversionError
    }
}

impl AsyncBufRead for File {
    type BufReadError = TokioFileError;

    async fn read_at_buf(
        &mut self,
        position: Self::Position,
        buffer: &mut [u8],
    ) -> Result<ReadBytesLen<Self::Size>, Self::BufReadError> {
        self.inner
            .seek(io::SeekFrom::Start(position))
            .await
            .map_err(Self::BufReadError::IoError)?;

        let read_len = self
            .inner
            .read(buffer)
            .await
            .map_err(Self::BufReadError::IoError)?
            .to_u64()
            .ok_or(Self::BufReadError::IntegerConversionError)?;

        self.inner
            .seek(io::SeekFrom::Start(self.size))
            .await
            .map_err(Self::BufReadError::IoError)?;

        Ok(ReadBytesLen { read_len })
    }
}
