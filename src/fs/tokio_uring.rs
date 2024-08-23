use std::{
    io,
    path::{Path, PathBuf},
};

use crate::io_types::{
    AppendLocation, AsyncAppend, AsyncBufRead, AsyncClose, AsyncFlush, AsyncRemove, FallibleEntity,
    IntegerConversionError, ReadBytes, SizedEntity, UnreadError, UnwrittenError,
};

use bytes::{Bytes, BytesMut};
use tokio_uring::fs::{File, OpenOptions};

pub struct TokioUringFile {
    inner: File,
    backing_file_path: PathBuf,
    size: u64,
}

pub enum TokioUringFileError {
    IoError(io::Error),

    IntegerConversionError,
}

impl TokioUringFile {
    pub async fn new<P: AsRef<Path>>(path: P) -> Result<Self, TokioUringFileError> {
        let backing_file_path = path.as_ref().to_path_buf();

        let inner = OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .create(true)
            .open(path.as_ref())
            .await
            .map_err(TokioUringFileError::IoError)?;

        let initial_size = inner
            .statx()
            .await
            .map_err(TokioUringFileError::IoError)?
            .stx_size;

        Ok(Self {
            inner,
            backing_file_path,
            size: initial_size,
        })
    }
}

impl SizedEntity for TokioUringFile {
    type Position = u64;

    type Size = u64;

    fn size(&self) -> Self::Size {
        self.size
    }
}

impl From<IntegerConversionError> for TokioUringFileError {
    fn from(_: IntegerConversionError) -> Self {
        Self::IntegerConversionError
    }
}

impl FallibleEntity for TokioUringFile {
    type Error = TokioUringFileError;
}

impl AsyncAppend for TokioUringFile {
    async fn append(
        &mut self,
        bytes: Bytes,
    ) -> Result<AppendLocation<Self::Position, Self::Size>, UnwrittenError<Self::Error>> {
        let write_position = self.size;

        let (result, bytes) = self.inner.write_at(bytes, write_position).submit().await;

        match result {
            Ok(write_len) => {
                self.size += write_len as u64;
                Ok(AppendLocation {
                    write_position,
                    write_len: write_len as u64,
                })
            }
            Err(err) => Err(UnwrittenError {
                unwritten: bytes,
                err: Self::Error::IoError(err),
            }),
        }
    }
}

impl AsyncBufRead for TokioUringFile {
    async fn read_at_buf(
        &mut self,
        position: Self::Position,
        buffer: BytesMut,
    ) -> Result<ReadBytes<BytesMut, Self::Size>, UnreadError<Self::Error>> {
        let (result, buffer) = self.inner.read_at(buffer, position).await;

        match result {
            Ok(read_len) => Ok(ReadBytes {
                read_bytes: buffer,
                read_len: read_len as u64,
            }),
            Err(err) => Err(UnreadError {
                unread: buffer,
                err: Self::Error::IoError(err),
            }),
        }
    }
}

impl AsyncFlush for TokioUringFile {
    async fn flush(&mut self) -> Result<(), Self::Error> {
        self.inner.sync_data().await.map_err(Self::Error::IoError)
    }
}

impl AsyncRemove for TokioUringFile {
    async fn remove(self) -> Result<(), Self::Error> {
        tokio_uring::fs::remove_file(&self.backing_file_path)
            .await
            .map_err(Self::Error::IoError)
    }
}

impl AsyncClose for TokioUringFile {
    async fn close(mut self) -> Result<(), Self::Error> {
        self.flush().await?;

        self.inner.close().await.map_err(Self::Error::IoError)
    }
}
