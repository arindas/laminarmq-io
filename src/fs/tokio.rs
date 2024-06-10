use num::ToPrimitive;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::{io, marker::PhantomData, path::PathBuf};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

use crate::{
    AppendLocation, AsyncAppend, AsyncBufRead, AsyncClose, AsyncRemove, AsyncTruncate,
    FallibleEntity, IntegerConversionError, ReadBytesLen, SizedEntity,
};

pub struct RandomRead;

pub struct Seek;

#[allow(unused)]
pub struct TokioFile<K> {
    inner: File,
    backing_file_path: PathBuf,

    size: u64,

    _phantom_data: PhantomData<K>,
}

impl<K> TokioFile<K> {
    pub async fn new<P: AsRef<Path>>(path: P) -> Result<Self, TokioFileError> {
        let backing_file_path = path.as_ref().to_path_buf();

        let inner = OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .create(true)
            .open(path.as_ref())
            .await
            .map_err(TokioFileError::IoError)?;

        let initial_size = inner
            .metadata()
            .await
            .map_err(TokioFileError::IoError)?
            .len();

        Ok(Self {
            inner,
            backing_file_path,
            size: initial_size,
            _phantom_data: PhantomData,
        })
    }
}

impl<K> SizedEntity for TokioFile<K> {
    type Position = u64;

    type Size = u64;

    fn size(&self) -> Self::Size {
        self.size
    }
}

pub enum TokioFileError {
    IoError(io::Error),

    IntoStdFileConversionFailed,

    IntegerConversionError,
}

impl From<IntegerConversionError> for TokioFileError {
    fn from(_: IntegerConversionError) -> Self {
        Self::IntegerConversionError
    }
}

impl<K> FallibleEntity for TokioFile<K> {
    type Error = TokioFileError;
}

impl<K> AsyncTruncate for TokioFile<K> {
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

impl<K> AsyncAppend for TokioFile<K> {
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

impl AsyncBufRead for TokioFile<Seek> {
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

#[cfg(target_family = "unix")]
impl AsyncBufRead for TokioFile<RandomRead> {
    async fn read_at_buf(
        &mut self,
        position: Self::Position,
        buffer: &mut [u8],
    ) -> Result<ReadBytesLen<Self::Size>, Self::Error> {
        let reader = self
            .inner
            .try_clone()
            .await
            .map_err(Self::Error::IoError)?
            .try_into_std()
            .map_err(|_| Self::Error::IntoStdFileConversionFailed)?;

        let mut read_buffer = vec![0_u8; buffer.len()];

        let read_buffer = tokio::task::spawn_blocking(move || {
            reader
                .read_exact_at(&mut read_buffer, position)
                .map(|_| read_buffer)
        })
        .await
        .unwrap()
        .unwrap();

        buffer.copy_from_slice(&read_buffer);

        Ok(ReadBytesLen { read_len: 0 })
    }
}

impl<K> AsyncRemove for TokioFile<K> {
    async fn remove(self) -> Result<(), Self::Error> {
        tokio::fs::remove_file(self.backing_file_path)
            .await
            .map_err(Self::Error::IoError)
    }
}

impl<K> AsyncClose for TokioFile<K> {
    async fn close(mut self) -> Result<(), Self::Error> {
        self.inner.flush().await.map_err(Self::Error::IoError)?;

        drop(self.inner);

        Ok(())
    }
}
