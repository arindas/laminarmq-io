use bytes::{Bytes, BytesMut};
use num::ToPrimitive;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::{io, marker::PhantomData, path::PathBuf};
use tokio::task::JoinError;
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

use crate::io_types::{
    AppendLocation, AsyncAppend, AsyncBufRead, AsyncClose, AsyncRemove, AsyncTruncate,
    FallibleEntity, IntegerConversionError, ReadBytes, SizedEntity, UnreadError, UnwrittenError,
};

pub struct RandomRead;

pub struct Seek;

pub struct TokioFile<K, const FLUSH_ON_APPEND: bool = false> {
    inner: File,
    backing_file_path: PathBuf,

    size: u64,

    _phantom_data: PhantomData<K>,
}

impl<K, const FA: bool> TokioFile<K, FA> {
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

impl<K, const FA: bool> SizedEntity for TokioFile<K, FA> {
    type Position = u64;

    type Size = u64;

    fn size(&self) -> Self::Size {
        self.size
    }
}

pub enum TokioFileError {
    IoError(io::Error),
    JoinError(JoinError),

    IntoStdFileConversionFailed,

    IntegerConversionError,
}

impl From<IntegerConversionError> for TokioFileError {
    fn from(_: IntegerConversionError) -> Self {
        Self::IntegerConversionError
    }
}

impl<K, const FA: bool> FallibleEntity for TokioFile<K, FA> {
    type Error = TokioFileError;
}

impl<K, const FA: bool> AsyncTruncate for TokioFile<K, FA> {
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

impl<K, const FLUSH_ON_APPEND: bool> AsyncAppend for TokioFile<K, FLUSH_ON_APPEND> {
    async fn append(
        &mut self,
        bytes: Bytes,
    ) -> Result<AppendLocation<Self::Position, Self::Size>, UnwrittenError<Self::Error>> {
        let write_position = self.size();

        let write_len = self
            .inner
            .write(&bytes)
            .await
            .map_err(|err| UnwrittenError {
                unwritten: bytes.clone(),
                err: TokioFileError::IoError(err),
            })?
            .to_u64()
            .ok_or_else(|| UnwrittenError {
                unwritten: bytes.clone(),
                err: Self::Error::IntegerConversionError,
            })?;

        if FLUSH_ON_APPEND {
            self.inner.flush().await.map_err(|err| UnwrittenError {
                unwritten: bytes.clone(),
                err: TokioFileError::IoError(err),
            })?;
        }

        self.size += write_len;

        Ok(AppendLocation {
            write_position,
            write_len,
        })
    }
}

impl AsyncBufRead for TokioFile<Seek, true> {
    async fn read_at_buf(
        &mut self,
        position: Self::Position,
        mut buffer: BytesMut,
    ) -> Result<ReadBytes<BytesMut, Self::Size>, UnreadError<Self::Error>> {
        self.inner
            .seek(io::SeekFrom::Start(position))
            .await
            .map_err(|err| UnreadError {
                unread: buffer.clone(),
                err: Self::Error::IoError(err),
            })?;

        let read_len = self
            .inner
            .read(&mut buffer)
            .await
            .map_err(|err| UnreadError {
                unread: buffer.clone(),
                err: Self::Error::IoError(err),
            })?
            .to_u64()
            .ok_or_else(|| UnreadError {
                unread: buffer.clone(),
                err: Self::Error::IntegerConversionError,
            })?;

        self.inner
            .seek(io::SeekFrom::Start(self.size))
            .await
            .map_err(|err| UnreadError {
                unread: buffer.clone(),
                err: Self::Error::IoError(err),
            })?;

        Ok(ReadBytes {
            read_bytes: buffer,
            read_len,
        })
    }
}

#[cfg(target_family = "unix")]
impl AsyncBufRead for TokioFile<RandomRead, true> {
    async fn read_at_buf(
        &mut self,
        position: Self::Position,
        buffer: BytesMut,
    ) -> Result<ReadBytes<BytesMut, Self::Size>, UnreadError<Self::Error>> {
        let reader = self
            .inner
            .try_clone()
            .await
            .map_err(|err| UnreadError {
                unread: buffer.clone(),
                err: Self::Error::IoError(err),
            })?
            .try_into_std()
            .map_err(|_| UnreadError {
                unread: buffer.clone(),
                err: Self::Error::IntoStdFileConversionFailed,
            })?;

        let read_len_usize = buffer.len();

        let read_len = read_len_usize.to_u64().ok_or_else(|| UnreadError {
            unread: buffer.clone(),
            err: Self::Error::IntegerConversionError,
        })?;

        let mut read_buffer = buffer;

        let read_buffer = tokio::task::spawn_blocking(move || {
            let x = reader.read_exact_at(&mut read_buffer, position);

            match x {
                Ok(_) => Ok(read_buffer),
                Err(err) => Err((err, read_buffer)),
            }
        })
        .await;

        match read_buffer {
            Ok(Ok(read_buffer)) => Ok(ReadBytes {
                read_bytes: read_buffer,
                read_len,
            }),
            Ok(Err((err, unread))) => Err(UnreadError {
                unread,
                err: Self::Error::IoError(err),
            }),
            Err(err) => Err(UnreadError {
                unread: Bytes::from(vec![0u8; read_len_usize]).into(),
                err: Self::Error::JoinError(err),
            }),
        }
    }
}

impl<K, const FA: bool> AsyncRemove for TokioFile<K, FA> {
    async fn remove(self) -> Result<(), Self::Error> {
        tokio::fs::remove_file(self.backing_file_path)
            .await
            .map_err(Self::Error::IoError)
    }
}

impl<K, const FA: bool> AsyncClose for TokioFile<K, FA> {
    async fn close(mut self) -> Result<(), Self::Error> {
        self.inner.flush().await.map_err(Self::Error::IoError)?;

        drop(self.inner);

        Ok(())
    }
}
