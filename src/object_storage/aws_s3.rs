use crate::io_types::{
    AppendInfo, AsyncAppend, AsyncClose, AsyncRemove, AsyncTruncate, ByteLender,
    FallibleByteLender, FallibleEntity, IntegerConversionError, SizedEntity, StreamRead,
    UnwrittenError,
};
use crate::stream::{self, Stream};
use crate::AppendLocation;
use aws_sdk_s3::{
    operation::get_object::GetObjectOutput,
    primitives::{ByteStream, ByteStreamError},
    Client,
};
use bytes::Bytes;
use futures::{prelude::Future, FutureExt};
use num::zero;
use serde::{Deserialize, Serialize};
use std::{
    cmp::{max, min},
    iter,
};

use crate::object_storage::BlockMap;

pub const BLOCK_SIZE_MAP_KEY_SUFFIX: &str = "_block_size_map.json";

pub const BLOCK_EXTENSION: &str = "txt";

async fn block_map_append_missing_blocks_from_aws_s3<BM: BlockMap>(
    block_map: &mut BM,
    aws_s3_client: &Client,
    bucket: &String,
    object_prefix: &String,
) -> Result<usize, AwsS3Error> {
    let mut blocks_added = 0;

    let block_suffix = format!(".{}", BLOCK_EXTENSION);

    for object in aws_s3_client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(object_prefix)
        .send()
        .await
        .map_err(|err| AwsS3Error::AwsSdkError(err.to_string()))?
        .contents()
    {
        let key = object.key().unwrap_or("");

        let object_size = object.size().unwrap_or(0);

        if !key.ends_with(&block_suffix) {
            continue;
        }

        let start = key.find('_').ok_or(AwsS3Error::ParseError(
            "Failed to find _ delim in block object key".to_string(),
        ))? + 1;
        let end = key.find('.').ok_or(AwsS3Error::ParseError(
            "Failed to find . delim in block object key".to_string(),
        ))?;

        if start >= end || end > key.len() {
            return Err(AwsS3Error::ParseError(
                "Invalid block_idx parse slice[] bounds".to_string(),
            ));
        }

        let block_idx: &usize = &key[start..end]
            .parse()
            .map_err(|_| AwsS3Error::ParseError("Failed parsing block idx".to_string()))?;

        if *block_idx < block_map.len() {
            continue;
        }

        block_map.append_block_with_block_size(
            object_size
                .try_into()
                .map_err(|_| AwsS3Error::IntegerConversionError)?,
        );

        blocks_added += 1;
    }

    Ok(blocks_added)
}

#[allow(unused)]
pub struct AwsS3BackedFile<BM> {
    client: Client,

    bucket: String,
    object_prefix: String,

    block_size_map: BM,
}

impl<BM> SizedEntity for AwsS3BackedFile<BM>
where
    BM: BlockMap,
{
    type Position = usize;

    type Size = usize;

    fn size(&self) -> Self::Size {
        self.block_size_map.size()
    }
}

pub enum AwsS3Error {
    ByteStreamError(ByteStreamError),

    IntegerConversionError,

    SerializationError(serde_json::Error),

    AwsSdkError(String),

    ParseError(String),

    PositionOutOfBounds,
}

impl<BM> AwsS3BackedFile<BM>
where
    BM: for<'a> Deserialize<'a> + BlockMap,
{
    pub async fn new(
        aws_s3_client: Client,
        bucket: String,
        object_prefix: String,
        mut fallback_block_map: BM,
    ) -> Result<Self, AwsS3Error> {
        let get_object_output = aws_s3_client
            .get_object()
            .bucket(&bucket)
            .key(format!("{}{}", &object_prefix, BLOCK_SIZE_MAP_KEY_SUFFIX))
            .send()
            .await
            .map_err(|err| AwsS3Error::AwsSdkError(err.to_string()))?;

        let bytes = get_object_output
            .body
            .collect()
            .await
            .map_err(|err| AwsS3Error::AwsSdkError(err.to_string()))?
            .into_bytes();

        let mut block_size_map = if let Ok(block_map) = serde_json::from_slice(&bytes) {
            block_map
        } else {
            fallback_block_map.clear();
            fallback_block_map
        };

        block_map_append_missing_blocks_from_aws_s3(
            &mut block_size_map,
            &aws_s3_client,
            &bucket,
            &object_prefix,
        )
        .await?;

        Ok(Self {
            client: aws_s3_client,
            bucket,
            object_prefix,
            block_size_map,
        })
    }
}

impl From<IntegerConversionError> for AwsS3Error {
    fn from(_: IntegerConversionError) -> Self {
        Self::IntegerConversionError
    }
}

impl<BM> FallibleEntity for AwsS3BackedFile<BM> {
    type Error = AwsS3Error;
}

pub struct GetObjectOutputFuture<F> {
    fut: Option<F>,
    byte_stream: ByteStream,
}

impl<F> Default for GetObjectOutputFuture<F> {
    fn default() -> Self {
        Self {
            fut: None,
            byte_stream: ByteStream::from_static(&[]),
        }
    }
}

impl<F> GetObjectOutputFuture<F> {
    pub fn new(fut: F) -> Self {
        Self {
            fut: Some(fut),
            byte_stream: ByteStream::from_static(&[]),
        }
    }
}

impl<F> Stream<FallibleByteLender<AwsS3ByteLender, AwsS3Error>> for GetObjectOutputFuture<F>
where
    F: Future<Output = Result<GetObjectOutput, String>>,
{
    async fn next<'a>(
        &'a mut self,
    ) -> Option<<FallibleByteLender<AwsS3ByteLender, AwsS3Error> as stream::Lender>::Item<'a>>
    where
        FallibleByteLender<AwsS3ByteLender, AwsS3Error>: 'a,
    {
        match match self.fut.take() {
            Some(f) => f.await.map(|x| Some(x.body)),
            None => Ok(None),
        } {
            Err(err) => return Some(Err(AwsS3Error::AwsSdkError(err.to_string()))),
            Ok(Some(stream)) => {
                self.byte_stream = stream;
            }
            _ => {}
        }

        self.byte_stream
            .next()
            .await
            .map(|x| x.map_err(AwsS3Error::ByteStreamError))
    }
}

pub struct AwsS3ByteLender;

impl ByteLender for AwsS3ByteLender {
    type ByteBuf<'a> = Bytes
    where
        Self: 'a;
}

impl<BM> StreamRead<AwsS3ByteLender> for AwsS3BackedFile<BM>
where
    BM: BlockMap,
{
    fn read_stream_at<'a>(
        &'a mut self,
        position: Self::Position,
        size: Self::Size,
    ) -> impl Stream<FallibleByteLender<AwsS3ByteLender, Self::Error>> + 'a
    where
        AwsS3ByteLender: 'a,
    {
        let first_block_idx = self
            .block_size_map
            .position_block_containing_offset(position)
            .unwrap_or(self.block_size_map.len());

        let get_object_output_future_iter = (first_block_idx..self.block_size_map.len())
            .scan(
                (position, size),
                |(read_position, bytes_left_to_read), idx| {
                    let block = self.block_size_map.get_block_at_idx(idx)?;

                    if *bytes_left_to_read <= zero() || *read_position >= block.end() {
                        return None;
                    }

                    let range_start = max(*read_position, block.offset);

                    let range_end = min(range_start + *bytes_left_to_read, block.end());

                    *bytes_left_to_read -= range_end - range_start;

                    // normalize range to [0, block.size)
                    let range_start = range_start - block.offset;
                    let range_end = range_end - block.offset;

                    Some((idx, range_start, range_end - 1))
                },
            )
            .map(|(block_idx, range_start, range_end)| {
                GetObjectOutputFuture::new(
                    self.client
                        .get_object()
                        .bucket(&self.bucket)
                        .key(format!(
                            "{}_{}.{}",
                            &self.object_prefix, block_idx, BLOCK_EXTENSION
                        ))
                        .range(format!("bytes={}-{}", range_start, range_end))
                        .send()
                        .map(|x| x.map_err(|err| err.to_string())),
                )
            });

        stream::iter_chain(get_object_output_future_iter, |_: &()| {
            Ok(Bytes::from_static(&[]))
        })
    }
}

impl<BM> AsyncAppend for AwsS3BackedFile<BM>
where
    BM: BlockMap,
{
    async fn append(
        &mut self,
        bytes: Bytes,
    ) -> Result<AppendInfo<Self::Position, Self::Size>, UnwrittenError<Self::Error>> {
        let block = self
            .block_size_map
            .append_block_with_block_size(bytes.len());

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(format!(
                "{}_{}.{}",
                &self.object_prefix,
                self.block_size_map.len() - 1,
                BLOCK_EXTENSION
            ))
            .body(bytes.clone().into())
            .send()
            .await
            .map_err(|err| UnwrittenError {
                err: AwsS3Error::AwsSdkError(err.to_string()),
                unwritten: bytes.clone(),
            })?;

        Ok(AppendInfo {
            bytes,
            location: AppendLocation {
                write_position: block.offset,
                write_len: block.size,
            },
        })
    }
}

impl<BM> AsyncTruncate for AwsS3BackedFile<BM>
where
    BM: BlockMap + Serialize,
{
    async fn truncate(&mut self, position: Self::Position) -> Result<(), Self::Error> {
        let old_block_size_map_len = self.block_size_map.len();

        let (last_block_idx, old_last_block_size, last_block_after_truncate) = self
            .block_size_map
            .truncate(position)
            .ok_or(AwsS3Error::PositionOutOfBounds)?;

        if old_last_block_size > last_block_after_truncate.size {
            let get_object_output = self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(format!(
                    "{}_{}.{}",
                    &self.object_prefix, last_block_idx, BLOCK_EXTENSION
                ))
                .range(format!(
                    "bytes={}-{}",
                    0,
                    last_block_after_truncate.size - 1
                ))
                .send()
                .await
                .map_err(|err| AwsS3Error::AwsSdkError(err.to_string()))?;

            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(format!(
                    "{}_{}.{}",
                    &self.object_prefix, last_block_idx, BLOCK_EXTENSION
                ))
                .body(get_object_output.body)
                .send()
                .await
                .map_err(|err| AwsS3Error::AwsSdkError(err.to_string()))?;
        }

        for block_idx in last_block_idx + 1..old_block_size_map_len {
            self.client
                .delete_object()
                .bucket(&self.bucket)
                .key(format!(
                    "{}_{}.{}",
                    &self.object_prefix, block_idx, BLOCK_EXTENSION
                ))
                .send()
                .await
                .map_err(|err| AwsS3Error::AwsSdkError(err.to_string()))?;
        }

        Ok(())
    }
}

impl<BM> AsyncRemove for AwsS3BackedFile<BM>
where
    BM: BlockMap,
{
    async fn remove(self) -> Result<(), Self::Error> {
        let keys =
            iter::once(format!(
                "{}{}",
                &self.object_prefix, BLOCK_SIZE_MAP_KEY_SUFFIX
            ))
            .chain((0..self.block_size_map.len()).map(|block_idx| {
                format!("{}_{}.{}", &self.object_prefix, block_idx, BLOCK_EXTENSION)
            }));

        for key in keys {
            self.client
                .delete_object()
                .bucket(&self.bucket)
                .key(key)
                .send()
                .await
                .map_err(|err| AwsS3Error::AwsSdkError(err.to_string()))?;
        }

        Ok(())
    }
}

impl<BM> AwsS3BackedFile<BM>
where
    BM: Serialize,
{
    async fn flush(&self) -> Result<(), AwsS3Error> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(format!(
                "{}{}",
                &self.object_prefix, BLOCK_SIZE_MAP_KEY_SUFFIX
            ))
            .body(
                serde_json::to_vec(&self.block_size_map)
                    .map_err(AwsS3Error::SerializationError)?
                    .into(),
            )
            .send()
            .await
            .map_err(|put_error| put_error.to_string())
            .map_err(AwsS3Error::AwsSdkError)
            .map(|_| ())
    }
}

impl<BM> AsyncClose for AwsS3BackedFile<BM>
where
    BM: Serialize,
{
    async fn close(self) -> Result<(), Self::Error> {
        self.flush().await?;

        Ok(())
    }
}
