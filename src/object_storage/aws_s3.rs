use crate::{
    stream, AppendLocation, AsyncAppend, AsyncClose, AsyncRemove, AsyncTruncate, ByteLender,
    FallibleByteLender, FallibleEntity, IntegerConversionError, SizedEntity, Stream, StreamRead,
};
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
    cmp::{max, min, Ordering},
    iter,
};

pub const PART_SIZE_MAP_KEY_SUFFIX: &str = "_part_size_map.json";

pub const PART_EXTENSION: &str = "txt";

pub trait PartMap {
    fn position_part_containing_offset(&self, offset: usize) -> Option<usize>;

    fn get_part_at_idx(&self, part_idx: usize) -> Option<Part>;

    fn get_part_containing_offset(&self, offset: usize) -> Option<Part> {
        self.position_part_containing_offset(offset)
            .and_then(|idx| self.get_part_at_idx(idx))
    }

    fn append_part_with_part_size(&mut self, part_size: usize) -> Part;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn truncate(&mut self, offset: usize) -> Option<(usize, usize, Part)>;

    fn size(&self) -> usize {
        self.get_part_at_idx(self.len() - 1)
            .map(|p| p.end())
            .unwrap_or(0)
    }

    fn clear(&mut self);

    fn append_missing_parts_from(
        &mut self,
        aws_s3_client: &Client,
        bucket: &String,
        object_prefix: &String,
    ) -> impl std::future::Future<Output = Result<usize, AwsS3Error>> {
        async move {
            let mut parts_added = 0;

            let part_suffix = format!(".{}", PART_EXTENSION);

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

                if !key.ends_with(&part_suffix) {
                    continue;
                }

                let start = key.find('_').ok_or(AwsS3Error::ParseError(
                    "Failed to find _ delim in part object key".to_string(),
                ))? + 1;
                let end = key.find('.').ok_or(AwsS3Error::ParseError(
                    "Failed to find . delim in part object key".to_string(),
                ))?;

                if start >= end || end > key.len() {
                    return Err(AwsS3Error::ParseError(
                        "Invalid part_idx parse slice[] bounds".to_string(),
                    ));
                }

                let part_idx: &usize = &key[start..end]
                    .parse()
                    .map_err(|_| AwsS3Error::ParseError("Failed parsing part idx".to_string()))?;

                if *part_idx < self.len() {
                    continue;
                }

                self.append_part_with_part_size(
                    object_size
                        .try_into()
                        .map_err(|_| AwsS3Error::IntegerConversionError)?,
                );

                parts_added += 1;
            }

            Ok(parts_added)
        }
    }
}

#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct Part {
    pub offset: usize,
    pub size: usize,
}

impl Part {
    pub fn end(&self) -> usize {
        self.offset + self.size
    }
}

impl PartMap for Vec<Part> {
    fn position_part_containing_offset(&self, offset: usize) -> Option<usize> {
        self.binary_search_by(|part| match part.offset.cmp(&offset) {
            Ordering::Less if offset < part.end() => Ordering::Equal,
            Ordering::Less => Ordering::Less,
            Ordering::Equal => Ordering::Equal,
            Ordering::Greater => Ordering::Greater,
        })
        .ok()
    }

    fn get_part_at_idx(&self, part_idx: usize) -> Option<Part> {
        self.get(part_idx).cloned()
    }

    fn append_part_with_part_size(&mut self, part_size: usize) -> Part {
        let offset = self.last().map_or(0, |x| x.end());

        let part = Part {
            offset,
            size: part_size,
        };

        self.push(part);

        part
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn truncate(&mut self, offset: usize) -> Option<(usize, usize, Part)> {
        let idx = self.position_part_containing_offset(offset)?;

        let mut truncated_part = self.get_part_at_idx(idx)?;

        let _ = self.split_off(idx);

        let old_part_size = truncated_part.size;

        truncated_part.size -= truncated_part.end() - offset;

        self.push(truncated_part);

        Some((idx, old_part_size, truncated_part))
    }

    fn clear(&mut self) {
        self.clear()
    }
}

#[derive(Clone, Copy, Serialize, Deserialize)]
pub struct FixedPartSizeMap {
    part_size: usize,
    len: usize,
}

impl PartMap for FixedPartSizeMap {
    fn position_part_containing_offset(&self, offset: usize) -> Option<usize> {
        let part_idx = offset / self.part_size;

        (part_idx < self.len).then_some(part_idx)
    }

    fn get_part_at_idx(&self, part_idx: usize) -> Option<Part> {
        (part_idx < self.len).then_some(Part {
            offset: part_idx * self.part_size,
            size: self.part_size,
        })
    }

    fn get_part_containing_offset(&self, offset: usize) -> Option<Part> {
        let part_idx = offset / self.part_size;

        (part_idx < self.len).then_some(Part {
            offset: part_idx * self.part_size,
            size: self.part_size,
        })
    }

    fn append_part_with_part_size(&mut self, _: usize) -> Part {
        let part = Part {
            offset: self.len * self.part_size,
            size: self.part_size,
        };

        self.len += 1;

        part
    }

    fn len(&self) -> usize {
        self.len
    }

    fn truncate(&mut self, offset: usize) -> Option<(usize, usize, Part)> {
        let idx = self.position_part_containing_offset(offset)?;

        self.len = idx;

        self.get_part_at_idx(self.len() - 1)
            .map(|x| (self.len() - 1, self.part_size, x))
    }

    fn clear(&mut self) {
        self.len = 0
    }
}

#[allow(unused)]
pub struct AwsS3BackedFile<P> {
    client: Client,

    bucket: String,
    object_prefix: String,

    part_size_map: P,
}

impl<P> SizedEntity for AwsS3BackedFile<P>
where
    P: PartMap,
{
    type Position = usize;

    type Size = usize;

    fn size(&self) -> Self::Size {
        self.part_size_map.size()
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

impl<P> AwsS3BackedFile<P>
where
    P: for<'a> Deserialize<'a> + PartMap,
{
    pub async fn new(
        aws_s3_client: Client,
        bucket: String,
        object_prefix: String,
        mut fallback_part_map: P,
    ) -> Result<Self, AwsS3Error> {
        let get_object_output = aws_s3_client
            .get_object()
            .bucket(&bucket)
            .key(format!(
                "{}_{}.txt",
                &object_prefix, PART_SIZE_MAP_KEY_SUFFIX
            ))
            .send()
            .await
            .map_err(|err| AwsS3Error::AwsSdkError(err.to_string()))?;

        let bytes = get_object_output
            .body
            .collect()
            .await
            .map_err(|err| AwsS3Error::AwsSdkError(err.to_string()))?
            .into_bytes();

        let mut part_size_map = if let Ok(part_map) = serde_json::from_slice(&bytes) {
            part_map
        } else {
            fallback_part_map.clear();
            fallback_part_map
        };

        part_size_map
            .append_missing_parts_from(&aws_s3_client, &bucket, &object_prefix)
            .await?;

        Ok(Self {
            client: aws_s3_client,
            bucket,
            object_prefix,
            part_size_map,
        })
    }
}

impl From<IntegerConversionError> for AwsS3Error {
    fn from(_: IntegerConversionError) -> Self {
        Self::IntegerConversionError
    }
}

impl<P> FallibleEntity for AwsS3BackedFile<P> {
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

impl<P> StreamRead<AwsS3ByteLender> for AwsS3BackedFile<P>
where
    P: PartMap,
{
    fn read_stream_at<'a>(
        &'a mut self,
        position: Self::Position,
        size: Self::Size,
    ) -> impl Stream<FallibleByteLender<AwsS3ByteLender, Self::Error>> + 'a
    where
        AwsS3ByteLender: 'a,
    {
        let first_part_idx = self
            .part_size_map
            .position_part_containing_offset(position)
            .unwrap_or(self.part_size_map.len());

        let get_object_output_future_iter = (first_part_idx..self.part_size_map.len())
            .scan(
                (position, size),
                |(read_position, bytes_left_to_read), idx| {
                    if *bytes_left_to_read <= zero() {
                        return None;
                    }

                    let part = self.part_size_map.get_part_at_idx(idx)?;

                    let range_start = max(*read_position, part.offset);

                    let range_end = min(range_start + *bytes_left_to_read, part.end());

                    *bytes_left_to_read -= range_end - range_start;

                    Some((idx, range_start, range_end - 1))
                },
            )
            .map(|(part_idx, range_start, range_end)| {
                GetObjectOutputFuture::new(
                    self.client
                        .get_object()
                        .bucket(&self.bucket)
                        .key(format!(
                            "{}_{}.{}",
                            &self.object_prefix, part_idx, PART_EXTENSION
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

impl<P> AsyncAppend for AwsS3BackedFile<P>
where
    P: PartMap + Serialize,
{
    async fn append(
        &mut self,
        bytes: &[u8],
    ) -> Result<AppendLocation<Self::Position, Self::Size>, Self::Error> {
        let part = self.part_size_map.append_part_with_part_size(bytes.len());

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(format!(
                "{}_{}.txt",
                &self.object_prefix,
                self.part_size_map.len() - 1
            ))
            .body(bytes.to_vec().into())
            .send()
            .await
            .map_err(|err| AwsS3Error::AwsSdkError(err.to_string()))?;

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(format!(
                "{}_{}",
                &self.object_prefix, PART_SIZE_MAP_KEY_SUFFIX
            ))
            .body(
                serde_json::to_vec(&part)
                    .map_err(AwsS3Error::SerializationError)?
                    .into(),
            )
            .send()
            .await
            .map_err(|err| AwsS3Error::AwsSdkError(err.to_string()))?;

        Ok(AppendLocation {
            write_position: part.offset,
            write_len: part.size,
        })
    }
}

impl<P> AsyncTruncate for AwsS3BackedFile<P>
where
    P: PartMap + Serialize,
{
    async fn truncate(&mut self, position: Self::Position) -> Result<(), Self::Error> {
        let old_part_size_map_len = self.part_size_map.len();

        let (last_part_idx, old_last_part_size, last_part_after_truncate) = self
            .part_size_map
            .truncate(position)
            .ok_or(AwsS3Error::PositionOutOfBounds)?;

        if old_last_part_size > last_part_after_truncate.size {
            let get_object_output = self
                .client
                .get_object()
                .bucket(&self.bucket)
                .key(format!("{}_{}.txt", &self.object_prefix, last_part_idx))
                .range(format!("bytes={}-{}", 0, last_part_after_truncate.size - 1))
                .send()
                .await
                .map_err(|err| AwsS3Error::AwsSdkError(err.to_string()))?;

            self.client
                .put_object()
                .bucket(&self.bucket)
                .key(format!("{}_{}.txt", &self.object_prefix, last_part_idx))
                .body(get_object_output.body)
                .send()
                .await
                .map_err(|err| AwsS3Error::AwsSdkError(err.to_string()))?;
        }

        for part_idx in last_part_idx + 1..old_part_size_map_len {
            self.client
                .delete_object()
                .bucket(&self.bucket)
                .key(format!("{}_{}.txt", &self.object_prefix, part_idx))
                .send()
                .await
                .map_err(|err| AwsS3Error::AwsSdkError(err.to_string()))?;
        }

        Ok(())
    }
}

impl<P> AsyncRemove for AwsS3BackedFile<P> {
    async fn remove(self) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl<P> AsyncClose for AwsS3BackedFile<P>
where
    P: PartMap,
{
    async fn close(self) -> Result<(), Self::Error> {
        let keys = iter::once(format!(
            "{}_{}",
            &self.object_prefix, PART_SIZE_MAP_KEY_SUFFIX
        ))
        .chain(
            (0..self.part_size_map.len())
                .map(|part_idx| format!("{}_{}.txt", &self.object_prefix, part_idx)),
        );

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
