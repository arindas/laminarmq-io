use crate::{stream, FallibleEntity, IntegerConversionError, SizedEntity, Stream, StreamRead};
use aws_sdk_s3::{
    operation::get_object::GetObjectOutput,
    primitives::{ByteStream, ByteStreamError},
    Client,
};
use bytes::Bytes;
use futures::prelude::Future;
use num::zero;
use std::cmp::{max, min, Ordering};
use std::error::Error;

pub const PART_SIZE_MAP_KEY_SUFFIX: &str = "_part_size_map.txt";

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
}

#[derive(Clone, Copy)]
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
}

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
}

#[allow(unused)]
pub struct AwsS3BackedFile<P> {
    client: Client,

    bucket: String,
    object_prefix: String,

    part_size_map: P,

    size: usize,
}

impl<P> SizedEntity for AwsS3BackedFile<P> {
    type Position = usize;

    type Size = usize;

    fn size(&self) -> Self::Size {
        self.size
    }
}

pub enum AwsS3Error {
    InvalidOp,

    ByteStreamError(ByteStreamError),

    IntegerConversionError,

    AwsSdkError(String),
}

impl From<IntegerConversionError> for AwsS3Error {
    fn from(_: IntegerConversionError) -> Self {
        Self::IntegerConversionError
    }
}

impl<P> FallibleEntity for AwsS3BackedFile<P> {
    type Error = AwsS3Error;
}

impl Stream for ByteStream {
    type Item<'a> = Result<Bytes, AwsS3Error>
    where
        Self: 'a;

    async fn next(&mut self) -> Option<Self::Item<'_>> {
        self.next()
            .await
            .map(|x| x.map_err(AwsS3Error::ByteStreamError))
    }
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

impl<F, E> Stream for GetObjectOutputFuture<F>
where
    F: Future<Output = Result<GetObjectOutput, E>>,
    E: Error,
{
    type Item<'a> = Result<Bytes, AwsS3Error>
    where
        Self: 'a;

    #[allow(clippy::manual_async_fn)]
    fn next(&mut self) -> impl Future<Output = Option<Self::Item<'_>>> + '_ {
        async {
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
}

impl<P> StreamRead for AwsS3BackedFile<P>
where
    P: PartMap,
{
    type ByteBuf<'a> = Bytes
    where
        Self: 'a;

    fn read_stream_at<'a, 'b>(
        &'a mut self,
        position: Self::Position,
        size: Self::Size,
    ) -> impl Stream<Item<'b> = Result<Self::ByteBuf<'b>, Self::Error>>
    where
        Self: 'a,
        'a: 'b,
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
                        .key(format!("{}_{}.txt", &self.object_prefix, part_idx))
                        .range(format!("bytes={}-{}", range_start, range_end))
                        .send(),
                )
            });

        stream::iter_chain(get_object_output_future_iter, || {
            Ok::<_, AwsS3Error>(Bytes::from_static(&[]))
        })
    }
}
