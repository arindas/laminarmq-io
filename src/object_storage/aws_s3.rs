use aws_sdk_s3::Client;
use std::cmp::Ordering;

pub const PART_SIZE_MAP_KEY_SUFFIX: &str = "_part_size_map.txt";

pub trait PartSizeMap {
    fn get_part_size(&self, part_idx: usize) -> Option<&usize>;

    fn append_part_size(&mut self, part_size: usize);

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn get_part(&self, offset: usize) -> Option<usize>;
}

pub struct PartBlock {
    pub offset: usize,
    pub size: usize,
}

impl PartBlock {
    pub fn end(&self) -> usize {
        self.offset + self.size
    }
}

impl PartSizeMap for Vec<PartBlock> {
    fn get_part_size(&self, part_idx: usize) -> Option<&usize> {
        self.get(part_idx).map(|x| &x.size)
    }

    fn append_part_size(&mut self, part_size: usize) {
        let offset = self.last().map_or(0, |x| x.end());

        self.push(PartBlock {
            offset,
            size: part_size,
        })
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn get_part(&self, offset: usize) -> Option<usize> {
        self.binary_search_by(|part| match part.offset.cmp(&offset) {
            Ordering::Less if offset < part.end() => Ordering::Equal,
            Ordering::Less => Ordering::Less,
            Ordering::Equal => Ordering::Equal,
            Ordering::Greater => Ordering::Greater,
        })
        .ok()
    }
}

pub struct FixedPartSizeMap {
    part_size: usize,
    len: usize,
}

impl PartSizeMap for FixedPartSizeMap {
    fn get_part_size(&self, _: usize) -> Option<&usize> {
        Some(&self.part_size)
    }

    fn append_part_size(&mut self, _: usize) {
        self.len += 1;
    }

    fn len(&self) -> usize {
        self.len
    }

    fn get_part(&self, offset: usize) -> Option<usize> {
        let part = offset / self.part_size;

        (part < self.len).then_some(part)
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
