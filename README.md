# laminarmq-storage

An attempt to rewrite [`laminarmq`](https://github.com/arindas/laminarmq) storage layer
to acommodate capped/bounded memory overhead, both when reading and writing.

## Usage

`laminarmq-storage` is a library crate. In order to use it, add the following to your `Cargo.toml`

```toml
[dependencies]
laminarmq-storage = { git = "https://github.com/arindas/laminarmq-storage.git" }
```

## Overview

This create aims to provide the following traits:

```rust

pub trait SizedEntity {
    type Position: Quantifier + From<Self::Size>;
    type Size: Quantifier + From<Self::Position>;

    fn size(&self) -> Self::Size;
}

pub struct AppendLocation<P, S> {
    pub write_position: P,
    pub write_len: S,
}

pub trait AsyncTruncate: SizedEntity {
    type TruncateError;

    fn truncate(
        &mut self,
        position: Self::Position,
    ) -> impl Future<Output = Result<(), Self::TruncateError>>;
}

pub trait AsyncAppend: SizedEntity {
    type AppendError;

    fn append(
        &mut self,
        bytes: &[u8],
    ) -> impl Future<Output = Result<AppendLocation<Self::Position, Self::Size>, Self::AppendError>>;
}

pub trait AsyncRead: SizedEntity {
    type ByteBuf<'a>: Deref<Target = [u8]> + 'a
    where
        Self: 'a;

    type ReadError;

    fn read_at(
        &mut self,
        position: Self::Position,
        size: Self::Size,
    ) -> impl Future<Output = Result<ReadBytes<Self::ByteBuf<'_>, Self::Size>, Self::ReadError>>;
}

pub struct ReadBytesLen<T>(T);

pub trait AsyncBufRead: SizedEntity {
    type BufReadError;

    fn read_at_buf(
        &mut self,
        position: Self::Position,
        size: Self::Size,
        buffer: &mut [u8],
    ) -> impl Future<Output = Result<ReadBytesLen<Self::Size>, Self::BufReadError>>;
}

```

On top of these traits, we need the following abstractions:

- Streaming read / write
- Direct Reader and Direct Writer
- Direct Reader and Buffered Writer
- Buffered Reader and Direct Writer
- Buffered Reader and Buffered Writer

Importantly, this library acknowledges the fact that even simply reading may require
mutation (such as advancing the filepointer) on different platforms. Hence all
operations are exclusive. This library aims to remove the need for unnecessary internal
locks in our I/O abstractions.

## License

This repository is licensed under the same terms as [`laminarmq`](https://github.com/arindas/laminarmq)
