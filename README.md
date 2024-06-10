# laminarmq-io

[![rust-ci](https://github.com/arindas/laminarmq-io/workflows/rust-ci/badge.svg?branch=main)](https://github.com/arindas/laminarmq-io/actions/workflows/rust-ci.yml)

An attempt to rewrite [`laminarmq`](https://github.com/arindas/laminarmq) I/O layer
to acommodate capped/bounded memory overhead, both when reading and writing.

## Usage

`laminarmq-io` is a library crate. In order to use it, add the following to your `Cargo.toml`

```toml
[dependencies]
laminarmq-io = { git = "https://github.com/arindas/laminarmq-io.git" }
```

## Overview

This crate provides the following traits:

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

pub struct ReadBytes<T, S> {
    pub read_bytes: T,
    pub read_len: S,
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

On top of these traits, we aim to provide the following abstractions:

- [x] Streaming read / append
- [x] Buffered Reader
- [x] Buffered Appender
- [x] Direct Reader and Buffered Appender
- [x] Buffered Reader and Direct Appender
- [x] Buffered Reader and Buffered Appender

Importantly, this library acknowledges the fact that even simply reading may require
mutation (such as advancing the filepointer) on different platforms. Hence all
operations are exclusive. This library aims to remove the need for unnecessary internal
locks in our I/O abstractions.

## License

This repository is licensed under the same terms as [`laminarmq`](https://github.com/arindas/laminarmq)
