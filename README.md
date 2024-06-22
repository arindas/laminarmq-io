# laminarmq-io

[![rust-ci](https://github.com/arindas/laminarmq-io/workflows/rust-ci/badge.svg)](https://github.com/arindas/laminarmq-io/actions/workflows/rust-ci.yml)
[![rustdoc](https://github.com/arindas/laminarmq-io/workflows/rustdoc/badge.svg)](https://github.com/arindas/laminarmq-io/actions/workflows/rustdoc.yml)

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

- `AsyncRead`
- `AsyncBufRead`
- `StreamRead`
- `AsyncAppend`
- `AsyncTruncate`
- `StreamAppend`
- `AsyncRemove`
- `AsyncClose`

On top of these traits, we aim to provide the following abstractions:

- [x] Streaming read / append
- [x] Buffered Reader
- [x] Buffered Appender
- [x] Direct Reader and Buffered Appender
- [x] Buffered Reader and Direct Appender
- [x] Buffered Reader and Buffered Appender
- [x] Streaming Reader and Buffered Appender

This library acknowledges the fact that even simply reading may require
mutation (such as advancing the filepointer) on different platforms. Hence
all operations are exclusive. This library aims to remove the need for
unnecessary internal locks in our I/O abstractions.

This generality allows us to abstract over local file system based storage
as well as object storage (like AWS S3) with the same API.

## License

This repository is licensed under the same terms as [`laminarmq`](https://github.com/arindas/laminarmq).
See [LICENSE](https://raw.githubusercontent.com/arindas/laminarmq-io/main/LICENSE) for more details.
