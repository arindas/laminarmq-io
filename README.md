<p align="center">
<h1 align="center"><code>laminarmq-io</code></h1>
</p>

<p align="center">
  <a href="https://github.com/arindas/laminarmq-io/actions/workflows/rust-ci.yml">
    <img src="https://github.com/arindas/laminarmq-io/actions/workflows/rust-ci.yml/badge.svg">
  </a>
  <a href="https://github.com/arindas/laminarmq-io/actions/workflows/rustdoc.yml">
    <img src="https://github.com/arindas/laminarmq-io/actions/workflows/rustdoc.yml/badge.svg">
  </a>
</p>

<p align="center">
An attempt to rewrite <a href="https://github.com/arindas/laminarmq"><code>laminarmq</code></a> I/O layer
to accommodate bounded memory overhead when reading and writing.
</p>

## Usage

`laminarmq-io` is a library crate. In order to use it, add the following to your `Cargo.toml`

```toml
[dependencies]
laminarmq-io = { git = "https://github.com/arindas/laminarmq-io.git" }
```

## Overview

This crate provides the following I/O related traits:

| **Trait**         | **Type**                | **Wrapper Implemented On**                                                                                                  |
| ----------------- | ----------------------- | --------------------------------------------------------------------------------------------------------------------------- |
| [`AsyncRead`]     | `R I/O`                 | [`AsyncBufRead`] (<code>struct <i>BufferedReader\*</i></code>),<br> [`AsyncRead`] (struct [`DirectReaderBufferedAppender`]) |
| [`AsyncBufRead`]  | `R I/O`                 |                                                                                                                             |
| [`StreamRead`]    | _Streaming_ <br>`R I/O` | [`AsyncRead`] (struct [`AsyncReadStreamer`],<br> <code>struct <i>BufferedStreamReader\*</i></code>)                         |
| [`AsyncAppend`]   | `W I/O`                 | [`AsyncAppend`] (<code>struct <i>\*BufferedAppender</i></code>)                                                             |
| [`StreamAppend`]  | _Streaming_ <br>`W I/O` | [`AsyncAppend`] (_trait impl_)                                                                                              |
| [`AsyncTruncate`] | `W I/O`                 |                                                                                                                             |
| [`AsyncRemove`]   | Management              |                                                                                                                             |
| [`AsyncClose`]    | Management              | <img width="1000" id="full-width-cell" />                                                                                   |

> The "Wrapper Implemented on" column denotes on which underlying trait, the current trait has an impl with the help of a wrapper struct wrapping the mentioned trait.
> For example, [`StreamRead`] is implemented by a wrapper struct [`AsyncReadStreamer`] which wraps an [`AsyncRead`] instance.
>
> Some traits in this table also have direct impls on other trait types e.g:
>
> ```text
> impl<T> StreamAppend for T where T: AsyncAppend { /* ... */ }
> ```
>
> They are marked with (_trait impl_).

<br>

This library makes the following improvments over existing I/O primitives in `laminarmq`:

- Provides traits at individual operaton level i.e `Read` / `Append` level as opposed to a unified `Storage` trait
- All operations are exclusive with a `&mut self` receiver to avoid internal locks
- Provides both streaming read and streaming write
- Provides impls on both filessytem based APIs and cloud object storage APIs such as S3

## License

This repository is licensed under the same terms as [`laminarmq`](https://github.com/arindas/laminarmq).
See [LICENSE](https://raw.githubusercontent.com/arindas/laminarmq-io/main/LICENSE) for more details.
