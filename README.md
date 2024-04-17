# laminarmq-storage

An attempt to rewrite [`laminarmq`](https://github.com/arindas/laminarmq) storage layer
to acommodate capped/bounded memory overhead, both when reading and writing.

## Overview

We want the following types of abstractions:

- A type that allows appending a slice of bytes
- A type that allows reading a specific number of bytes from a given position
- A type that allows reading enough bytes to fill a user provided buffer from a given position ()
- A type that allows adapting a type that requires buffer to the one that doesn't need it
- A type that wraps a writer and a reader that doesn't require user provided buffers. This type
  maintains it own read and write buffers. Reads to the underlying rader never exceeds a specific
  limit. Both reads and writes are effectively buffered. We make optimaal use of both the read
  and write buffers to service reads. Don't allow allocations
- A type that wraps a writer and a reader that requires user provided buffers. This type
  maintains it own read and write buffers. Reads to the underlying rader are made using this types
  read bufer. Both reads and writes are effectively buffered. We make optimaal use of both the
  read and write buffers to service reads. Don't allow allocations
- Also have buffered types that only read
- Find a way to specify a maximum memory limit in total across a set of these types
