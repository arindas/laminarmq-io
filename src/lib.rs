#![doc = include_str!("../README.md")]

pub mod anchored_buffer;
pub mod buffered_io;
pub mod fs;
pub mod io_types;

#[cfg(feature = "legacy")]
pub mod legacy;

pub mod object_storage;
pub mod stream;

pub mod prelude {
    pub use crate::buffered_io::*;
    pub use crate::io_types::*;
    pub use crate::stream::*;
}

pub use prelude::*;
