// Copyright 2025 BAAI, Inc.

//! # httpdrs
//!
//! httpdrs is a simple http client written in rust.

pub mod bandwidth;
mod download;
pub mod downloader;
pub mod merge;
pub mod prelude;
pub mod presign;
mod reader;
pub mod runtime;
pub mod stats;
pub mod stream;
pub mod watch;

pub mod logger {
    pub use httpdrs_logger::*;
}

pub mod core {
    pub use httpdrs_core::*;
}
