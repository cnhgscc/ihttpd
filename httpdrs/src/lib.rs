// Copyright 2025 BAAI, Inc.

//! # httpdrs
//!
//! httpdrs is a simple http client written in rust.

mod bandwidth;
mod download;
mod downloader;
mod merge;
pub mod prelude;
mod presgin;
mod reader;
pub mod runtime;
mod stats;
mod watch;

pub mod logger {
    pub use httpdrs_logger::*;
}

pub mod core {
    pub use httpdrs_core::*;
}
