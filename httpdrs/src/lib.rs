// Copyright 2025 BAAI, Inc.

//! # httpdrs
//!
//! httpdrs - A lightweight HTTP client for storage read/write operations, implemented in Rust.

pub mod bandwidth;
pub mod prelude;
pub mod read;
mod write;

pub mod logger {
    pub use httpdrs_logger::*;
}

pub mod core {
    pub use httpdrs_core::*;
}
