// Copyright 2025 BAAI, Inc.

//! # httpdrs
//!
//! httpdrs is a simple http client written in rust.

pub mod runtime;
pub mod prelude;
mod bandwidth;
mod stats;
mod reader;

pub mod logger {
    pub use httpdrs_logger::*;
}

pub mod core {
    pub use httpdrs_core::*;
}