// Copyright 2025 BAAI, Inc.

//! # httpdrs
//!
//! httpdrs is a simple http client written in rust.

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
