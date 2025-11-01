// Copyright 2025 BAAI, Inc.

//! # httpdrs
//!
//! httpdrs is a simple http client written in rust.

mod runtime;

pub use httpdrs_core::*;

pub mod logger {
    pub use httpdrs_logger::*;
}

pub use runtime::*;