mod client;
mod reader;
pub mod request;

pub mod httpd {
    pub use crate::client::*;
    pub use crate::reader::*;
    pub use httpdrs_bandwidth::*;
    pub use httpdrs_sign::jwtsign::*;
    pub use httpdrs_sign::*;
}

/// 本地文件相关
pub mod io {
    pub use crate::reader::*;
}

/// 远程文件相关
pub use request::*;

pub mod pbar {
    pub use httpdrs_pbar::*;
}
