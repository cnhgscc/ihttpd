mod client;
mod reader;


pub mod httpd {
    pub use crate::client::*;
    pub use crate::reader::*;
    pub use httpdrs_sign::*;
}


pub mod io {
    pub use crate::reader::*;
}
