use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Debug, Serialize, Deserialize)]
pub struct ReaderRequest<'a> {
    #[serde(borrow)]
    pub network: &'a str,
    pub download_sign: String,
}

impl ReaderRequest<'_> {
    pub fn new(network: &'_ str, download_sign: String) -> ReaderRequest<'_> {
        ReaderRequest {
            network,
            download_sign,
        }
    }
}

impl Display for ReaderRequest<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} -> {}", self.network, self.download_sign)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReaderData {
    pub endpoint: String,
}

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize)]
pub struct ReaderResponse {
    pub code: i32,
    pub message: String,
    pub data: ReaderData,
}
