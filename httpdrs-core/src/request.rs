use std::sync::Arc;

#[derive(Debug)]
pub struct FSReader {
    pub request_sign: String,
    pub require_size: u64,
    pub chunk_size: u64,
}

impl FSReader {
    pub fn new(sign: String, size: u64) -> Arc<Self> {
        Arc::new(FSReader {
            request_sign: sign,
            require_size: size,
            chunk_size: 1024 * 1024 * 5,
        })
    }

    pub fn total_parts(&self) -> u64 {
        self.require_size.div_ceil(self.chunk_size)
    }
}
