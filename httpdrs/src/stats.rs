use std::sync::{Arc, LazyLock, Mutex};

#[allow(dead_code)]
#[derive(Debug, Default)]
pub struct RuntimeContext{
    pub meta_path: String,
    pub data_path: String,
    pub temp_path: String,

    pub require_count: u64,
    pub require_bytes: u64,


    pub download_speed: u64,

    pub completed_count: u64,
    pub completed_bytes: u64,

    pub download_bytes: u64,
    pub download_count: u64,
}

pub(crate)  static RUNTIME: LazyLock<Arc<Mutex<RuntimeContext>>> =
    LazyLock::new(|| Arc::new(Mutex::new(RuntimeContext::default())));
