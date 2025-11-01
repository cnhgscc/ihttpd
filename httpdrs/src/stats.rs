use std::sync::{Arc, LazyLock, Mutex};

#[allow(dead_code)]
#[derive(Debug, Default)]
pub struct RuntimeContext{
    pub request_count: i64,
    pub request_bytes: i64,


    pub download_speed: u64
}

pub(crate)  static RUNTIME: LazyLock<Arc<Mutex<RuntimeContext>>> =
    LazyLock::new(|| Arc::new(Mutex::new(RuntimeContext::default())));
