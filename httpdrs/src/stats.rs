use indicatif::HumanBytes;
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::{Arc, LazyLock, Mutex};

#[allow(dead_code)]
#[derive(Debug, Default)]
pub struct RuntimeContext {
    pub meta_path: String,
    pub data_path: String,
    pub temp_path: String,

    // TODO: check
    pub flag: u64, // 运行标志
    pub download_speed: u64,
    pub parallel_sumit: usize,

    // require
    pub require_count: u64, // 需要下载的文件数量
    pub require_bytes: u64, // 需要下载的文件大小

    // checkpoint
    pub download_bytes: u64, // 断点续传时, 已经完成的文件数量
    pub download_count: u64, // 断点续传时, 已经完成的文件大小

    // 下载成功
    pub completed_count: u64, // 已经完成的文件数量
    pub completed_bytes: u64, // 已经完成的文件大小

    // 下载失败
    pub uncompleted_count: u64, // 未完成下载的文件数量
    pub uncompleted_bytes: u64, // 未完成下载的文件大小
}

impl Display for RuntimeContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let completed_bytes_human = HumanBytes(self.completed_bytes + self.download_bytes);
        let uncompleted_bytes_human = HumanBytes(self.uncompleted_bytes);
        let download_bytes_human = HumanBytes(self.download_bytes);

        write!(
            f,
            "Success: {}/{}, Fail: {}/{}, Skip: {}/{}",
            self.completed_count + self.download_count,
            completed_bytes_human,
            self.uncompleted_count,
            uncompleted_bytes_human,
            self.download_count,
            download_bytes_human
        )
    }
}

pub(crate) static RUNTIME: LazyLock<Arc<Mutex<RuntimeContext>>> =
    LazyLock::new(|| Arc::new(Mutex::new(RuntimeContext::default())));

/// META 管理所有meta文件新
pub(crate) static META: LazyLock<Arc<Mutex<HashMap<String, u64>>>> =
    LazyLock::new(|| Arc::new(Mutex::new(HashMap::new())));
