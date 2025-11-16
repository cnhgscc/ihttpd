use std::collections::HashMap;
use std::fmt::Display;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, LazyLock, Mutex, OnceLock};

use indicatif::HumanBytes;
use tokio::sync::RwLock;

pub(crate) static RUNTIME: OnceLock<RuntimeContext> = OnceLock::new();

pub(crate) fn init_runtime(meta_path: String, data_path: String, temp_path: String) {
    RUNTIME.get_or_init(|| RuntimeContext {
        meta_path: RwLock::new(meta_path),
        data_path: RwLock::new(data_path),
        temp_path: RwLock::new(temp_path),
        ..Default::default()
    });
}

#[allow(dead_code)]
#[derive(Debug, Default)]
pub struct RuntimeContext {
    pub meta_path: RwLock<String>,
    pub data_path: RwLock<String>,
    pub temp_path: RwLock<String>,

    // require
    pub require_count: AtomicU64, // 需要下载的文件数量
    pub require_bytes: AtomicU64, // 需要下载的文件大小

    // checkpoint
    pub download_bytes: AtomicU64, // 断点续传时, 已经完成的文件数量
    pub download_count: AtomicU64, // 断点续传时, 已经完成的文件大小

    // 下载成功
    pub completed_count: AtomicU64, // 已经完成的文件数量
    pub completed_bytes: AtomicU64, // 已经完成的文件大小

    // 下载失败
    pub uncompleted_count: AtomicU64, // 未完成下载的文件数量
    pub uncompleted_bytes: AtomicU64, // 未完成下载的文件大小
}

impl RuntimeContext {
    pub fn init(&mut self, meta_path: String, data_path: String, temp_path: String) {
        self.meta_path.get_mut().push_str(&meta_path);
        self.data_path.get_mut().push_str(&data_path);
        self.temp_path.get_mut().push_str(&temp_path);
    }

    pub fn add_require(&self, count: u64, bytes: u64) {
        self.require_count
            .fetch_add(count, std::sync::atomic::Ordering::Relaxed);
        self.require_bytes
            .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
    }
    pub fn add_download(&self, count: u64, bytes: u64) {
        self.download_count
            .fetch_add(count, std::sync::atomic::Ordering::Relaxed);
        self.download_bytes
            .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
    }
    pub fn add_completed(&self, count: u64, bytes: u64) {
        self.completed_count
            .fetch_add(count, std::sync::atomic::Ordering::Relaxed);
        self.completed_bytes
            .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
    }
    pub fn add_uncompleted(&self, count: u64, bytes: u64) {
        self.uncompleted_count
            .fetch_add(count, std::sync::atomic::Ordering::Relaxed);
        self.uncompleted_bytes
            .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
    }
}

impl RuntimeContext {
    pub fn snapshot(&self) -> RuntimeSnapshot {
        RuntimeSnapshot {
            require_count: self
                .require_count
                .load(std::sync::atomic::Ordering::Relaxed),
            require_bytes: self
                .require_bytes
                .load(std::sync::atomic::Ordering::Relaxed),

            download_bytes: self
                .download_bytes
                .load(std::sync::atomic::Ordering::Relaxed),
            download_count: self
                .download_count
                .load(std::sync::atomic::Ordering::Relaxed),

            completed_count: self
                .completed_count
                .load(std::sync::atomic::Ordering::Relaxed),
            completed_bytes: self
                .completed_bytes
                .load(std::sync::atomic::Ordering::Relaxed),

            uncompleted_count: self
                .uncompleted_count
                .load(std::sync::atomic::Ordering::Relaxed),
            uncompleted_bytes: self
                .uncompleted_bytes
                .load(std::sync::atomic::Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeSnapshot {
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

impl Display for RuntimeSnapshot {
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
/// META 管理所有meta文件新
pub(crate) static META: LazyLock<Arc<Mutex<HashMap<String, u64>>>> =
    LazyLock::new(|| Arc::new(Mutex::new(HashMap::new())));


pub static META_FILE_LIST: LazyLock<Arc<RwLock<Vec<String>>>> =
    LazyLock::new(|| Arc::new(RwLock::new(Vec::new())));
