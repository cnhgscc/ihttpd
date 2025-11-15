use std::sync::Arc;

use reqwest::Client;
use tokio::sync::{Semaphore, mpsc};
use tokio::time::Instant;

use httpdrs_core::httpd;
use httpdrs_core::httpd::{Bandwidth, SignatureClient};

use crate::merge::{MergeMessage, MergeSender};
use crate::stats::RUNTIME;
use crate::stream;

pub struct DownloadFileConfig {
    pub sign: String,
    pub require_size: u64,
    pub chunk_size: u64,
}

impl DownloadFileConfig {
    pub fn new(sign: String, require_size: u64) -> Self {
        DownloadFileConfig {
            sign,
            require_size,
            chunk_size: 1024 * 1024 * 5,
        }
    }
}

pub async fn download_file(
    bandwidth: Arc<Bandwidth>,
    jobs: Arc<Semaphore>,
    client_down: Arc<Client>,
    client_sign: Arc<SignatureClient>,
    merge_sender: Arc<MergeSender>,
    config: DownloadFileConfig,
) -> Option<(String, tokio::time::Duration)> {
    let start = Instant::now();
    let require_size = config.require_size;
    let chunk_size = config.chunk_size;
    let sign = config.sign;

    let data_path = Arc::new({
        RUNTIME.lock().unwrap().data_path.clone() // 提前获取并释放锁
    });
    let temp_path = Arc::new({
        RUNTIME.lock().unwrap().temp_path.clone() // 提前获取并释放锁
    });

    let args = stream::Args::new(data_path.to_string(), temp_path.to_string());

    let reader_ref = Arc::new(httpd::reader_parse(sign.clone()).ok()?);
    let local_path = reader_ref.local_absolute_path_str(data_path.as_str());

    if let Some(local_size) = httpd::check_file_meta(local_path.clone())
        && local_size == config.require_size
    {
        RUNTIME.lock().unwrap().completed_bytes += local_size; // TODO: 计算完成字节
        return Some((
            reader_ref
                .local_relative_path()
                .to_string_lossy()
                .to_string(),
            start.elapsed(),
        ));
    }

    let total_parts = require_size.div_ceil(chunk_size);

    let (tx_part, mut rx_part) = mpsc::channel::<(u64, usize, i32)>(100);
    let reader_merge = Arc::clone(&reader_ref);

    for idx_part in 0..total_parts {
        let part_start = idx_part * chunk_size;
        let part_end = (idx_part + 1) * chunk_size;
        let part_end = if part_end > require_size {
            require_size
        } else {
            part_end
        };
        let part_size = part_end - part_start;

        let reader_ = Arc::clone(&reader_ref);
        let bandwidth_ = Arc::clone(&bandwidth);
        let jobs_ = Arc::clone(&jobs);
        let tx_part_ = tx_part.clone();
        let sign_ = sign.clone();
        let args_ = Arc::clone(&args);

        let client_down_span = Arc::clone(&client_down);
        let client_sign_span = Arc::clone(&client_sign);

        let _ = bandwidth_.permit(part_size).await; // 获取可以使用带宽后才可以下载
        tokio::spawn(async move {
            let _permit = jobs_.acquire().await.unwrap(); // 下载器并发控制
            {
                let jobs_count = jobs_.available_permits();
                tracing::info!("download_jobs: available {}", jobs_count);
            }

            let (len, signal) = match stream::stream_download_range(
                client_down_span,
                client_sign_span,
                reader_,
                stream::Range::new(idx_part, part_start, part_end, total_parts, sign_, args_),
            )
            .await
            {
                Some(resp_len) => (resp_len, 1),
                None => (0, 0), //
            };

            tx_part_.send((idx_part, len, signal)).await.unwrap();
        });
    }
    drop(tx_part);

    let mut completed_parts = 0;
    while let Some((_idx_part, download_len, _download_signal)) = rx_part.recv().await {
        // TODO: 处理经过重试，下载失败的数据进行记录逻辑 _download_signal
        completed_parts += 1;
        RUNTIME.lock().unwrap().download_bytes += download_len as u64;
    }
    RUNTIME.lock().unwrap().download_count += 1;

    if total_parts > 1 && completed_parts == total_parts {
        merge_sender
            .send(MergeMessage {
                reader: Arc::clone(&reader_merge),
                total_parts,
                data_path: (*data_path).clone(),
                temp_path: (*temp_path).clone(),
            })
            .await
            .unwrap();
    }
    Some((
        reader_ref
            .local_relative_path()
            .to_string_lossy()
            .to_string(),
        start.elapsed(),
    ))
}
