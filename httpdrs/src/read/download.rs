use std::sync::Arc;

use reqwest::Client;
use tokio::sync::{Semaphore, mpsc};
use tokio::time::Instant;

use httpdrs_core::httpd;
use httpdrs_core::httpd::{Bandwidth, SignatureClient};
use httpdrs_core::request;

use crate::read::merge::{MergeMessage, MergeSender};
use crate::read::state::RUNTIME;
use crate::read::stream;

pub async fn download_file(
    bandwidth: Arc<Bandwidth>,
    jobs: Arc<Semaphore>,
    client_down: Arc<Client>,
    client_sign: Arc<SignatureClient>,
    merge_sender: Arc<MergeSender>,
    request_reader: Arc<request::FSReader>,
) -> Option<(String, tokio::time::Duration)> {
    let start = Instant::now();
    let chunk_size = request_reader.chunk_size;
    let sign = &request_reader.request_sign;
    let total_parts = request_reader.total_parts();
    let require_size = request_reader.require_size;

    let data_path = RUNTIME.get().unwrap().data_path.read().await.clone();
    let temp_path = RUNTIME.get().unwrap().temp_path.read().await.clone();

    let args = stream::Args::new(data_path.to_string(), temp_path.to_string());

    let reader_ref = Arc::new(httpd::reader_parse(sign.clone()).ok()?);
    let local_path = reader_ref.local_absolute_path_str(data_path.as_str());

    if let Some(local_size) = httpd::check_file_meta(local_path.clone()).await {
        if local_path.exists() {
            if local_size == request_reader.require_size {
                RUNTIME.get()?.add_completed(0, local_size);
                return Some((
                    reader_ref
                        .local_relative_path()
                        .to_string_lossy()
                        .to_string(),
                    start.elapsed(),
                ));
            } else {
                tracing::warn!(
                    "download_check, require_size: {}, local_size: {}, local_path: {:?}",
                    request_reader.require_size,
                    local_size,
                    local_path.clone()
                );
            }
        } else {
            tracing::debug!("download_proc, local_path: {:?}", local_path.clone());
        }
    }

    let (tx_part, mut rx_part) = mpsc::channel::<(u64, usize, i32)>(100);
    let reader_merge = Arc::clone(&reader_ref);

    // 在循环外部创建信号量
    for idx_part in 0..total_parts {
        let part_start = idx_part * chunk_size;
        let part_end = (idx_part + 1) * chunk_size;
        let part_end = if part_end > request_reader.require_size {
            request_reader.require_size
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

        tokio::spawn(async move {
            // 检查这个分片是否已经下载
            let range =
                stream::Range::new(idx_part, part_start, part_end, total_parts, sign_, args_);

            let (range_path, total_parts) = range.path(reader_.clone());
            if total_parts > 1
                && range_path.exists()
                && let Some(local_size) = httpd::check_file_meta(range_path.clone()).await
                && local_size == range.size()
            {
                tracing::info!(
                    "download_range, skip: ({}){}-{}",
                    range.idx_part,
                    range.start_pos,
                    range.end_pos
                );
                // 0: skip, 1: down, 2: fail
                tx_part_
                    .send((idx_part, local_size as usize, 0))
                    .await
                    .unwrap();
                return;
            }

            let _permit = jobs_.acquire().await.unwrap(); // 下载器并发控制
            let _ = bandwidth_.permit(part_size, format!("{}", idx_part)).await; // 获取可以使用带宽后才可以下载
            {
                let jobs_count = jobs_.available_permits();
                tracing::info!("download_jobs: available {}", jobs_count);
            }

            let (length, state) = match stream::stream_download_range(
                client_down_span,
                client_sign_span,
                reader_,
                range,
            )
            .await
            {
                Some((resp_len, resp_state)) => (resp_len, resp_state),
                None => (0, 2),
            };

            // 0: skip, 1: down, 2: fail
            tx_part_
                .send((idx_part, length, state as i32))
                .await
                .unwrap();
        });
    }
    drop(tx_part);

    let mut completed_parts = 0;
    while let Some((_idx_part, range_length, range_state)) = rx_part.recv().await {
        match range_state {
            // 根据状态修改文件处理大小
            0 => {
                completed_parts += 1;
                RUNTIME.get()?.add_download(0, range_length as u64)
            }
            1 => {
                completed_parts += 1;
                RUNTIME.get()?.add_completed(0, range_length as u64)
            }
            _ => {
                RUNTIME.get()?.add_download(0, range_length as u64);
            }
        }
    }

    // 合并逻辑, 状态只修改文件数量
    match total_parts {
        1 => {
            // 不需要合并
            if completed_parts == 1 {
                RUNTIME.get()?.add_completed(1, 0)
            } else {
                RUNTIME.get()?.add_uncompleted(1, 0);
            }
        }
        _ => {
            if completed_parts == total_parts {
                // 需要合并
                RUNTIME.get()?.add_completed(1, 0);
                merge_sender
                    .send(MergeMessage {
                        reader: Arc::clone(&reader_merge),
                        total_parts,
                        total_bytes: require_size,
                        data_path,
                        temp_path,
                    })
                    .await
                    .unwrap();
            } else {
                // 下载失败的, 失败数量+1，不尽兴合并
                RUNTIME.get()?.add_uncompleted(1, 0);
            }
        }
    }

    Some((
        reader_ref
            .local_relative_path()
            .to_string_lossy()
            .to_string(),
        start.elapsed(),
    ))
}
