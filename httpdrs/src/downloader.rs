use std::sync::Arc;

use csv::Reader;
use reqwest::Client;
use tokio::sync::{Semaphore, mpsc};
use tokio::time::Instant;

use httpdrs_core::httpd;
use httpdrs_core::httpd::Bandwidth;
use httpdrs_core::httpd::{HttpdMetaReader, SignatureClient};

use crate::download::download_part;
use crate::stats::RUNTIME;

// 下载流程
pub(crate) async fn down(
    bandwidth: Arc<Bandwidth>,
    jobs: Arc<Semaphore>,
    client_down: Arc<Client>,
    client_sign: Arc<SignatureClient>,
    tx_merge: Arc<mpsc::Sender<(Arc<HttpdMetaReader>, u64, String, String)>>,
) {
    let meta_path = RUNTIME.lock().unwrap().meta_path.clone();
    let data_path = RUNTIME.lock().unwrap().data_path.clone();
    let csv_paths = std::fs::read_dir(meta_path.as_str()).unwrap();

    // 获取未下载的文件
    // meta 文件并发读取量为 10
    let (tx_read, mut rx_read) = mpsc::channel::<(String, String, u64)>(10);
    let (tx_down, mut rx_down) = mpsc::channel::<(String, tokio::time::Duration)>(10000);

    tokio::spawn(async move {
        // 文件下载并发控制10000, 主要受限于存储的QPS
        let semaphore = Arc::new(Semaphore::new(10000));
        let chunk_size = 1024 * 1024 * 5;
        while let Some((_meta_path, sign, size)) = rx_read.recv().await {
            let bandwidth_ = Arc::clone(&bandwidth);
            let jobs_ = Arc::clone(&jobs);
            let client_down_ = Arc::clone(&client_down);
            let client_sign_ = Arc::clone(&client_sign);
            let tx_merge_ = Arc::clone(&tx_merge);
            let tx_down_ = tx_down.clone();
            let semaphore_ = Arc::clone(&semaphore);

            // 开启一个任务下载文件
            tokio::spawn(async move {
                let _permit = semaphore_.acquire().await.unwrap(); // 最大并发下载文件数量
                let (download_name, download_duration) = match download_file(
                    bandwidth_,
                    jobs_,
                    client_down_,
                    client_sign_,
                    tx_merge_,
                    sign,
                    size,
                    chunk_size,
                )
                .await
                {
                    Ok((download_name, download_duration)) => {
                        (download_name, Option::from(download_duration))
                    }
                    Err(e) => (e.to_string(), None),
                };

                match download_duration {
                    Some(download_duration) => {
                        tx_down_
                            .send((download_name, download_duration))
                            .await
                            .unwrap();
                    }
                    None => {}
                }
            });
        }
        drop(tx_down);
    });

    tokio::spawn(async move {
        for csv_path in csv_paths {
            let tx_sender = tx_read.clone();
            let data_path = data_path.clone();
            tokio::spawn(async move {
                let csv_meta_path = csv_path.unwrap().path().to_string_lossy().to_string();
                let mut csv_reader = Reader::from_path(csv_meta_path.as_str()).unwrap();

                for raw_result in csv_reader.records() {
                    let raw_line = raw_result.unwrap();
                    let sign = raw_line.get(0).unwrap().to_string();
                    let size = raw_line.get(1).unwrap().parse::<u64>().unwrap();
                    let httpd_reader = httpd::reader_parse(sign.clone()).unwrap();
                    if let Some(reader_size) = httpd_reader.check_local_file(data_path.as_str()) {
                        if reader_size == size {
                            continue;
                        }
                        tx_sender
                            .send((csv_meta_path.clone(), sign, size))
                            .await
                            .unwrap();
                    } else {
                        tx_sender
                            .send((csv_meta_path.clone(), sign, size))
                            .await
                            .unwrap();
                    }
                }
            });
        }
        // 检查完毕
        drop(tx_read);
    });

    while let Some((name, use_ms)) = rx_down.recv().await {
        tracing::debug!("download_complete, use: {:?}, file: {:?}", use_ms, name);
    } // 下载任务处理完成

    // TODO: 释放 tx_merge
}

async fn download_file(
    bandwidth: Arc<Bandwidth>,
    jobs: Arc<Semaphore>,
    client_down: Arc<Client>,
    client_sign: Arc<SignatureClient>,
    tx_merge: Arc<mpsc::Sender<(Arc<HttpdMetaReader>, u64, String, String)>>,
    sign: String,
    require_size: u64,
    chunk_size: u64,
) -> Result<(String, tokio::time::Duration), Box<dyn std::error::Error>> {
    let start = Instant::now();

    let data_path = Arc::new({
        RUNTIME.lock().unwrap().data_path.clone() // 提前获取并释放锁
    });
    let temp_path = Arc::new({
        RUNTIME.lock().unwrap().temp_path.clone() // 提前获取并释放锁
    });

    let reader_ref = Arc::new(httpd::reader_parse(sign.clone())?);
    let local_path = reader_ref.local_absolute_path_str(data_path.as_str());
    tracing::debug!(
        "download, sign: {} -> {:?}",
        reader_ref,
        reader_ref.local_absolute_path_str(data_path.as_str())
    );

    if let Some(local_size) = httpd::check_file_meta(local_path.clone()) {
        if local_size == require_size {
            return Ok((
                reader_ref
                    .local_relative_path()
                    .to_string_lossy()
                    .to_string(),
                start.elapsed(),
            ));
        } else {
            tracing::debug!(
                "download, start: {}, local: {}, require: {}",
                reader_ref,
                local_size,
                require_size
            );
        }
    }

    let total_parts = (require_size + chunk_size - 1) / chunk_size;

    // 每个文件都创建一下分片下载的最大检查队列
    let (tx_part, mut rx_part) = mpsc::channel::<(u64, u128, i32)>(100);
    let reader_merge = Arc::clone(&reader_ref);

    tracing::debug!("download, parts: {}, {}", total_parts, reader_ref);

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
        let data_path_ = Arc::clone(&data_path);
        let temp_path_ = Arc::clone(&temp_path);

        let client_down_span = Arc::clone(&client_down);
        let client_sign_span = Arc::clone(&client_sign);

        // TODO: remove permit
        let _ = bandwidth_.permit(part_size).await; // 获取可以使用带宽后才可以下载
        tokio::spawn(async move {
            let _permit = jobs_.acquire().await.unwrap(); // 下载器并发控制
            let (download_len, download_signal) = match download_part(
                client_down_span,
                client_sign_span,
                reader_,
                idx_part,
                part_start,
                part_end,
                part_size,
                sign_,
                data_path_.as_str(),
                temp_path_.as_str(),
            )
            .await
            {
                Ok(resp_len) => (resp_len, 1),
                Err(e) => {
                    tracing::error!("download_part, error: {}, part: {:?}", e, idx_part);
                    (0, 0)
                }
            };

            tx_part_
                .send((idx_part, download_len, download_signal))
                .await
                .unwrap();
        });
    }
    drop(tx_part);

    let mut completed_parts = 0;
    while let Some((idx_part, download_len, download_signal)) = rx_part.recv().await {
        completed_parts += 1;
        RUNTIME.lock().unwrap().download_bytes += download_len as u64;
        tracing::debug!(
            "download_part, complete {}, use: {}  part: {:?}, {}",
            reader_merge,
            download_len,
            idx_part,
            download_signal
        );
    }

    // 文件下载完成，计数加1
    RUNTIME.lock().unwrap().download_count += 1;

    // 下载完毕触发合并
    if completed_parts == total_parts {
        tx_merge
            .send((
                Arc::clone(&reader_merge),
                total_parts,
                (*data_path).clone(),
                (*temp_path).clone(),
            ))
            .await
            .unwrap();
    }
    Ok((
        reader_ref
            .local_relative_path()
            .to_string_lossy()
            .to_string(),
        start.elapsed(),
    ))
}
