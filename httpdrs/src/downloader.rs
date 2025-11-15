use std::sync::Arc;

use csv::Reader;
use reqwest::Client;
use tokio::sync::{Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

use httpdrs_core::httpd;
use httpdrs_core::httpd::Bandwidth;
use httpdrs_core::httpd::SignatureClient;

use crate::download::{DownloadFileConfig, download_file};
use crate::merge::MergeSender;
use crate::meta;
use crate::stats::RUNTIME;

// 下载流程
pub(crate) async fn down(
    bandwidth: Arc<Bandwidth>,
    jobs: Arc<Semaphore>,
    client_down: Arc<Client>,
    client_sign: Arc<SignatureClient>,
    tx_merge: Arc<MergeSender>,
    cancel: CancellationToken,
) {
    let meta_path = RUNTIME.lock().unwrap().meta_path.clone();
    let data_path = RUNTIME.lock().unwrap().data_path.clone();
    let temp_path = RUNTIME.lock().unwrap().temp_path.clone();

    let meta_list = format!("{}/meta.list", temp_path);
    while std::fs::metadata(&meta_list).is_err() {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }

    let (tx_meta, mut rx_meta) = mpsc::channel::<String>(100);
    tokio::spawn(meta::read_meta(meta_list, tx_meta, cancel.clone(), 2));

    // 获取未下载的文件
    let (tx_read, mut rx_read) = mpsc::channel::<(String, String, u64)>(5);

    let stop_down = cancel.clone();
    let stop = tokio::spawn(async move {
        // 文件下载并发控制10000, 主要受限于存储的QPS
        let semaphore = Arc::new(Semaphore::new(10000));

        while let Some((_meta_path, sign, size)) = rx_read.recv().await {
            if stop_down.is_cancelled() {
                break;
            }

            let bandwidth_ = Arc::clone(&bandwidth);
            let parallel_ = Arc::clone(&jobs);
            let client_down_ = Arc::clone(&client_down);
            let client_sign_ = Arc::clone(&client_sign);
            let tx_merge_ = Arc::clone(&tx_merge);
            let semaphore_ = Arc::clone(&semaphore);

            let config_down = DownloadFileConfig::new(sign, size);

            // 开启一个异步任务下载文件
            tokio::spawn(async move {
                let _permit = semaphore_.acquire().await.unwrap(); // 最大并发下载文件数量
                tracing::info!(
                    "download_submit, available_permits: {}",
                    semaphore_.available_permits()
                );
                download_file(
                    bandwidth_,
                    parallel_,
                    client_down_,
                    client_sign_,
                    tx_merge_,
                    config_down,
                )
                .await
            });
        }
    });

    let stop_read = cancel.clone();
    tokio::spawn(async move {
        while let Some(meta_name) = rx_meta.recv().await {
            if stop_read.is_cancelled() {
                break;
            }
            let tx_sender = tx_read.clone();
            let data_path = data_path.clone();
            let csv_meta_path = format!("{}/{}", meta_path, meta_name);

            tokio::spawn(async move {
                let mut csv_reader = Reader::from_path(csv_meta_path.as_str()).unwrap();
                for raw_result in csv_reader.records() {
                    let raw_line = raw_result.unwrap();
                    let sign = raw_line.get(0).unwrap().to_string();
                    let size = raw_line.get(1).unwrap().parse::<u64>().unwrap();
                    let httpd_reader = httpd::reader_parse(sign.clone()).unwrap();
                    if let Some(reader_size) = httpd_reader.check_local_file(data_path.as_str()) {
                        if reader_size == size {
                            let mut rt = RUNTIME.lock().unwrap();
                            rt.completed_bytes += size;
                            rt.completed_count += 1;
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

    // 等到处理完成
    stop.await.unwrap();
}
