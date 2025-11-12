use std::sync::Arc;

use csv::Reader;
use httpdrs_core::httpd;
use httpdrs_core::httpd::Bandwidth;
use httpdrs_core::httpd::{HttpdMetaReader, SignatureClient};
use reqwest::Client;
use tokio::sync::{Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

use crate::download::download_file;
use crate::stats::RUNTIME;

// 下载流程
pub(crate) async fn down(
    bandwidth: Arc<Bandwidth>,
    jobs: Arc<Semaphore>,
    client_down: Arc<Client>,
    client_sign: Arc<SignatureClient>,
    tx_merge: Arc<mpsc::Sender<(Arc<HttpdMetaReader>, u64, String, String)>>,
    cancel: CancellationToken,
) {
    let meta_path = RUNTIME.lock().unwrap().meta_path.clone();
    let data_path = RUNTIME.lock().unwrap().data_path.clone();
    let csv_paths = std::fs::read_dir(meta_path.as_str()).unwrap();

    // 获取未下载的文件
    // meta 文件并发读取量为 10
    let (tx_read, mut rx_read) = mpsc::channel::<(String, String, u64)>(10);

    let stop = tokio::spawn(async move {
        // 文件下载并发控制10000, 主要受限于存储的QPS
        let semaphore = Arc::new(Semaphore::new(10000));
        let chunk_size = 1024 * 1024 * 5;
        while let Some((_meta_path, sign, size)) = rx_read.recv().await {
            if cancel.is_cancelled() {
                // TODO: 停止下载文件
                break;
            }

            let bandwidth_ = Arc::clone(&bandwidth);
            let jobs_ = Arc::clone(&jobs);
            let client_down_ = Arc::clone(&client_down);
            let client_sign_ = Arc::clone(&client_sign);
            let tx_merge_ = Arc::clone(&tx_merge);
            let semaphore_ = Arc::clone(&semaphore);

            // 开启一个异步任务下载文件
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
                        tracing::info!(
                            "{} downloaded in {} seconds",
                            download_name,
                            download_duration.as_secs()
                        );
                    }
                    None => {}
                }
            });
        }
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
                            // TODO: 完成下载的文件
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
