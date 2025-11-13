use std::sync::Arc;

use httpdrs_core::httpd;
use httpdrs_core::httpd::{Bandwidth, HttpdMetaReader, SignatureClient};
use reqwest::Client;
use tokio::sync::{Semaphore, mpsc};
use tokio::time::Instant;

use crate::stats::RUNTIME;
use crate::stream;

pub async fn download_file(
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
            {
                let jobs_count = jobs_.available_permits();
                tracing::info!("download_jobs: available {}", jobs_count);
            }

            let (download_len, download_signal) = match stream::download_part(
                client_down_span,
                client_sign_span,
                reader_,
                idx_part,
                part_start,
                part_end,
                total_parts,
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

    // TODO: test 需要合并分片的，下载完毕触发合并
    if total_parts > 1 && completed_parts == total_parts {
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
