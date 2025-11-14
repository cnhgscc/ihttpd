use std::sync::Arc;

use httpdrs_core::httpd::HttpdMetaReader;
use tokio::fs;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

/// 获取队列文件进行合并
pub async fn init(
    mut mpsc_merge: mpsc::Receiver<(Arc<HttpdMetaReader>, u64, String, String)>,
    cancel: CancellationToken,
) {
    let (tx_merge, mut rx_merge) = mpsc::channel::<u64>(3000);

    let stop = tokio::spawn(async move {
        while let Some(total_parts) = rx_merge.recv().await {
            if cancel.is_cancelled() {
                break;
            }
            tracing::info!("download_merge, total_parts: {}", total_parts);
        }
    });

    while let Some((reader_merge, total_parts, data_path, temp_path)) = mpsc_merge.recv().await {
        let tx_merge_ = tx_merge.clone();
        tokio::spawn(async move {
            match download_merge(
                Arc::clone(&reader_merge),
                total_parts,
                data_path.as_str(),
                temp_path.as_str(),
            )
            .await
            {
                Ok(use_ms) => {
                    tracing::info!("download_merge, use: {:?}", use_ms);
                }
                Err(e) => {
                    tracing::error!("download_merge, error: {}", e);
                }
            };
            tx_merge_.send(total_parts).await.unwrap();
        });
    }
    drop(tx_merge);

    // 等待所有任务完成的通知
    match stop.await {
        Ok(_) => {
            tracing::info!("download_merge: tasks completed");
        }
        Err(e) => {
            tracing::error!("download_merge: merge task monitoring failed: {}", e);
        }
    }
}

// 合并文件
pub async fn download_merge(
    reader: Arc<HttpdMetaReader>,
    chunk_nums: u64,
    data_path: &str,
    temp_path: &str,
) -> Result<tokio::time::Duration, Box<dyn std::error::Error>> {
    let start = Instant::now();

    let file_path = reader.local_absolute_path_str(data_path);
    let _ = tokio::fs::remove_file(file_path.clone())
        .await
        .unwrap_or(());

    if let Some(parent) = std::path::Path::new(&file_path).parent() {
        if !parent.exists() {
            fs::create_dir_all(parent).await?;
        }
    }
    let mut dest_file = match fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(file_path.clone())
        .await
    {
        Ok(dest_file) => dest_file,
        Err(err) => {
            return Err(format!(
                "download_merge, when open dest file, encountered en err: {}, file: {:?}",
                err,
                file_path.clone()
            )
            .into());
        }
    };

    for idx_part in 0..chunk_nums {
        let part_path = reader.local_part_path(data_path, idx_part, temp_path);
        let mut part_file = match fs::File::open(part_path.clone()).await {
            Ok(part_file) => part_file,
            Err(err) => {
                return Err(format!(
                    "download_merge, when open part file, encountered an err: {}，part: {:?}",
                    err,
                    part_path.clone()
                )
                .into());
            }
        };
        tokio::io::copy(&mut part_file, &mut dest_file).await?;
    }
    for idx_part in 0..chunk_nums {
        let part_path = reader.local_part_path(data_path, idx_part, temp_path);
        let _ = tokio::fs::remove_file(part_path).await.unwrap_or(());
    }

    // TODO: 放到分片下载完成处理
    // RUNTIME.lock().unwrap().download_count += 1;

    Ok(start.elapsed())
}
