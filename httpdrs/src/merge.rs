use std::sync::Arc;

use httpdrs_core::httpd::HttpdMetaReader;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

// 或者更清晰地定义结构体
#[derive(Debug)]
pub struct MergeMessage {
    pub(crate) reader: Arc<HttpdMetaReader>,
    pub(crate) total_parts: u64,
    pub(crate) total_bytes: u64,
    pub(crate) data_path: String,
    pub(crate) temp_path: String,
}

pub type MergeSender = mpsc::Sender<MergeMessage>;

pub type MergeReceiver = mpsc::Receiver<MergeMessage>;

/// 获取队列文件进行合并
pub async fn init(mut merge_receiver: MergeReceiver, cancel: CancellationToken) {
    let (tx_merge, mut rx_merge) = mpsc::channel::<u64>(3000);

    let stop = tokio::spawn(async move {
        while let Some(total_parts) = rx_merge.recv().await {
            if cancel.is_cancelled() {
                break;
            }
            tracing::info!("download_merge, total_parts: {}", total_parts);
        }
    });

    while let Some(message) = merge_receiver.recv().await {
        let tx_merge_ = tx_merge.clone();
        tokio::spawn(async move {
            match download_merge(
                Arc::clone(&message.reader),
                message.total_parts,
                message.total_bytes,
                message.data_path.as_str(),
                message.temp_path.as_str(),
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
            tx_merge_.send(message.total_parts).await.unwrap();
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


const BIG_CHUNK_SIZE: usize = 500 * 1024 * 1024;


pub async fn download_merge(
    reader: Arc<HttpdMetaReader>,
    total_parts: u64,
    total_bytes: u64,
    data_path: &str,
    temp_path: &str,
) -> Result<tokio::time::Duration, Box<dyn std::error::Error>> {
    let start = Instant::now();
    let file_path = reader.local_absolute_path_str(data_path);

    tokio::fs::remove_file(file_path.clone()).await.unwrap_or(());
    if let Some(parent) = std::path::Path::new(&file_path).parent() {
        fs::create_dir_all(parent).await?;
    }

    // 使用 BufWriter 提升写入性能
    let dest_file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)  // 改用 write 模式，append 可能稍慢
        .open(file_path.clone())
        .await?;
    let mut writer = tokio::io::BufWriter::with_capacity(8 * 1024 * 1024, dest_file);

    let mut big_buffer = Vec::with_capacity(BIG_CHUNK_SIZE);
    let mut current_size = 0;

    // 保存分片路径，避免在循环中重复构造路径

    for idx_part in 0..total_parts {
        let part_path = reader.local_part_path(data_path, idx_part, temp_path);

        let mut part_file = fs::File::open(&part_path).await?;
        let metadata = part_file.metadata().await?;
        let part_size = metadata.len();
        let chunk_size = 1024 * 1024 * 5;

        let mut part_data = Vec::new();

        if idx_part == total_parts - 1 {
            let last_part_size = total_bytes - (total_parts - 1) * chunk_size;
            if part_size > last_part_size {
                part_data = vec![0u8; last_part_size as usize];
                part_file.read_exact(&mut part_data).await?;
            } else {
                tokio::io::copy(&mut part_file, &mut part_data).await?;
            }
        } else if part_size > chunk_size {
            part_data = vec![0u8; chunk_size as usize];
            part_file.read_exact(&mut part_data).await?;
        } else {
            tokio::io::copy(&mut part_file, &mut part_data).await?;
        }

        big_buffer.extend_from_slice(&part_data);
        current_size += part_data.len();

        // 缓冲区满了就写入
        if current_size >= BIG_CHUNK_SIZE {
            writer.write_all(&big_buffer[..current_size]).await?;
            big_buffer.clear();
            current_size = 0;
        }
    }

    // 写入剩余数据
    if current_size > 0 {
        writer.write_all(&big_buffer[..current_size]).await?;
    }

    // 确保所有数据都写入磁盘
    writer.flush().await?;

    for idx_part in 0..total_parts {
        let part_path = reader.local_part_path(data_path, idx_part, temp_path);
        tokio::fs::remove_file(part_path).await.unwrap_or(());
    }
    Ok(start.elapsed())
}

// 合并文件
pub async fn download_merge_old(
    reader: Arc<HttpdMetaReader>,
    total_parts: u64,
    total_bytes: u64,
    data_path: &str,
    temp_path: &str,
) -> Result<tokio::time::Duration, Box<dyn std::error::Error>> {
    let start = Instant::now();

    let file_path = reader.local_absolute_path_str(data_path);

    tokio::fs::remove_file(file_path.clone())
        .await
        .unwrap_or(());

    if let Some(parent) = std::path::Path::new(&file_path).parent() {
        fs::create_dir_all(parent).await?;
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

    for idx_part in 0..total_parts {
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

        let metadata = part_file.metadata().await?;
        let part_size = metadata.len();
        let chunk_size = 1024 * 1024 * 5;

        // 最后一块
        if idx_part == total_parts - 1 {
            let last_part_size = total_bytes - (total_parts - 1) * chunk_size;
            if part_size > last_part_size {
                tracing::warn!(
                    "download_merge, last_part: {}, local_part_size: {}, require_part_size: {}",
                    idx_part,
                    part_size,
                    last_part_size
                );
                let mut buffer = vec![0u8; last_part_size as usize];
                part_file.read_exact(&mut buffer).await?;
                dest_file.write_all(&buffer).await?;
            } else {
                tokio::io::copy(&mut part_file, &mut dest_file).await?;
            }
        } else if part_size > chunk_size {
            let mut buffer = vec![0u8; chunk_size as usize];
            part_file.read_exact(&mut buffer).await?;
            dest_file.write_all(&buffer).await?;
            tracing::warn!(
                "download_merge, idx_part: {}, local_part_size: {}",
                idx_part,
                part_size
            )
        } else {
            tokio::io::copy(&mut part_file, &mut dest_file).await?;
        }
    }
    for idx_part in 0..total_parts {
        let part_path = reader.local_part_path(data_path, idx_part, temp_path);
        tokio::fs::remove_file(part_path).await.unwrap_or(());
    }

    Ok(start.elapsed())
}
