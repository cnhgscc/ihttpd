use std::sync::Arc;

use indicatif::HumanBytes;
use reqwest::Client;
use reqwest::header::RANGE;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::time::Instant;

use httpdrs_core::httpd::{HttpdMetaReader, SignatureClient};

use crate::presign;

/// download_part 请求网络获取分片数据
pub async fn download_part(
    client_down: Arc<Client>,
    client_sign: Arc<SignatureClient>,
    reader_ref: Arc<HttpdMetaReader>,
    idx_part: u64,
    start_pos: u64,
    end_pos: u64,
    total_parts: u64,
    sign: String,
    data_path: &str,
    temp_path: &str,
) -> Result<u128, Box<dyn std::error::Error>> {
    let start = Instant::now();

    let presign_url = match presign::read(sign.clone(), client_sign).await {
        Ok(presign_url) => presign_url,
        Err(err) => {
            tracing::error!("download_err, presign err: {}", err);
            return Err(format!("download_err, presign err: {}", err).into());
        }
    };

    let presign_url = match presign_url.as_str() {
        "" => {
            tracing::error!("download_err, presign_url is empty");
            return Err("download_err, presign_url is empty".into());
        }
        _ => presign_url,
    };

    tracing::info!(
        "download_part, total_parts: {}, presign_url: {}",
        total_parts,
        presign_url
    );

    let path_save = match total_parts {
        1 => reader_ref.local_absolute_path_str(data_path),
        _ => reader_ref.local_part_path(data_path, idx_part, temp_path),
    };

    let range = format!("bytes={}-{}", start_pos, end_pos);
    tracing::debug!(
        "download_part, presign: {}, use: {:?}",
        presign_url,
        start.elapsed()
    );

    let max_retries = 20;
    let mut retry_count = 0;
    let resp_part = loop {
        let resp_part = client_down
            .get(presign_url.clone())
            .header(RANGE, range.clone())
            .send()
            .await;
        match resp_part {
            Ok(resp_part) => match resp_part.status().as_u16() {
                200..=299 => {
                    break resp_part;
                }
                _ => {
                    retry_count += 1;
                    if retry_count >= max_retries {
                        return Err(format!(
                            "download_err, resp status is {}, {}",
                            resp_part.status(),
                            presign_url
                        )
                        .into());
                    }
                    tokio::time::sleep(tokio::time::Duration::from_millis(100 * retry_count)).await;
                }
            },
            Err(err) => {
                retry_count += 1;
                if retry_count >= max_retries {
                    return Err(format!(
                        "download_err, reqwest_retry: {}  err: {}",
                        retry_count, err
                    )
                    .into());
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(2000 * retry_count)).await;
            }
        }
    };

    let resp_status = resp_part.status();
    let resp_byte = match resp_part.bytes().await.ok() {
        Some(resp_bytes) => resp_bytes,
        None => {
            tracing::error!("download_part, to bytes err, when status {}", resp_status);
            return Err("download_err, to bytes err".into());
        }
    };

    if let Some(parent) = std::path::Path::new(&path_save).parent() {
        if !parent.exists() {
            fs::create_dir_all(parent).await?;
        }
    }
    let mut file = match fs::File::create(path_save.clone()).await {
        Ok(file) => file,
        Err(err) => {
            return Err(format!("download_err, file create err, {:?}", err).into());
        }
    };

    let resp_len = match file.write_all(&resp_byte).await {
        Ok(_) => resp_byte.len(),
        Err(err) => {
            return Err(format!("download_err, write_all err: {}", err).into());
        }
    };

    let end_duration = start.elapsed();
    let use_ms = end_duration.as_millis();
    let download_speed = resp_len / use_ms as usize * 1000;
    let download_speed_str = HumanBytes(download_speed as u64);

    tracing::info!(
        "download_part:, use: {:?}, ({}/{}s), retry: {}, pos: ({}){}-{}",
        end_duration,
        resp_len,
        download_speed_str,
        retry_count,
        idx_part,
        start_pos,
        end_pos
    );
    Ok(resp_len as u128)
}
