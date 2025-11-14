use std::sync::Arc;

use indicatif::HumanBytes;
use reqwest::Client;
use reqwest::header::RANGE;
use tokio::fs;
use tokio::time::Instant;

use httpdrs_core::httpd::{HttpdMetaReader, SignatureClient};

use crate::presign;

pub struct DownloadConfig {
    pub data_path: String,
    pub temp_path: String,
    pub max_retries: u32,
}

pub struct DownloadPartParams {
    pub idx_part: u64,
    pub start_pos: u64,
    pub end_pos: u64,
    pub total_parts: u64,
    pub sign: String,
}

/// download_part 请求网络获取分片数据
pub async fn download_part(
    client_down: Arc<Client>,
    client_sign: Arc<SignatureClient>,
    reader_ref: Arc<HttpdMetaReader>,
    params: DownloadPartParams,
    config: DownloadConfig,
) -> Result<u128, Box<dyn std::error::Error>> {
    let start = Instant::now();

    let presign_url = presign::read(params.sign.clone(), client_sign)
        .await
        .map_err(|err| {
            tracing::error!("download_err, presign err: {}", err);
            format!("download_err, presign err: {}", err)
        })?;

    if presign_url.is_empty() {
        tracing::error!("download_err, presign_url is empty");
        return Err("download_err, presign_url is empty".into());
    }

    let path_save = match params.total_parts {
        1 => reader_ref.local_absolute_path_str(&config.data_path),
        _ => reader_ref.local_part_path(&config.data_path, params.idx_part, &config.temp_path),
    };

    let range = format!("bytes={}-{}", params.start_pos, params.end_pos);

    let max_retries = config.max_retries;
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
                    tokio::time::sleep(tokio::time::Duration::from_millis(
                        (100 * retry_count) as u64,
                    ))
                    .await;
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
                tokio::time::sleep(tokio::time::Duration::from_millis(
                    (2000 * retry_count) as u64,
                ))
                .await;
            }
        }
    };

    let resp_status = resp_part.status();
    let resp_byte = resp_part.bytes().await.map_err(|err| {
        tracing::error!(
            "download_part, to bytes err: {}, when status {}",
            err,
            resp_status
        );
        "download_err, to bytes err"
    })?;

    if let Some(parent) = std::path::Path::new(&path_save).parent() {
        fs::create_dir_all(parent).await?;
    }
    tokio::fs::write(path_save.clone(), &resp_byte)
        .await
        .map_err(|err| format!("download_err, file write err: {}", err))?;
    let resp_len = resp_byte.len();

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
        params.idx_part,
        params.start_pos,
        params.end_pos
    );
    Ok(resp_len as u128)
}
