use std::sync::Arc;

use indicatif::HumanBytes;
use reqwest::Client;
use reqwest::header::RANGE;
use tokio::{fs, time};
use tokio::time::Instant;
use tokio_util::bytes::Bytes;

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
) -> Option<usize> {
    let start = Instant::now();

    let presign_url = match presign::read(params.sign.clone(), client_sign).await{
        Ok(presign_url) => {
            if presign_url.is_empty() {
                tracing::error!("download_err, presign_url is empty");
                return  None
            }
            presign_url
        },
        Err(err) => {
            tracing::error!("download_err, presign err: {}", err);
            return None
        }
    };

    let path_save = match params.total_parts {
        1 => reader_ref.local_absolute_path_str(&config.data_path),
        _ => reader_ref.local_part_path(&config.data_path, params.idx_part, &config.temp_path),
    };

    let range = format!("bytes={}-{}", params.start_pos, params.end_pos);

    let mut retry_count  = 0;
    let resp_bytes = loop {
        let resp_range = stream_request_range(Arc::clone(&client_down), &presign_url, &range).await;
        match resp_range {
            Some(resp_part) => break Some(resp_part),
            None => {
                retry_count += 1;
                if retry_count > config.max_retries {
                    tracing::error!("download_retry, retry max count: {}", retry_count);
                    break None
                }
                time::sleep(time::Duration::from_secs(retry_count as u64)).await;
            }
        }
    }?;

    if let Some(parent) = std::path::Path::new(&path_save).parent() {
        match fs::create_dir_all(parent).await{
            Ok(_) => {}
            Err(err) => {
                tracing::error!("download_err, create dir err: {}", err);
                return None
            }
        }
    }
    let resp_len = resp_bytes.len();
    match fs::write(path_save.clone(), &resp_bytes).await {
        Ok(_) => {
        }
        Err(err) => {
            tracing::error!("download_err, save err: {}", err);
            return None
        }
    }

    let end_duration = start.elapsed();
    let use_sec = end_duration.as_secs();
    let download_speed = resp_len / use_sec as usize;
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
    Some(resp_len)
}


pub async fn stream_request_range(
    client: Arc<Client>,
    url: &str,
    range: &str
) -> Option<Bytes> {

    let rs_send =  client.get(url).header(RANGE, range).send().await;
    let resp = match rs_send {
        Ok(resp) => {
            resp
        },
        Err(err) => {
            tracing::error!("stream_request, reqwest err: {}", err);
            return None
        }
    };

    if !resp.status().is_success(){
        tracing::error!("stream_request, reqwest status: {}", resp.status());
        return None
    }

    let rs_bytes = resp.bytes().await;
    let bytes = match rs_bytes {
        Ok(bytes) => {
            bytes
        },
        Err(err) => {
            tracing::error!("stream_request, reqwest read bytes err: {}", err);
            return None
        }
    };

    Some(bytes)
}