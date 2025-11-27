use std::path::PathBuf;
use std::sync::Arc;

use indicatif::HumanBytes;
use reqwest::Client;
use reqwest::header::RANGE;
use tokio::time::Instant;
use tokio::{fs, time};
use tokio_util::bytes::Bytes;

use httpdrs_core::httpd::{HttpdMetaReader, SignatureClient};
use httpdrs_core::read::presign;

pub struct Args {
    pub data_path: String,
    pub temp_path: String,
}

impl Args {
    pub fn new(data_path: String, temp_path: String) -> Arc<Self> {
        let args = Args {
            data_path,
            temp_path,
        };
        Arc::new(args)
    }
}

pub struct Range {
    pub idx_part: u64,
    pub start_pos: u64,
    pub end_pos: u64,
    pub total_parts: u64,
    pub sign: String,
    pub args: Arc<Args>,
}

impl Range {
    pub fn new(
        idx_part: u64,
        start_pos: u64,
        end_pos: u64,
        total_parts: u64,
        sign: String,
        args: Arc<Args>,
    ) -> Self {
        Range {
            idx_part,
            start_pos,
            end_pos,
            total_parts,
            sign,
            args,
        }
    }

    pub fn header(&self) -> String {
        // bytes=0-10 是 11个字节，所以需要减1
        format!("bytes={}-{}", self.start_pos, self.end_pos - 1)
    }

    pub fn path(&self, reader: Arc<HttpdMetaReader>) -> (PathBuf, u64) {
        match self.total_parts {
            1 => (
                reader.local_absolute_path_str(self.args.data_path.as_str()),
                self.total_parts,
            ),
            _ => (
                reader.local_part_path(
                    self.args.data_path.as_str(),
                    self.idx_part,
                    self.args.temp_path.as_str(),
                ),
                self.total_parts,
            ),
        }
    }
    pub fn size(&self) -> u64 {
        self.end_pos - self.start_pos
    }
}

/// stream_download_range 请求网络获取数据块
/// 返回值是下载的(数据块大小, 下载状态)，None -> retry
/// 下载状态 0: skip, 1: down
pub async fn stream_download_range(
    client_down: Arc<Client>,
    client_sign: Arc<SignatureClient>,
    reader_ref: Arc<HttpdMetaReader>,
    range: Range,
) -> Option<(usize, usize)> {
    let start = Instant::now();

    let (range_path, _) = range.path(reader_ref);

    let presign_url = presign::read(range.sign.clone(), client_sign).await?;

    let mut retry_count = 0;
    let max_retries = 20;
    let resp_bytes = loop {
        let resp_range =
            stream_request_range(Arc::clone(&client_down), &presign_url, &range.header()).await;
        match resp_range {
            Some(resp_part) => break Some(resp_part),
            None => {
                retry_count += 1;
                if retry_count <= max_retries {
                    time::sleep(time::Duration::from_secs(retry_count as u64)).await;
                    break None;
                }
                tracing::error!(
                    "download_retry, retry: {}, presign: {}",
                    retry_count,
                    presign_url
                );
            }
        }
    }?;

    if let Some(parent) = std::path::Path::new(&range_path).parent() {
        match fs::create_dir_all(parent).await {
            Ok(_) => {}
            Err(err) => {
                tracing::error!("download_err, create dir err: {}", err);
                return None;
            }
        }
    }
    let resp_len = resp_bytes.len();
    match fs::write(range_path.clone(), &resp_bytes).await {
        Ok(_) => {}
        Err(err) => {
            tracing::error!("download_err, save err: {}", err);
            return None;
        }
    }

    let end_duration = start.elapsed();
    let use_sec = end_duration.as_millis();
    let download_speed = resp_len / use_sec as usize * 1000;
    let download_speed_str = HumanBytes(download_speed as u64);

    tracing::info!(
        "download_range, use: {:?}, ({}/{}s), retry: {}, pos: ({}){}-{}",
        end_duration,
        resp_len,
        download_speed_str,
        retry_count,
        range.idx_part,
        range.start_pos,
        range.end_pos
    );
    Some((resp_len, 1))
}

pub async fn stream_request_range(client: Arc<Client>, url: &str, range: &str) -> Option<Bytes> {
    let rs_send = client.get(url).header(RANGE, range).send().await;
    let resp = match rs_send {
        Ok(resp) => resp,
        Err(err) => {
            tracing::error!("stream_request, reqwest err: {}", err);
            return None;
        }
    };

    if !resp.status().is_success() {
        tracing::error!("stream_request, reqwest status: {}", resp.status());
        return None;
    }

    // TODO: 这种情况的需要增加重试
    let rs_bytes = resp.bytes().await;
    let bytes = match rs_bytes {
        Ok(bytes) => bytes,
        Err(err) => {
            tracing::error!("stream_request, reqwest read bytes err: {}", err);
            return None;
        }
    };

    Some(bytes)
}
