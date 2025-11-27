use std::sync::Arc;

use tokio::time;

use crate::httpd::SignatureClient;

/// read 获取下载链接
pub async fn read(sign: String, with_client: Arc<SignatureClient>) -> Option<String> {
    let start = time::Instant::now();

    let reader = match with_client.reader_get(sign).await {
        Ok(reader) => reader,
        Err(err) => {
            tracing::error!("download_err, reader_presign err: {}", err);
            return None;
        }
    };

    if reader.code != 0 {
        tracing::error!(
            "download_presign, resp_code != 0, message: {}",
            reader.message
        );
        return None;
    }
    if reader.data.endpoint.is_empty() {
        tracing::error!("download_presign, endpoint is empty");
        return None;
    }

    tracing::info!("download_presign, use {:?}", start.elapsed());
    Some(reader.data.endpoint)
}
