use httpdrs_core::httpd::SignatureClient;
use std::sync::Arc;
use tokio::time::Instant;

pub async fn read(
    sign: String,
    with_client: Arc<SignatureClient>,
) -> Result<String, Box<dyn std::error::Error>> {
    let start = Instant::now();
    let reader = match with_client.reader_get(sign).await {
        Ok(reader) => reader,
        Err(err) => {
            return Err(format!("download_err, reader_presign err: {}", err).into());
        }
    };
    if reader.code != 0 {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            "status_code != 200",
        )));
    }
    tracing::info!("download_presign, use {:?}", start.elapsed());
    Ok(reader.data.endpoint)
}
