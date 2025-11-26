use httpdrs_core::httpd::Bandwidth;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub(crate) async fn reset_period(
    reset_bandwidth: Arc<Bandwidth>,
    token_bandwidth: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(1000)) => {
                reset_bandwidth.reset_period(1000);
            }
            _ = token_bandwidth.cancelled() => {
                break;
            }
        }
    }
}
