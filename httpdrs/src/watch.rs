use crate::stats::RUNTIME;
use tokio_util::sync::CancellationToken;
use indicatif::ProgressBar;
use httpdrs_core::pbar;

pub(crate) async fn init(pb:ProgressBar, token_bandwidth: CancellationToken){
    pb.set_message(pbar::format(0, 0, 0));
    loop {
        tokio::select! {
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(300)) => {
                pb.set_length(RUNTIME.lock().unwrap().require_count);
                // pb.set_position(RUNTIME.lock().unwrap().download_count);
            }
            _ = token_bandwidth.cancelled() => {
                break;
            }
        }
    }

}