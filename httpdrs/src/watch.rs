use crate::stats::RUNTIME;
use tokio_util::sync::CancellationToken;
use indicatif::ProgressBar;
use httpdrs_core::pbar;

pub(crate) async fn init(pb:ProgressBar, token_bandwidth: CancellationToken){
    pb.set_message(pbar::format(0, 0, 0));
    loop {
        tokio::select! {
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(300)) => {
                let (require_bytes, require_count) = {
                    let runtime = RUNTIME.lock().unwrap();
                    (runtime.require_bytes, runtime.require_count)
                };

                let (completed_bytes, completed_count) = {
                    let runtime = RUNTIME.lock().unwrap();
                    (runtime.completed_bytes, runtime.completed_count)
                };

                tracing::info!(
                    "==========require_bytes: {}, require_count: {}, completed_bytes: {}, completed_count: {}==============",
                    require_bytes, require_count, completed_bytes, completed_count);

                pb.set_length(RUNTIME.lock().unwrap().require_count);
                pb.set_position(completed_count + RUNTIME.lock().unwrap().download_count);
                pb.set_message(pbar::format(require_bytes, require_count, RUNTIME.lock().unwrap().download_speed));
            }
            _ = token_bandwidth.cancelled() => {
                break;
            }
        }
    }

}