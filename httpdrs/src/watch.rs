use tokio_util::sync::CancellationToken;
use tokio::time::Instant;
use indicatif::ProgressBar;

use httpdrs_core::pbar;

use crate::stats::RUNTIME;


pub(crate) async fn init(pb:ProgressBar, token_bandwidth: CancellationToken){
    let start = Instant::now();

    pb.set_message(pbar::format(0, 0, 0, 0.0));

    loop {
        tokio::select! {
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(1000)) => {
                let (require_bytes, require_count) = {
                    let runtime = RUNTIME.lock().unwrap();
                    (runtime.require_bytes, runtime.require_count)
                };

                let (completed_bytes, completed_count) = {
                    let runtime = RUNTIME.lock().unwrap();
                    (runtime.completed_bytes, runtime.completed_count)
                };

                let (download_bytes, download_count) = {
                    let runtime = RUNTIME.lock().unwrap();
                    (runtime.download_bytes, runtime.download_count)
                };

                let download_percent = match require_bytes {
                    0 => 0.0,
                    _ => (download_bytes + completed_bytes) as f64 / require_bytes as f64,
                };

                let use_ms = start.elapsed().as_millis();
                let period_download_bytes =  RUNTIME.lock().unwrap().download_bytes;
                let period_speed = match use_ms {
                    0 => 0,
                    _ => period_download_bytes  / use_ms as u64  * 1000,
                };

                // TODO: 进度条显示
                pb.set_length(RUNTIME.lock().unwrap().require_count);
                pb.set_position(completed_count + download_count);
                pb.set_message(pbar::format(require_bytes, require_count, period_speed, download_percent));
            }
            _ = token_bandwidth.cancelled() => {
                break;
            }
        }
    }

}