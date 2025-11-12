use tokio_util::sync::CancellationToken;
use tokio::time::Instant;
use indicatif::{HumanBytes, ProgressBar};

use httpdrs_core::pbar;

use crate::stats::RUNTIME;


pub(crate) async fn init(pb:ProgressBar, token_bandwidth: CancellationToken){
    let start = Instant::now();

    pb.set_message(pbar::format(0, 0, 0, 0.0));

    let mut last_count: u64 = 0;
    let mut last_bytes: u64 = 0;
    let mut last_speed: u64 = 0;

    loop {
        tokio::select! {
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(1000)) => {
                let start_count = last_count;
                let start_bytes = last_bytes;


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

                last_count = completed_count + download_count;
                last_bytes = completed_bytes + download_bytes;

                let download_percent = match require_bytes {
                    0 => 0.0,
                    _ => (download_bytes + completed_bytes) as f64 / require_bytes as f64,
                };

                let use_ms = start.elapsed().as_millis();
                let period_bytes = last_bytes-start_bytes;
                let period_speed = match use_ms {
                    0 => 0,
                    _ => period_bytes,
                };

                if download_percent < 1.0 {
                    last_speed = period_speed;
                }

                tracing::info!("download_watch, last_speed: {}/s", HumanBytes(last_speed));
                // TODO: 进度条显示
                pb.set_length(RUNTIME.lock().unwrap().require_count);
                pb.set_position(last_count);
                pb.set_message(pbar::format(require_bytes, require_count, period_speed, download_percent));
            }
            _ = token_bandwidth.cancelled() => {
                break;
            }
        }
    }


    let (
        runtime_require_bytes, runtime_require_count, runtime_download_speed
    ) = {
        let r = RUNTIME.lock().unwrap();
        (r.require_bytes, r.require_count, r.download_speed)
    };

    pb.set_message(pbar::format(runtime_require_bytes, runtime_require_count, runtime_download_speed, 1.0));
    pb.finish();

}