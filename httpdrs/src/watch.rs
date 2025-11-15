use indicatif::{HumanBytes, ProgressBar};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use httpdrs_core::pbar;

use crate::state::RUNTIME;

pub(crate) async fn init(pb: ProgressBar, token_bandwidth: CancellationToken) {
    let start = Instant::now();

    pb.set_message(pbar::format(0, 0, 0.0, 0, 0));

    let mut last_count: u64 = 0;
    let mut last_bytes: u64 = 0;
    let mut last_speed: u64 = 0;

    loop {
        tokio::select! {
            _ = tokio::time::sleep(tokio::time::Duration::from_millis(1000)) => {
                let _start_count = last_count;
                let start_bytes = last_bytes;

                let (
                    require_bytes,
                    require_count,
                    completed_bytes,
                    completed_count,
                    uncompleted_bytes,
                    uncompleted_count,
                    download_bytes,
                    download_count
                ) = {
                    let runtime = RUNTIME.get().unwrap().snapshot();
                    (
                        runtime.require_bytes,
                        runtime.require_count,
                        runtime.completed_bytes,
                        runtime.completed_count,
                        runtime.uncompleted_bytes,
                        runtime.uncompleted_count,
                        runtime.download_bytes,
                        runtime.download_count,
                    )
                };

                // 下载百分比, 断点续传+成功+失败 / 总量
                let download_percent = match require_bytes {
                    0 => 0.0,
                    _ => (download_bytes + completed_bytes + uncompleted_bytes) as f64 / require_bytes as f64,
                };

                // 计算带宽(成功)
                last_bytes = completed_bytes;

                let use_ms = start.elapsed().as_millis();
                let period_bytes = last_bytes - start_bytes;
                let period_speed = match use_ms {
                    0 => 0,
                    _ => period_bytes,
                };

                if download_percent < 1.0 {
                    last_speed = period_speed;
                }

                // 所有处理过的文件数量 成功+失败+断点续传
                last_count = completed_count + uncompleted_count + download_count;

                // TODO: 计算剩余时间
                let process_bytes = completed_bytes + uncompleted_bytes + download_bytes;
                let remaining_bytes = require_bytes.saturating_sub(process_bytes);


                let speed_avg = match use_ms {
                    0 => 0,
                    _ =>{
                        let speed = period_bytes as u128 * 1000 / use_ms;
                        match speed{
                            0 => download_bytes  as u128 / use_ms, // 检查速度
                            _ => speed // 下载的速度
                        }
                    },
                };

                let remaining_time=  match speed_avg {
                    0 => 0,
                    _ => remaining_bytes as u128 / speed_avg,
                };

                tracing::info!("download_watch, last_speed: {}/s", HumanBytes(last_speed));
                pb.set_length(require_count);
                pb.set_position(last_count);

                pb.set_message(pbar::format(
                    require_bytes,
                    period_speed,
                    download_percent,
                    process_bytes,
                    remaining_time
                ));
            }
            _ = token_bandwidth.cancelled() => {
                break;
            }
        }
    }
}
