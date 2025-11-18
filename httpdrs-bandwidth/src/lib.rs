use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Notify;

#[derive(Debug)]
pub struct Bandwidth {
    max_bs: u64,            // 最大带宽 bytes
    period_used: AtomicU64, // 已经使用的字节数

    period_start: AtomicU64, // 当前周期开始时间

    wait_count: AtomicU64, // 排队数量
    notify: Notify,
}

fn current_time_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as u64
}

impl Bandwidth {
    pub fn new(max_bs: u64) -> Arc<Self> {
        let bw = Bandwidth {
            max_bs,
            period_used: AtomicU64::new(0),
            period_start: AtomicU64::new(current_time_ms()),
            wait_count: AtomicU64::new(0),
            notify: Notify::new(),
        };
        tracing::info!("Bandwidth init: {}", max_bs);
        Arc::new(bw)
    }

    pub fn reset_period(&self, elapsed_ms: u64) -> u64 {
        self.period_start
            .store(current_time_ms(), Ordering::Relaxed);
        let used_bytes = self.period_used.swap(0, Ordering::Relaxed);
        if used_bytes != 0 {
            tracing::info!("download_bandwidth, reset_period: {}", used_bytes);

            let current_waiting = self.wait_count.load(Ordering::Relaxed) as usize;
            let to_notify = current_waiting.min(50);
            for _ in 0..to_notify {
                self.notify.notify_one();
            }
        }

        if elapsed_ms == 0 {
            return 0
        }
        used_bytes * 1000 / elapsed_ms
    }

    pub async fn permit(
        &self,
        desired_bytes: u64,
        name: String,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        loop {
            let old_used = self.period_used.fetch_add(desired_bytes, Ordering::Relaxed);
            if old_used.saturating_add(desired_bytes) > self.max_bs {
                self.period_used.fetch_sub(desired_bytes, Ordering::Relaxed);
                tracing::info!(
                    "download_bandwidth, waiting -> desired: {}  exceed max_bs: {}",
                    desired_bytes,
                    self.max_bs
                );

                let wait_count = self.wait_count.fetch_add(1, Ordering::Relaxed);
                // 时间窗口是 1000ms
                let period_start = self.period_start.load(Ordering::Relaxed);
                let now = current_time_ms();
                // 计算等待时间
                let wait_ms = if now >= period_start + 1000 {
                    // 已经超出时间窗口，但最多等待2秒避免无限等待
                    1000.min(2000) // 等待1秒，但不超过2秒
                } else {
                    period_start + 1000 - now
                };

                tokio::select! {
                    _ = self.notify.notified() => {
                        // 被选中通知的任务, 会比等待的先执行
                    }
                    _ = tokio::time::sleep(tokio::time::Duration::from_millis(wait_ms)) => {
                        // 超时唤醒获取执行权限更低
                    }
                }
                tracing::info!(
                    "download_bandwidth, wakeup -> desired: {}, wait: {}, {}",
                    desired_bytes,
                    wait_count,
                    name
                );

                self.wait_count.fetch_sub(1, Ordering::Relaxed);
                continue;
            }

            return Ok(old_used);
        }
    }
}
