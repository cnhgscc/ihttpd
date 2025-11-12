use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Notify;


#[derive(Debug)]
pub struct Bandwidth {
    max_bs: u64, // 最大带宽 bytes
    period_used: AtomicU64, // 已经使用的字节数

    notify: Notify,
}

impl Bandwidth {
    pub fn new(max_bs: u64) -> Arc<Self>{
        let bw = Bandwidth {
            max_bs,
            period_used: AtomicU64::new(0),
            notify: Notify::new(),
        };
        tracing::info!("Bandwidth init: {}", max_bs);
        Arc::new(bw)
    }

    pub fn reset_period(&self, elapsed_ms: u64) -> u64 {
        // 获取值
        let used_bytes = self.period_used.swap(0, Ordering::Relaxed);
        if used_bytes != 0 {
            tracing::info!("download_bandwidth, reset_period: {}", used_bytes);
            self.notify.notify_waiters();
        }
        used_bytes * 1000 / elapsed_ms
    }

    pub async fn permit(&self, desired_bytes: u64) -> Result<u64, Box<dyn std::error::Error>>{
        let start = tokio::time::Instant::now();

        let mut loop_count = 0;
        loop {
            let old_used = self.period_used.fetch_add(desired_bytes, Ordering::Relaxed);
            if old_used + desired_bytes > self.max_bs {
                self.period_used.fetch_sub(desired_bytes, Ordering::Relaxed);
                tracing::info!("download_bandwidth, waiting -> desired: {}  exceed max_bs: {}", desired_bytes, self.max_bs);
                self.notify.notified().await;
                if loop_count > 0 {
                    if loop_count < 2 {
                        tokio::time::sleep(tokio::time::Duration::from_millis(100 * loop_count)).await;
                    }else {
                        tokio::time::sleep(tokio::time::Duration::from_millis(1024)).await;
                    }
                }
                loop_count += 1;
                continue
            }
            if loop_count > 0 {
                tracing::info!("bandwidth, permit -> use: {:?}, loop_count: {} desired: {},  pool: {}", start.elapsed(), loop_count, desired_bytes, old_used);
            }
            return Ok(old_used)
        }
    }
}