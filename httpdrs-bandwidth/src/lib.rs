use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::Notify;

pub struct Bandwidth {
    max_bs: u64, // 最大带宽 bytes
    period_used: AtomicU64, // 已经使用的字节数

    notify: Notify
}

impl Bandwidth {
    pub fn new(max_bs: u64) -> Self {
        Bandwidth {
            max_bs,
            period_used: AtomicU64::new(0),
            notify: Notify::new(),
        }
    }

    pub fn reset_period(&self) {
        self.period_used.store(0, Ordering::Relaxed);
        self.notify.notify_waiters();
    }

    pub async fn permit(&self, desired_bytes: u64) -> Result<u64, Box<dyn std::error::Error>>{
        loop {
            let old_used = self.period_used.fetch_add(desired_bytes, Ordering::Relaxed);
            if old_used + desired_bytes > self.max_bs {
                self.period_used.fetch_sub(desired_bytes, Ordering::Relaxed); // 允许其他任务继续执行
                self.notify.notified().await; // 超出带宽限制，等待通知
                continue
            }
            return Ok(old_used) // 返回期待值之前的值，这样获取到权限的任务，可以知道自己执行完时，一共已经用了多少
        }
    }
}