use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Notify;
use tracing::info;

#[derive(Debug)]
pub struct Bandwidth {
    max_bandwidth: u64, // 最大带宽 bytes/s
    capacity: u64,      // 桶容量
    tokens: AtomicU64,  // 当前令牌数
    last_refill: AtomicU64, // 上次补充令牌的时间（微秒）
    notify: Notify,
}

impl Bandwidth {
    pub fn new(max_bandwidth: u64) -> Arc<Self> {
        // 设置桶容量为最大带宽的1/10，至少为1
        let capacity = std::cmp::max(max_bandwidth / 10, 1);

        let now_micros = Self::current_time_micros();

        let bw = Bandwidth {
            max_bandwidth,
            capacity,
            tokens: AtomicU64::new(capacity),
            last_refill: AtomicU64::new(now_micros),
            notify: Notify::new(),
        };
        info!("Bandwidth init: {} bytes/s, capacity: {}", max_bandwidth, capacity);
        Arc::new(bw)
    }

    /// 获取当前时间（微秒）
    fn current_time_micros() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64
    }

    /// 内部方法：补充令牌并返回当前令牌数
    fn refill_tokens(&self) -> u64 {
        let now_micros = Self::current_time_micros();
        let last_refill = self.last_refill.load(Ordering::Acquire);

        // 计算经过的时间（秒）
        let elapsed_secs = if now_micros > last_refill {
            (now_micros - last_refill) as f64 / 1_000_000.0
        } else {
            0.0
        };

        // 计算应该补充的令牌数
        let new_tokens = (elapsed_secs * self.max_bandwidth as f64) as u64;

        if new_tokens == 0 {
            return self.tokens.load(Ordering::Relaxed);
        }

        // 使用 CAS 循环来更新令牌数和时间
        let mut current_tokens = self.tokens.load(Ordering::Relaxed);
        loop {
            // 计算新的令牌数（不超过容量）
            let new_token_count = std::cmp::min(current_tokens + new_tokens, self.capacity);

            // 尝试原子性地更新 last_refill 和 tokens
            match self.last_refill.compare_exchange_weak(
                last_refill,
                now_micros,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    // 成功更新了时间，现在更新令牌数
                    let old_tokens = self.tokens.swap(new_token_count, Ordering::AcqRel);
                    return old_tokens;
                }
                Err(actual_last_refill) => {
                    // 其他线程已经更新了时间，重新计算
                    let new_elapsed_secs = if now_micros > actual_last_refill {
                        (now_micros - actual_last_refill) as f64 / 1_000_000.0
                    } else {
                        0.0
                    };

                    let actual_new_tokens = (new_elapsed_secs * self.max_bandwidth as f64) as u64;
                    if actual_new_tokens == 0 {
                        return self.tokens.load(Ordering::Relaxed);
                    }

                    current_tokens = self.tokens.load(Ordering::Relaxed);
                    let new_token_count = std::cmp::min(current_tokens + actual_new_tokens, self.capacity);

                    // 尝试更新令牌数
                    match self.tokens.compare_exchange_weak(
                        current_tokens,
                        new_token_count,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => {
                            // 如果可能，更新 last_refill（不强制）
                            let _ = self.last_refill.compare_exchange(
                                actual_last_refill,
                                now_micros,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            );
                            return new_token_count;
                        }
                        Err(actual_tokens) => {
                            current_tokens = actual_tokens;
                            // 继续循环
                        }
                    }
                }
            }
        }
    }

    /// 尝试获取令牌，如果成功返回获取的数量，否则返回需要等待的时间
    fn try_acquire(&self, desired_bytes: u64) -> Result<u64, Duration> {
        // 先补充令牌
        let current_tokens = self.refill_tokens();

        if current_tokens >= desired_bytes {
            // 使用 CAS 循环来获取令牌
            let mut current = current_tokens;
            loop {
                if current < desired_bytes {
                    // 令牌不足，计算需要等待的时间
                    let missing_tokens = desired_bytes - current;
                    let wait_secs = missing_tokens as f64 / self.max_bandwidth as f64;
                    return Err(Duration::from_secs_f64(wait_secs));
                }

                match self.tokens.compare_exchange_weak(
                    current,
                    current - desired_bytes,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return Ok(desired_bytes),
                    Err(actual) => current = actual,
                }

                // 如果令牌数变化，重新检查
                if current < desired_bytes {
                    let missing_tokens = desired_bytes - current;
                    let wait_secs = missing_tokens as f64 / self.max_bandwidth as f64;
                    return Err(Duration::from_secs_f64(wait_secs));
                }
            }
        } else {
            // 令牌不足
            let missing_tokens = desired_bytes - current_tokens;
            let wait_secs = missing_tokens as f64 / self.max_bandwidth as f64;
            Err(Duration::from_secs_f64(wait_secs))
        }
    }

    pub async fn permit(&self, desired_bytes: u64) -> Result<u64, Box<dyn std::error::Error>> {
        let start = Instant::now();
        let mut loop_count = 0;
        let mut total_acquired = 0;
        let mut remaining = desired_bytes;

        while remaining > 0 {
            match self.try_acquire(remaining) {
                Ok(acquired) => {
                    total_acquired += acquired;
                    break;
                }
                Err(wait_time) => {
                    loop_count += 1;

                    if loop_count == 1 {
                        info!("download_bandwidth waiting -> desired: {}, wait_time: {:?}",
                              desired_bytes, wait_time);
                    }

                    // 使用较短的等待时间，避免等待过长
                    let actual_wait = std::cmp::min(wait_time, Duration::from_millis(100));

                    tokio::select! {
                        _ = tokio::time::sleep(actual_wait) => {},
                        _ = self.notify.notified() => {
                            // 有通知，重新检查
                        }
                    }

                    // 防止无限循环
                    if loop_count > 1000 {
                        return Err("Bandwidth: too many retries".into());
                    }

                    // 更新剩余需要获取的令牌数
                    if wait_time <= actual_wait {
                        // 如果实际等待时间足够，假设可以获取部分令牌
                        let expected_acquired = (actual_wait.as_secs_f64() * self.max_bandwidth as f64) as u64;
                        if expected_acquired > 0 {
                            remaining = remaining.saturating_sub(expected_acquired);
                        }
                    }
                }
            }
        }

        if loop_count > 0 {
            info!("bandwidth permit -> total_acquired: {}, wait_time: {:?}, loop_count: {}",
                  total_acquired, start.elapsed(), loop_count);
        }

        Ok(total_acquired)
    }

    /// 获取当前令牌状态
    pub fn status(&self) -> (u64, u64) {
        let tokens = self.tokens.load(Ordering::Relaxed);
        (tokens, self.capacity)
    }

    /// 强制重置令牌桶
    pub fn reset(&self) {
        let now_micros = Self::current_time_micros();
        self.tokens.store(self.capacity, Ordering::Release);
        self.last_refill.store(now_micros, Ordering::Release);
        self.notify.notify_waiters();
    }

    /// 手动触发令牌补充
    pub fn refill(&self) -> u64 {
        self.refill_tokens()
    }
}