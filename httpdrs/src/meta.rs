use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::state::{META, META_FILE_LIST};

pub async fn read_meta(
    _meta_list: String,
    tx_meta: mpsc::Sender<String>,
    stop_meta: CancellationToken,
    flag_status: u64,
) {
    let mut loop_count = 0;
    loop {
        if stop_meta.is_cancelled() {
            break;
        }

        loop_count += 1;

        // 尝试获取读锁
        let mata_list_path = match META_FILE_LIST.try_read() {
            Ok(meta_guard) => {
                meta_guard.clone()
            }
            Err(_) => {
                // 获取读锁失败，释放当前可能持有的其他资源
                // 然后等待一秒
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                continue;
            }
        };
        let tx_meta_ = tx_meta.clone();

        let mut stop = false;
        for line in mata_list_path {
            let trimmed_line = line.trim();
            if trimmed_line == "---start---" {
                continue;
            }

            if trimmed_line == "---end---" {
                stop = true;
                break;
            }

            {
                let mut meta_map = META.lock().unwrap();
                let flag = *meta_map.get(trimmed_line).unwrap_or(&0);
                if flag & flag_status == flag_status {
                    // 已经读取的跳过
                    continue;
                }
                meta_map.insert(trimmed_line.to_string(), flag | flag_status);
            }

            tx_meta_.send(trimmed_line.to_string()).await.unwrap();
        }

        if stop {
            break;
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }

    tracing::info!("download_read: flag: {}, loop: {}", flag_status, loop_count);
}
