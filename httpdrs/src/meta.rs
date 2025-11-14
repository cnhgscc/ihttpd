use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::stats::META;

pub async fn read_meta(
    meta_list: String,
    tx_meta: mpsc::Sender<String>,
    stop_meta: CancellationToken,
) {
    loop {
        if stop_meta.is_cancelled() {
            break;
        }

        let meta_content = std::fs::read_to_string(&meta_list).unwrap();
        let tx_meta_ = tx_meta.clone();

        let mut stop = false;
        for line in meta_content.lines() {
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
                if flag & 1 == 1 {
                    continue;
                }
                meta_map.insert(trimmed_line.to_string(), 1); // 设置为初始状态
            }

            tx_meta_.send(trimmed_line.to_string()).await.unwrap();
        }

        if stop {
            break;
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }
}
