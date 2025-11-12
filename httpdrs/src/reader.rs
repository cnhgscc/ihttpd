use crate::stats::RUNTIME;
use csv::Reader;
use httpdrs_core::httpd;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

pub(crate) async fn init(cancel: CancellationToken) {
    let start = Instant::now();

    let meta_path = RUNTIME.lock().unwrap().meta_path.clone();
    let csv_paths = std::fs::read_dir(meta_path.as_str()).unwrap();

    let (tx, mut rx) = mpsc::channel::<(String, u64, u64)>(2);

    let stop_wait = cancel.clone();
    let stop = tokio::spawn(async move {
        while let Some((_csv, bytes, size)) = rx.recv().await {
            if stop_wait.is_cancelled() {
                break;
            }
            // 执行下载逻辑
            let mut rt = RUNTIME.lock().unwrap();
            rt.require_bytes += bytes;
            rt.require_count += size;
        }
    });

    let stop_read = cancel.clone();
    tokio::spawn(async move {
        for csv_path in csv_paths {
            if stop_read.is_cancelled() {
                break;
            }

            let tx_sender = tx.clone();
            tokio::spawn(async move {
                let meta_path = csv_path.unwrap().path().to_string_lossy().to_string();
                let mut csv_reader = Reader::from_path(meta_path.as_str()).unwrap();

                let mut require_bytes = 0;
                let mut require_count = 0;
                for raw_result in csv_reader.records() {
                    tracing::info!("init reading: {}, {:?}", meta_path, raw_result);
                    let raw_line = raw_result.unwrap();
                    let size = raw_line.get(1).unwrap().parse::<u64>().unwrap();
                    require_count += 1;
                    require_bytes += size;
                }
                tx_sender
                    .send((meta_path.clone(), require_bytes, require_count))
                    .await
                    .unwrap();
                tracing::debug!(
                    "init: use {:?}, {}, {}, {}",
                    start.elapsed(),
                    meta_path,
                    require_bytes,
                    require_count
                );
            });
        }
        drop(tx);
    });

    stop.await.unwrap();
    tracing::info!("reading: use {:?}", start.elapsed());
}
