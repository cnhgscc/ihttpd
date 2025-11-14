use csv::Reader;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::meta;
use crate::stats::RUNTIME;

pub(crate) async fn init(cancel: CancellationToken) {
    let start = Instant::now();

    let meta_path = RUNTIME.lock().unwrap().meta_path.clone();
    let temp_path = RUNTIME.lock().unwrap().temp_path.clone();

    let meta_list = format!("{}/meta.list", temp_path);
    while std::fs::metadata(&meta_list).is_err() {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }

    let (tx_meta, mut rx_meta) = mpsc::channel::<String>(100);
    tokio::spawn(meta::read_meta(meta_list, tx_meta, cancel.clone(), 1));

    let (tx, mut rx) = mpsc::channel::<(String, u64, u64)>(2);

    let stop_wait = cancel.clone();
    let stop = tokio::spawn(async move {
        while let Some((_csv, bytes, size)) = rx.recv().await {
            if stop_wait.is_cancelled() {
                break;
            }
            let mut rt = RUNTIME.lock().unwrap();
            rt.require_bytes += bytes;
            rt.require_count += size;
        }
    });

    let stop_read = cancel.clone();
    tokio::spawn(async move {
        while let Some(meta_name) = rx_meta.recv().await {
            if stop_read.is_cancelled() {
                break;
            }

            let meta_path = format!("{}/{}", meta_path, meta_name);

            let tx_sender = tx.clone();
            tokio::spawn(async move {
                let mut csv_reader = Reader::from_path(meta_path.as_str()).unwrap();

                let mut require_bytes = 0;
                let mut require_count = 0;
                for raw_result in csv_reader.records() {
                    tracing::debug!("init reading: {}, {:?}", meta_path, raw_result);
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
