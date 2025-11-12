use crate::stats::RUNTIME;
use csv::Reader;
use httpdrs_core::httpd;
use tokio::sync::mpsc;
use tokio::time::Instant;

pub(crate) async fn init() {
    let start = Instant::now();

    let meta_path = RUNTIME.lock().unwrap().meta_path.clone();
    let csv_paths = std::fs::read_dir(meta_path.as_str()).unwrap();

    let (tx, mut rx) = mpsc::channel::<(String, u64, u64)>(100);
    for csv_path in csv_paths {
        let tx_sender = tx.clone();
        tokio::spawn(async move {
            let meta_path = csv_path.unwrap().path().to_string_lossy().to_string();
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

    while let Some((_csv, bytes, size)) = rx.recv().await {
        // 执行下载逻辑
        let mut rt = RUNTIME.lock().unwrap();
        rt.require_bytes += bytes;
        rt.require_count += size;
    }
    tracing::info!("reading: use {:?}", start.elapsed());
}

pub(crate) async fn checkpoint() {
    let start = Instant::now();

    let meta_path = RUNTIME.lock().unwrap().meta_path.clone();
    let csv_paths = std::fs::read_dir(meta_path.as_str()).unwrap();

    let (tx, mut rx) = mpsc::channel::<(String, u64, u64)>(100);
    for csv_path in csv_paths {
        let tx_sender = tx.clone();
        tokio::spawn(async move {
            let meta_path = csv_path.unwrap().path().to_string_lossy().to_string();
            let mut csv_reader = Reader::from_path(meta_path.as_str()).unwrap();

            let mut download_bytes = 0;
            let mut download_count = 0;
            for raw_result in csv_reader.records() {
                tracing::debug!("read_checkpoint reading: {}, {:?}", meta_path, raw_result);
                let raw_line = raw_result.unwrap();
                let sign = raw_line.get(0).unwrap().to_string();
                let size = raw_line.get(1).unwrap().parse::<u64>().unwrap();
                let httpd_reader = httpd::reader_parse(sign.clone()).unwrap();
                if let Some(reader_size) =
                    httpd_reader.check_local_file(RUNTIME.lock().unwrap().data_path.as_str())
                {
                    if reader_size != size {
                        tracing::debug!(
                            "read_checkpoint: {} size not match, local_size: {}, meta_size: {}",
                            httpd_reader,
                            reader_size,
                            size
                        );
                        continue;
                    }
                    download_bytes += size;
                    download_count += 1;
                }
            }
            tx_sender
                .send((meta_path.clone(), download_bytes, download_count))
                .await
                .unwrap();
            tracing::debug!(
                "read_checkpoint: use {:?}, {}, {}, {}",
                start.elapsed(),
                meta_path,
                download_bytes,
                download_count
            );
        });
    }
    drop(tx);

    while let Some((_csv, bytes, size)) = rx.recv().await {
        // 执行下载逻辑
        let mut rt = RUNTIME.lock().unwrap();
        rt.completed_bytes += bytes;
        rt.completed_count += size;
    }
    tracing::debug!("checkpoint: use {:?}", start.elapsed());
}
