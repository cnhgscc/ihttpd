use std::sync::Arc;
use csv::Reader;
use futures::future::join_all;
use tokio::sync::{mpsc, Semaphore};
use tokio::time::Instant;
use httpdrs_core::{httpd};
use crate::stats::RUNTIME;

pub(crate) async fn init(){
    // 增加运行时间统计
    let start = Instant::now();

    let meta_path = RUNTIME.lock().unwrap().meta_path.clone();
    let csv_paths = std::fs::read_dir(meta_path.as_str()).unwrap();

    let (tx, mut rx) = mpsc::channel::<(String, u64, u64)>(100);
    for csv_path in csv_paths {
        let tx_sender = tx.clone();
        tokio::spawn(async move {
            let meta_path =  csv_path.unwrap().path().to_string_lossy().to_string();
            let mut csv_reader = Reader::from_path(meta_path.as_str()).unwrap();

            let mut require_bytes = 0;
            let mut require_count = 0;
            for raw_result in csv_reader.records(){
                tracing::debug!("init reading: {}, {:?}", meta_path, raw_result);
                let raw_line = raw_result.unwrap();
                let size = raw_line.get(1).unwrap().parse::<u64>().unwrap();
                require_count += 1;
                require_bytes += size;
            }
            tx_sender.send((meta_path.clone(), require_bytes, require_count)).await.unwrap();
            tracing::info!("init reading: use {:?}, {}", start.elapsed(), meta_path);
        });
    }
    drop(tx);

    while let Some((csv, bytes, size)) = rx.recv().await {
        // 执行下载逻辑
        let mut rt = RUNTIME.lock().unwrap();
        rt.require_bytes += bytes;
        rt.require_count += size;
    }
    tracing::info!("init reading: use {:?}", start.elapsed());
}

pub(crate) async fn checkpoint() {
    let csv_paths = std::fs::read_dir(RUNTIME.lock().unwrap().meta_path.clone()).unwrap();

    let mut jobs = Vec::new();
    let semaphore = Arc::new(Semaphore::new(100));
    for csv_path in csv_paths {
        let se = semaphore.clone();
        let job = tokio::spawn(async move {
            let _permit = se.acquire().await.unwrap();
            let meta_path = csv_path.unwrap().path().to_string_lossy().to_string();
            let mut csv_reader = Reader::from_path(meta_path.as_str()).unwrap();
            for raw_result in csv_reader.records() {
                let raw_line = raw_result.unwrap();
                let sign = raw_line.get(0).unwrap().to_string();
                let size = raw_line.get(1).unwrap().parse::<u64>().unwrap();
                let httpd_reader = httpd::reader_parse(sign.clone()).unwrap();
                if let Some(reader_size) = httpd_reader.check_local_file(RUNTIME.lock().unwrap().data_path.as_str()) {
                    if reader_size != size {
                        continue
                    }
                    RUNTIME.lock().unwrap().download_bytes += size;
                    RUNTIME.lock().unwrap().download_count += 1;
                }
            }
        });
        jobs.push(job);
    }
    join_all(jobs).await;
}