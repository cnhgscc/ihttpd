use csv::Reader;
use tokio::sync::{mpsc};
use tokio::time::Instant;
use httpdrs_core::{httpd};
use crate::stats::RUNTIME;

pub(crate) async fn down() {
    let start = Instant::now();

    let meta_path = RUNTIME.lock().unwrap().meta_path.clone();
    let csv_paths = std::fs::read_dir(meta_path.as_str()).unwrap();

    let (tx, mut rx) = mpsc::channel::<(String, String, u64)>(100);
    for csv_path in csv_paths {
        let tx_sender = tx.clone();
        let data_path = {
            RUNTIME.lock().unwrap().data_path.clone() // 提前获取并释放锁
        };
        tokio::spawn(async move {
            let meta_path =  csv_path.unwrap().path().to_string_lossy().to_string();
            let mut csv_reader = Reader::from_path(meta_path.as_str()).unwrap();

            for raw_result in csv_reader.records(){
                tracing::debug!("init reading: {}, {:?}", meta_path, raw_result);
                let raw_line = raw_result.unwrap();
                let sign = raw_line.get(0).unwrap().to_string();
                let size = raw_line.get(1).unwrap().parse::<u64>().unwrap();
                let httpd_reader = httpd::reader_parse(sign.clone()).unwrap();
                if let Some(reader_size) = httpd_reader.check_local_file(data_path.as_str()) {
                    if reader_size == size {
                        continue
                    }
                    tx_sender.send((meta_path.clone(), sign, size)).await.unwrap();
                }
            }
            tracing::info!("down_checkpoint: use {:?}, {}", start.elapsed(), meta_path);
        });
    }
    drop(tx);

    while let Some((meta_path, sign, size)) = rx.recv().await {
        // 执行下载逻辑
        tracing::info!("download: {}, {}, {}", meta_path, sign, size);
    }
    tracing::info!("download: use {:?}", start.elapsed());
}