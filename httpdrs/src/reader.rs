use std::sync::Arc;
use csv::Reader;
use futures::future::join_all;
use tokio::sync::Semaphore;
use httpdrs_core::{httpd};
use crate::stats::RUNTIME;

pub(crate) async fn init(){
    let mut jobs = Vec::new();
    let semaphore = Arc::new(Semaphore::new(100));

    let mut csv_reader = Reader::from_path(RUNTIME.lock().unwrap().meta_path.clone()).unwrap();
    for raw_result in csv_reader.records(){
        let sa = semaphore.clone();
        let job = tokio::spawn(async move {
            let _permit = sa.acquire().await.unwrap();
            let raw_line = raw_result.unwrap();
            let sign = raw_line.get(0).unwrap().to_string();
            let size = raw_line.get(1).unwrap().parse::<u64>().unwrap();
            RUNTIME.lock().unwrap().request_bytes += size;
            RUNTIME.lock().unwrap().request_count += 1;

        });
        jobs.push(job);
    }
    join_all(jobs).await;
    tracing::info!("init reading: Done");
}

pub(crate) async fn checkpoint(){
    let mut jobs = Vec::new();
    let semaphore = Arc::new(Semaphore::new(100));

    let mut csv_reader = Reader::from_path(RUNTIME.lock().unwrap().meta_path.clone()).unwrap();
    for raw_result in csv_reader.records(){
        let sa = semaphore.clone();
        let job = tokio::spawn(async move {
            let _permit = sa.acquire().await.unwrap();
            let raw_line = raw_result.unwrap();
            let sign = raw_line.get(0).unwrap().to_string();
            let size = raw_line.get(1).unwrap().parse::<u64>().unwrap();
            let httpd_reader  = httpd::reader_parse(sign).unwrap();
            if let Some(reader_size) =  httpd_reader.check_local_file(RUNTIME.lock().unwrap().data_path.as_str()){
                if reader_size != size {
                    tracing::warn!("checkpoint mismatch: {}", httpd_reader);
                    return
                }
                RUNTIME.lock().unwrap().download_bytes += reader_size;
                RUNTIME.lock().unwrap().download_count += 1;
            }
        });
        jobs.push(job);
    }
    join_all(jobs).await;
    tracing::info!("checkpoint reading: Done");
}
