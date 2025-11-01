use std::sync::{Arc, Mutex, LazyLock};
use csv::Reader;
use tokio::runtime;
use tokio::sync::{Semaphore, mpsc};
use futures::future::join_all;

use httpdrs_core:: {httpd, io};
use httpdrs_core::httpd::jwtsign::{jwtsign};

#[allow(dead_code)]
#[derive(Debug, Default)]
pub struct EventReader{
    pub total_size: i64,
    pub total_count: i64,

    pub download_size: u64,
    pub download_count: u64,
    pub download_err_count: u64,
}

static RUNTIME_STATS: LazyLock<Arc<Mutex<EventReader>>> =
    LazyLock::new(|| Arc::new(Mutex::new(EventReader::default())));


pub fn start_multi_thread() -> Result<(), Box<dyn std::error::Error>>{

    let rt = runtime::Builder::new_multi_thread()
        .worker_threads(100)
        .enable_all()
        .build()
        .unwrap();

    tracing::info!("Runtime initialized: baai-flagdataset-rs");


    let spawn_reader = rt.spawn(async {
        let mut csv_reader = io::CSVMetaReader::new("/Users/hgshicc/test/flagdataset/AIM-500/meta".to_string());
        let (lines, bytes) = csv_reader.init().await.unwrap();
        tracing::info!("CSVReader initialized: baai-flagdataset-rs: {}", csv_reader);
        RUNTIME_STATS.lock().unwrap().total_size = bytes;
        RUNTIME_STATS.lock().unwrap().total_count = lines;
    });

    let spawn_filter = rt.spawn(async {
        // 获取已经下载的文件信息

        let semaphore = Arc::new(Semaphore::new(100));
        let csv_reader = io::CSVMetaReader::new("/Users/hgshicc/test/flagdataset/AIM-500/meta".to_string());

        let data_path = "/Users/hgshicc/test/flagdataset/AIM-500/data";

        let mut jobs = Vec::new();
        let _ = csv_reader.read_meta(&mut |meta_path: String| {
            let se = semaphore.clone();
            let job = tokio::spawn(async move {
                let _permit = se.acquire().await.unwrap();
                let _ = io::read_meta_bin(meta_path.as_str(), &mut |sign, size, _extn |{
                    tracing::debug!("reading: {}", sign);
                    let httpd_reader  = jwtsign(sign).unwrap();
                    if let Some(reader_size) =  httpd_reader.check_local_file(data_path){
                        if reader_size != size as u64 {
                            tracing::warn!("file size mismatch: {}", httpd_reader);
                            RUNTIME_STATS.lock().unwrap().download_err_count += 1;
                            return
                        }
                        RUNTIME_STATS.lock().unwrap().download_size += reader_size;
                        RUNTIME_STATS.lock().unwrap().download_count += 1;
                    }

                }).await;
            });
            jobs.push(job);
            true
        }).await;

        join_all(jobs).await;
        tracing::info!("reading: Done");
    });

    let spawn_download = rt.spawn(async {

        let data_path = "/Users/hgshicc/test/flagdataset/AIM-500/data";
        let csv_meta_reader = io::CSVMetaReader::new("/Users/hgshicc/test/flagdataset/AIM-500/meta".to_string());

        let (tx, mut rx) = mpsc::channel::<(String, u64)>(100);
        tokio::spawn(async move {
            let paths = std::fs::read_dir(csv_meta_reader.meta_path.as_str()).unwrap();

            // 控制并发
            let semaphore = Arc::new(Semaphore::new(100));
            for path in paths {
                let tx_sender = tx.clone();
                let se = semaphore.clone();

                tokio::spawn(async move {
                    let _permit = se.acquire().await.unwrap();
                    let meta_path =  path.unwrap().path().to_string_lossy().to_string();
                    let mut csv_reader = Reader::from_path(meta_path.as_str()).unwrap();
                    for raw_result in csv_reader.records(){
                        let raw_line = raw_result.unwrap();
                        let sign = raw_line.get(0).unwrap().to_string();
                        let size = raw_line.get(1).unwrap().parse::<u64>().unwrap();

                        let httpd_reader  = jwtsign(sign.clone()).unwrap();
                        if let Some(reader_size) =  httpd_reader.check_local_file(data_path){
                            if reader_size == size{
                                continue
                            }
                            tx_sender.send((sign.clone(), size)).await.unwrap();
                        }

                    }
                });

            }
        });

        while let Some((sign, size)) = rx.recv().await {
            // 执行下载逻辑
            tokio::spawn(async move {
                tracing::info!("downloading: {}", sign);
            });
        }
        tracing::info!("downloading: Done");
    });


    let event_tasks = vec![spawn_reader, spawn_filter, spawn_download];

    let _ = rt.block_on(join_all(event_tasks));
    rt.shutdown_background();

    tracing::info!("Runtime shutdown: baai-flagdataset-rs: {:?}", RUNTIME_STATS);

    Ok(())
}