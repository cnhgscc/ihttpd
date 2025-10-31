use std::sync::{Arc, Mutex, LazyLock};
use csv::Reader;
use tokio::runtime;
use tokio::sync::{Semaphore, mpsc};
use futures::future::join_all;

use crate::reader;
use crate::reader::read_meta_bin;

#[allow(dead_code)]
#[derive(Debug, Default)]
pub struct EventReader{
    pub total_size: i64,
    pub total_count: i64,
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
        let mut csv_reader = reader::CSVMetaReader::new("/Users/hgshicc/test/flagdataset/AIM-500/meta".to_string());
        let (lines, bytes) = csv_reader.init().await.unwrap();
        tracing::info!("CSVReader initialized: baai-flagdataset-rs: {}", csv_reader);
        RUNTIME_STATS.lock().unwrap().total_size = bytes;
        RUNTIME_STATS.lock().unwrap().total_count = lines;
    });

    let spawn_filter = rt.spawn(async {
        // 获取已经下载的文件信息

        let semaphore = Arc::new(Semaphore::new(100));
        let csv_reader = reader::CSVMetaReader::new("/Users/hgshicc/test/flagdataset/AIM-500/meta".to_string());

        let mut jobs = Vec::new();
        let _ = csv_reader.read_meta(&mut |meta_path: String| {
            let se = semaphore.clone();
            let job = tokio::spawn(async move {
                let _permit = se.acquire().await.unwrap();
                let _ = read_meta_bin(meta_path.as_str(), &mut |sign, _size, _extn |{
                    tracing::debug!("reading: {}", sign);
                }).await;
            });
            jobs.push(job);
            true
        }).await;

        join_all(jobs).await;
        tracing::info!("reading: Done");
    });

    let spawn_download = rt.spawn(async {

        let csv_meta_reader = reader::CSVMetaReader::new("/Users/hgshicc/test/flagdataset/AIM-500/meta".to_string());

        let paths = std::fs::read_dir(csv_meta_reader.meta_path.as_str()).unwrap();
        for path in paths {
            let meta_path =  path.unwrap().path().to_string_lossy().to_string();
            let mut csv_reader = Reader::from_path(meta_path.as_str()).unwrap();

            for raw_result in csv_reader.records(){
                let raw_line = raw_result.unwrap();
                let sign = raw_line.get(0).unwrap();
                let _size = raw_line.get(1).unwrap().parse::<i64>().unwrap();
                let s= httpdrs_sign::SignatureClient::new();
                let reader_endpoint = s.reader_get(sign.to_string()).await.unwrap();
                print!("{:?}", reader_endpoint);
                break
            }
            break
        }

    });


    let event_tasks = vec![spawn_reader, spawn_filter, spawn_download];

    let _ = rt.block_on(join_all(event_tasks));
    rt.shutdown_background();

    tracing::info!("Runtime shutdown: baai-flagdataset-rs");

   Ok(())
}