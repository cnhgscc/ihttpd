use std::sync::{Arc};
use csv::Reader;
use tokio::runtime;
use tokio::sync::{Semaphore, mpsc};
use tokio_util::sync::CancellationToken;
use futures::future::join_all;

use crate::core::{httpd, io, pbar};
use crate::{bandwidth, reader};
use crate::stats::RUNTIME;


pub fn start_multi_thread() -> Result<(), Box<dyn std::error::Error>>{

    let rt = runtime::Builder::new_multi_thread()
        .worker_threads(100)
        .enable_all()
        .build()
        .unwrap();

    let rt_token = CancellationToken::new();

    RUNTIME.lock().unwrap().meta_path = "/Users/hgshicc/test/flagdataset/AIM-500/meta".to_string();
    RUNTIME.lock().unwrap().data_path = "/Users/hgshicc/test/flagdataset/AIM-500/data".to_string();

    tracing::info!("Runtime initialized: baai-flagdataset-rs");

    let pb = pbar::create();


    let httpd_bandwidth =  httpd::Bandwidth::init(1024*1024*100);
    rt.spawn(bandwidth::reset_period(Arc::clone(&httpd_bandwidth),  rt_token.clone()));

    let spawn_reader = rt.spawn(reader::init());

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

                        let httpd_reader  = httpd::reader_parse(sign.clone()).unwrap();
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

        while let Some((sign, _size)) = rx.recv().await {
            // 执行下载逻辑
            tokio::spawn(async move {
                tracing::info!("downloading: {}", sign);
            });
        }
        tracing::info!("downloading: Done");
    });


    let event_tasks = vec![spawn_reader, spawn_download];

    let _ = rt.block_on(join_all(event_tasks));
    rt.shutdown_background();

    rt_token.cancel();
    tracing::info!("Runtime shutdown: baai-flagdataset-rs: {:?}", RUNTIME);
    pb.finish();

    Ok(())
}