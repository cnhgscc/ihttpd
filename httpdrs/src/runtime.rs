use std::sync::{Arc};
use std::time::Duration;

use tokio::runtime;
use tokio_util::sync::CancellationToken;
use reqwest::Client;
use futures::future::join_all;
use httpdrs_core::httpd::SignatureClient;
use crate::core::{httpd, pbar};
use crate::{bandwidth, downloader, reader, watch};
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
    RUNTIME.lock().unwrap().temp_path = "/Users/hgshicc/test/flagdataset/AIM-500/temp".to_string();

    let client_down = Arc::new(Client::builder()
        .pool_max_idle_per_host(1000)
        .pool_idle_timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(300))
        .user_agent("WiSearch Downloader")
        .build()
        .expect("Failed to build reqwest client"));

    let client_sign = Arc::new(SignatureClient::new("http://127.0.0.1:30000/v1/storage/download/presign".to_string()));

    tracing::info!("Runtime initialized: baai-flagdataset-rs");


    let pb = pbar::create();


    let httpd_bandwidth =  httpd::Bandwidth::init(1024*1024*20); // 网络带宽控制
    rt.spawn(bandwidth::reset_period(Arc::clone(&httpd_bandwidth),  rt_token.clone()));
    // rt.spawn(watch::init(pb.clone(), rt_token.clone()));

    let spawn_read = rt.spawn(reader::init());
    let spawn_check = rt.spawn(reader::checkpoint());
    let spawn_down = rt.spawn(
        downloader::down(
            Arc::clone(&httpd_bandwidth),
            Arc::clone(&client_down),
            Arc::clone(&client_sign))
    );
    let event_tasks = vec![spawn_read, spawn_check, spawn_down];

    let _ = rt.block_on(join_all(event_tasks));
    rt.shutdown_background();

    rt_token.cancel();
    tracing::info!("Runtime shutdown: baai-flagdataset-rs: {:?}", RUNTIME);
    let (
        runtime_require_bytes, runtime_require_count, runtime_download_speed
    ) = {
        let r = RUNTIME.lock().unwrap();
        (r.require_bytes, r.require_count, r.download_speed)
    };

    pb.set_message(pbar::format(runtime_require_bytes, runtime_require_count, runtime_download_speed));
    pb.finish();

    Ok(())
}