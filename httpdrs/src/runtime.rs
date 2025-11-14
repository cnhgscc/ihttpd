use std::sync::Arc;
use std::time::Duration;

use crate::core::{httpd, pbar};
use crate::merge::MergeMessage;
use crate::stats::{META, RUNTIME};
use crate::{bandwidth, downloader, merge, reader, watch};
use futures::future::join_all;
use httpdrs_core::httpd::SignatureClient;
use reqwest::Client;
use tokio::runtime;
use tokio::sync::{Semaphore, mpsc};
use tokio_util::sync::CancellationToken;

pub fn start_multi_thread(
    max_bandwidth: u64,
    max_parallel: usize,
    use_loc: String,
    presign_api: String,
    network: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let rt = runtime::Builder::new_multi_thread()
        .worker_threads(100)
        .enable_all()
        .build()
        .unwrap();

    let rt_token = CancellationToken::new();

    RUNTIME.lock().unwrap().meta_path = format!("{}/meta", use_loc);
    RUNTIME.lock().unwrap().data_path = format!("{}/data", use_loc);
    RUNTIME.lock().unwrap().temp_path = format!("{}/temp", use_loc);

    let client_down = Arc::new(
        Client::builder()
            .pool_max_idle_per_host(1000)
            .pool_idle_timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(300))
            .user_agent("baai-downloader")
            .build()
            .expect("failed to build reqwest client"),
    );

    let client_sign = Arc::new(SignatureClient::new(presign_api, network));

    let httpd_bandwidth = httpd::Bandwidth::new(1024 * 1024 * (max_bandwidth + 1)); // 网络带宽控制
    let httpd_jobs = Arc::new(Semaphore::new(max_parallel)); // 下载器并发控制

    // 处理合并的队列
    let (tx_merge, rx_merge) = mpsc::channel::<MergeMessage>(100);

    tracing::info!("Runtime initialized: baai-flagdataset-rs");

    let pb = pbar::create();

    rt.spawn(bandwidth::reset_period(
        Arc::clone(&httpd_bandwidth),
        rt_token.clone(),
    ));
    rt.spawn(watch::init(pb.clone(), rt_token.clone()));

    let spawn_read = rt.spawn(reader::init(rt_token.clone()));
    let spawn_down = rt.spawn(downloader::down(
        Arc::clone(&httpd_bandwidth),
        Arc::clone(&httpd_jobs),
        Arc::clone(&client_down),
        Arc::clone(&client_sign),
        Arc::new(tx_merge),
        rt_token.clone(),
    ));
    let spawn_merge = rt.spawn(merge::init(rx_merge, rt_token.clone()));

    // 等待所以任务处理完成
    rt.block_on(async move {
        #[cfg(unix)]
        let signal_future = async {
            let mut sigint =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
                    .expect("");
            let mut sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("");

            tokio::select! {
                _ = sigint.recv() => {
                    println!("\n收到中断信号 (Ctrl+C)，正在退出...");
                },
                _ = sigterm.recv() => {
                    println!("\n收到终止信号，正在退出...");
                }
            }
        };

        #[cfg(windows)]
        let signal_future = async {
            tokio::signal::ctrl_c()
                .await
                .expect("Failed to install Ctrl+C handler");
            println!("\n收到 Ctrl+C 信号，正在退出...");
        };

        let event_tasks = vec![spawn_read, spawn_down, spawn_merge];
        tokio::select! {
            _ = join_all(event_tasks) => {
                let (runtime_require_bytes, runtime_require_count, runtime_download_speed) = {
                    let r = RUNTIME.lock().unwrap();
                    (r.require_bytes, r.require_count, r.download_speed)
                };
                pb.set_message(pbar::format(
                    runtime_require_bytes,
                    runtime_require_count,
                    runtime_download_speed,
                    1.0,
                ));

                pb.finish();

            }
            _ = signal_future => {
                rt_token.cancel();
            }
        }
    });

    META.lock().unwrap().iter().for_each(|(k, v)| {
        tracing::info!("download_meta: {} = {}", k, v);
    });
    rt.shutdown_background();

    Ok(())
}
