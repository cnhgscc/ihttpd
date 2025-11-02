use std::sync::Arc;
use tokio::fs;
use tokio::sync::{mpsc};
use tokio::time::Instant;

use csv::Reader;

use httpdrs_core::{httpd};
use httpdrs_core::httpd::HttpdMetaReader;
use httpdrs_core::httpd::Bandwidth;
use crate::stats::RUNTIME;

pub(crate) async fn down(bandwidth: Arc<Bandwidth>) {
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
        });
    }
    drop(tx);

    while let Some((meta_path, sign, size)) = rx.recv().await {
        // 执行下载逻辑
        let chunk_size = 1024 * 1024 * 5;
        download(Arc::clone(&bandwidth), sign, size,chunk_size).await;

    }
    tracing::info!("download: use {:?}", start.elapsed());
}


async fn download(bandwidth_ref: Arc<Bandwidth>, sign: String, require_size: u64, _chunk_size: i32){
    let chunk_size = 1024 * 1024 * 5;


    let data_path = {
        RUNTIME.lock().unwrap().data_path.clone() // 提前获取并释放锁
    };
    let temp_path = {
        RUNTIME.lock().unwrap().temp_path.clone() // 提前获取并释放锁
    };

    let reader_ref = Arc::new(httpd::reader_parse(sign.clone()).unwrap());
    let local_path =  reader_ref.local_absolute_path_str(data_path.as_str());
    tracing::info!("download, sign: {} -> {:?}", reader_ref, reader_ref.local_absolute_path_str(data_path.as_str()));

    let local_size = httpd::check_file_meta(local_path.clone()).unwrap();
    if local_size == require_size {
        return
    }else {
        tracing::info!("download, start: {}, local: {}, require: {}", reader_ref, local_size, require_size);
    }

    let total_parts =  (require_size + chunk_size - 1) / chunk_size;

    // 每个文件都创建一下分片下载的最大检查队列
    let (tx_part, mut rx_part) = mpsc::channel::<(u64, u128)>(100);
    let reader_merge = Arc::clone(&reader_ref);
    tokio::spawn(async move {
        while let Some((_idx_part,  _use_ms)) = rx_part.recv().await {
        }

        // 下载完毕触发合并
        tracing::info!("download, merge: {}, {:?}", reader_merge, local_path.clone());

    });

    tracing::info!("download, parts: {}, {}", total_parts, reader_ref);

    for idx_part in 0..total_parts {
        let part_start = idx_part * chunk_size;
        let part_end = (idx_part + 1) * chunk_size;
        let part_end = if part_end > require_size { require_size } else { part_end };
        let part_size = part_end - part_start;
        let reader_ = Arc::clone(&reader_ref);
        let bandwidth_ = Arc::clone(&bandwidth_ref);
        let tx_part_ = tx_part.clone();
        tokio::spawn(async move {
            let _ = bandwidth_.permit(part_size); // 带宽控制
            let use_ms = download_part(reader_, idx_part, part_start, part_end, part_size).await.ok();
            tx_part_.send((idx_part, use_ms.unwrap())).await.unwrap();
        });
    }
    drop(tx_part);


    let presign_url =  presign(sign).await.unwrap();
    tracing::info!("download, presign: {}, {}, {}", presign_url, data_path, temp_path);
}

async fn presign(sign: String) -> Result<String, Box<dyn std::error::Error>>{
    let start = Instant::now();
    let chttp = httpd::SignatureClient::new();
    let reader = chttp.reader_get(sign).await.unwrap();
    if reader.code != 0 {
        return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "status_code != 200")))
    }
    tracing::info!("download, presign->use {:?}", start.elapsed());
    Ok(reader.data.endpoint)
}


pub async fn download_part (
    reader_ref: Arc<HttpdMetaReader>,
    idx_part: u64,
    start_pos: u64,
    end_pos: u64,
    part_size: u64,
) -> Result<u128, Box<dyn std::error::Error>>
{
    let start = Instant::now();



    Ok(start.elapsed().as_millis())
}

pub async  fn download_merge (
    reader: HttpdMetaReader,
    chunk_nums: u64,
    data_path: &str,
    temp_path: &str,
) -> Result<u128, Box<dyn std::error::Error>>
{

    let start = Instant::now();

    let file_path = reader.local_absolute_path_str(data_path);
    // let _ext = Path::new(file_path).extension().and_then(|s| s.to_str()).unwrap();

    let _ = tokio::fs::remove_file(file_path.clone()).await.unwrap_or( ());

    let mut dest_file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(file_path)
        .await?;

    for idx_part in 0..chunk_nums {
        let part_path = reader.local_part_path(data_path, idx_part, temp_path);
        let mut part_file = fs::File::open(part_path).await?;
        tokio::io::copy(&mut part_file, &mut dest_file).await?;
    }
    for idx_part in 0..chunk_nums {
        let part_path = reader.local_part_path(data_path, idx_part, temp_path);
        let _ = tokio::fs::remove_file(part_path).await.unwrap_or( ());
    }

    Ok(start.elapsed().as_millis())
}
