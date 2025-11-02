use tokio::fs;
use tokio::sync::{mpsc};
use tokio::time::Instant;

use csv::Reader;

use httpdrs_core::{httpd};
use httpdrs_core::httpd::HttpdMetaReader;
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
        });
    }
    drop(tx);

    while let Some((meta_path, sign, size)) = rx.recv().await {
        // 执行下载逻辑
        let chunk_size = 1024 * 1024 * 5;
        download(sign, size,chunk_size).await;

    }
    tracing::info!("download: use {:?}", start.elapsed());
}


async fn download(sign: String, _size: u64, _chunk_size: i32){
    let data_path = {
        RUNTIME.lock().unwrap().data_path.clone() // 提前获取并释放锁
    };
    let temp_path = {
        RUNTIME.lock().unwrap().temp_path.clone() // 提前获取并释放锁
    };

    let reader = httpd::reader_parse(sign.clone()).unwrap();
    tracing::info!("download, sign: {}", reader);

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
