use std::sync::Arc;
use tokio::fs;
use tokio::sync::{mpsc, Semaphore};
use tokio::time::Instant;
use tokio::io::AsyncWriteExt;
use reqwest::Client;
use reqwest::header::{RANGE};

use csv::Reader;
use httpdrs_core::{httpd};
use httpdrs_core::httpd::{HttpdMetaReader, SignatureClient};
use httpdrs_core::httpd::Bandwidth;
use crate::stats::RUNTIME;

pub(crate) async fn down(bandwidth: Arc<Bandwidth>, client_down: Arc<Client>, client_sign: Arc<SignatureClient>) {

    let meta_path = RUNTIME.lock().unwrap().meta_path.clone();
    let data_path = RUNTIME.lock().unwrap().data_path.clone();
    let csv_paths = std::fs::read_dir(meta_path.as_str()).unwrap();

    // 检查未下载并发控制
    let (tx, mut rx) = mpsc::channel::<(String, String, u64)>(10);
    for csv_path in csv_paths {
        let tx_sender = tx.clone();
        let data_path = data_path.clone();
        tokio::spawn(async move {
            let csv_meta_path =  csv_path.unwrap().path().to_string_lossy().to_string();
            let mut csv_reader = Reader::from_path(csv_meta_path.as_str()).unwrap();

            for raw_result in csv_reader.records(){
                let raw_line = raw_result.unwrap();
                let sign = raw_line.get(0).unwrap().to_string();
                let size = raw_line.get(1).unwrap().parse::<u64>().unwrap();
                let httpd_reader = httpd::reader_parse(sign.clone()).unwrap();
                if let Some(reader_size) = httpd_reader.check_local_file(data_path.as_str()) {
                    if reader_size == size {
                        continue
                    }
                    tx_sender.send((csv_meta_path.clone(), sign, size)).await.unwrap();
                }else {
                    tx_sender.send((csv_meta_path.clone(), sign, size)).await.unwrap();
                }
            }
        });
    }
    drop(tx);

    let semaphore = Arc::new(Semaphore::new(500)); // 文件下载并发控制
    let (tx_down, mut rx_down) = mpsc::channel::<(String, tokio::time::Duration)>(100);
    let chunk_size = 1024 * 1024 * 5;
    while let Some((_meta_path, sign, size)) = rx.recv().await {
        let bandwidth_ = Arc::clone(&bandwidth);
        let client_down_ = Arc::clone(&client_down);
        let client_sign_ = Arc::clone(&client_sign);
        let tx_down_ = tx_down.clone();
        let semaphore_ = Arc::clone(&semaphore);
        tokio::spawn(async move {
            let _permit = semaphore_.acquire().await.unwrap();
            let (download_name, download_duration) = match download(
                bandwidth_,
                client_down_,
                client_sign_,
                sign,
                size,
                chunk_size
            ).await{
                Ok((download_name, download_duration)) => {
                    (download_name, Option::from(download_duration))
                }
                Err(e) => {
                    (e.to_string(), None)
                }
            };

           match download_duration {
               Some(download_duration) => {
                   tx_down_.send((download_name, download_duration)).await.unwrap();
               }
               None => {
               }
           }

        });
    }
    drop(tx_down);
    while let Some((name, use_ms)) = rx_down.recv().await {
        tracing::info!("download_complete, use: {:?}, file: {:?}", use_ms, name);
    } // 下载任务处理完成
}


async fn download(
    bandwidth: Arc<Bandwidth>,
    client_down: Arc<Client>,
    client_sign: Arc<SignatureClient>,
    sign: String,
    require_size: u64,
    chunk_size: u64
) -> Result<(String, tokio::time::Duration), Box<dyn std::error::Error>>
{
    let start = Instant::now();

    let data_path = Arc::new({
        RUNTIME.lock().unwrap().data_path.clone() // 提前获取并释放锁
    });
    let temp_path = Arc::new({
        RUNTIME.lock().unwrap().temp_path.clone() // 提前获取并释放锁
    });

    let reader_ref = Arc::new(httpd::reader_parse(sign.clone())?);
    let local_path =  reader_ref.local_absolute_path_str(data_path.as_str());
    tracing::debug!("download, sign: {} -> {:?}", reader_ref, reader_ref.local_absolute_path_str(data_path.as_str()));

    if let Some(local_size) = httpd::check_file_meta(local_path.clone()){
        if local_size == require_size {
            return Ok((reader_ref.local_relative_path().to_string_lossy().to_string(), start.elapsed()));
        }else {
            tracing::debug!("download, start: {}, local: {}, require: {}", reader_ref, local_size, require_size);
        }
    }


    let total_parts =  (require_size + chunk_size - 1) / chunk_size;

    // 每个文件都创建一下分片下载的最大检查队列
    let (tx_part, mut rx_part) = mpsc::channel::<(u64, u128, i32)>(100);
    let reader_merge = Arc::clone(&reader_ref);

    tracing::debug!("download, parts: {}, {}", total_parts, reader_ref);

    for idx_part in 0..total_parts {
        let part_start = idx_part * chunk_size;
        let part_end = (idx_part + 1) * chunk_size;
        let part_end = if part_end > require_size { require_size } else { part_end };
        let part_size = part_end - part_start;
        let reader_ = Arc::clone(&reader_ref);
        let bandwidth_ = Arc::clone(&bandwidth);
        let tx_part_ = tx_part.clone();
        let sign_ = sign.clone();
        let data_path_ = Arc::clone(&data_path);
        let temp_path_ = Arc::clone(&temp_path);
        let client_down_span = Arc::clone(&client_down);
        let client_sign_span = Arc::clone(&client_sign);
        tokio::spawn(async move {
            let _ = bandwidth_.permit(part_size).await; // 带宽控制

            let (download_len, download_signal)=  match download_part(
                client_down_span,
                client_sign_span,
                reader_,
                idx_part,
                part_start,
                part_end,
                part_size,
                sign_,
                data_path_.as_str(),
                temp_path_.as_str(),
            ).await{
                Ok(resp_len) => {
                    (resp_len, 1)
                }
                Err(e) => {
                    tracing::error!("download_part, error: {}, part: {:?}", e, idx_part);
                    (0, 0)
                }
            };


            tx_part_.send((idx_part, download_len, download_signal)).await.unwrap();
        });
    }
    drop(tx_part);

    let mut completed_parts = 0;
    while let Some((idx_part,  download_len, download_signal)) = rx_part.recv().await {
        completed_parts += 1;
        RUNTIME.lock().unwrap().download_bytes += download_len as u64;
        tracing::info!("download_part, complete {}, use: {}  part: {:?}, {}", reader_merge, download_len, idx_part, download_signal);
    }

    // 下载完毕触发合并
    if completed_parts == total_parts {
        match download_merge(Arc::clone(&reader_merge), total_parts, data_path.as_str(), temp_path.as_str()).await{
            Ok(use_ms) => {
                tracing::info!("download_merge, use: {:?}", use_ms);
            },
            Err(e) => {
                tracing::error!("download_merge, error: {}", e);
                return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "download_merge error")))
            }
        };
    }
    Ok((reader_ref.local_relative_path().to_string_lossy().to_string(), start.elapsed()))
}

async fn presign(sign: String, with_client: Arc<SignatureClient>) -> Result<String, Box<dyn std::error::Error>>{
    let start = Instant::now();
    let reader = match  with_client.reader_get(sign).await{
        Ok(reader) => {
            reader
        },
        Err(err) => {
            return Err(format!("download_err, reader_presign err: {}", err).into());
        }
    };
    if reader.code != 0 {
        return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, "status_code != 200")))
    }
    tracing::info!("download, presign->use {:?}", start.elapsed());
    Ok(reader.data.endpoint)
}


pub async fn download_part (
    client_down: Arc<Client>,
    client_sign: Arc<SignatureClient>,
    reader_ref: Arc<HttpdMetaReader>,
    idx_part: u64,
    start_pos: u64,
    end_pos: u64,
    _part_size: u64,
    sign: String,
    data_path: &str,
    temp_path: &str,
) -> Result<u128, Box<dyn std::error::Error>>
{
    let start = Instant::now();


    let presign_url =  match presign(sign.clone(), client_sign).await{
        Ok(presign_url) => {
            presign_url
        },
        Err(err) => {
            tracing::error!("download_err, presign err: {}", err);
            return Err(format!("download_err, presign err: {}", err).into());
        }
    };

    let presign_url = match presign_url.as_str() {
        "" => {
            tracing::error!("download_err, presign_url is empty");
            return Err("download_err, presign_url is empty".into());
        },
        _ => {
            presign_url
        }
    };

    let part_path = reader_ref.local_part_path(data_path, idx_part, temp_path);
    let range = format!("bytes={}-{}", start_pos, end_pos);
    tracing::info!("download_part, presign: {}, use: {:?}", presign_url, start.elapsed());


    let max_retries = 20;
    let mut retry_count = 0;
    let resp_part = loop {
        let resp_part = client_down.get(presign_url.clone()).header(RANGE, range.clone()).send().await;
        match resp_part {
            Ok(resp_part) => {
                match resp_part.status().as_u16() {
                    200..=299 => {
                        break resp_part;
                    },
                    _ => {
                        retry_count += 1;
                        if retry_count >= max_retries {
                            return Err(format!("download_err, resp status is {}, {}", resp_part.status(), presign_url).into());
                        }
                        tokio::time::sleep(tokio::time::Duration::from_millis(100 * retry_count)).await;
                    }
                }
            },
            Err(err) => {
                retry_count += 1;
                if retry_count >= max_retries {
                    return Err(format!("download_err, reqwest_retry: {}  err: {}", retry_count, err).into());
                }
                tokio::time::sleep(tokio::time::Duration::from_millis(2000 * retry_count)).await;
            }
        }
    };

    let resp_byte = match  resp_part.bytes().await.ok(){
        Some(resp_bytes) => {
            resp_bytes
        },
        None => {
            return Err("download_err, to bytes err".into());
        }
    };

    if let Some(parent) = std::path::Path::new(&part_path).parent() {
        if !parent.exists() {
            fs::create_dir_all(parent).await?;
        }
    }
    let mut file = match fs::File::create(part_path.clone()).await {
        Ok(file) => file,
        Err(err) => {
            return Err(format!("download_err, file create err, {:?}", err).into());
        }
    };

    let resp_len = match file.write_all(&resp_byte).await{
        Ok(_) => {
            resp_byte.len()
        },
        Err(err) => {
            return Err(format!("download_err, write_all err: {}", err).into());
        }
    };

    // TODO: 计算分块的平均速度
    tracing::info!(
        "download_part: completed: ({}), start_pos {}, end_pos: {}, use: {:?}, path: {:?}",
        resp_len,
        start_pos,
        end_pos,
        start.elapsed(),
        part_path);
    Ok(resp_len as u128)
}

pub async  fn download_merge (
    reader: Arc<HttpdMetaReader>,
    chunk_nums: u64,
    data_path: &str,
    temp_path: &str,
) -> Result< tokio::time::Duration, Box<dyn std::error::Error>>
{

    let start = Instant::now();

    let file_path = reader.local_absolute_path_str(data_path);
    let _ = tokio::fs::remove_file(file_path.clone()).await.unwrap_or( ());

    if let Some(parent) = std::path::Path::new(&file_path).parent() {
        if !parent.exists() {
            fs::create_dir_all(parent).await?;
        }
    }
    let mut dest_file = match fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(file_path.clone())
        .await{
        Ok(dest_file) => dest_file,
        Err(err) => {
            return Err(format!("download_merge, when open dest file, encountered en err: {}, file: {:?}", err, file_path.clone()).into());
        }
    };

    for idx_part in 0..chunk_nums {
        let part_path = reader.local_part_path(data_path, idx_part, temp_path);
        let mut part_file = match fs::File::open(part_path.clone()).await{
            Ok(part_file) => part_file,
            Err(err) => {
                return Err(format!("download_merge, when open part file, encountered an err: {}，part: {:?}", err, part_path.clone()).into());
            }
        };
        tokio::io::copy(&mut part_file, &mut dest_file).await?;
    }
    for idx_part in 0..chunk_nums {
        let part_path = reader.local_part_path(data_path, idx_part, temp_path);
        let _ = tokio::fs::remove_file(part_path).await.unwrap_or( ());
    }

    RUNTIME.lock().unwrap().download_count += 1;

    Ok(start.elapsed())
}
