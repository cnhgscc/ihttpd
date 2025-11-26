use crate::reader::{ReaderRequest, ReaderResponse};

pub mod jwtsign;
pub mod reader;

pub struct SignatureClient {
    client: reqwest::Client,
    network: String,
    reader_presign: String,
}

impl SignatureClient {
    pub fn new(reader_presign: String, network: String) -> Self {
        let client = reqwest::Client::new();
        SignatureClient {
            client,
            network,
            reader_presign: reader_presign.to_string(),
        }
    }
    pub async fn ping_get(&self) -> Result<String, reqwest::Error> {
        let response = self.client.get("https://www.baidu.com").send().await?;
        let text = response.text().await?;
        Ok(text)
    }

    pub async fn reader_get(
        &self,
        sign_data: String,
    ) -> Result<ReaderResponse, Box<dyn std::error::Error>> {
        let req = ReaderRequest::new(self.network.as_str(), sign_data);
        let reader_presign = self.reader_presign.as_str();
        tracing::debug!("reader_presign: {}, req: {}", reader_presign, req);

        let max_retries = 10;
        let mut retry_count = 0;
        loop {
            match self.client.post(reader_presign).json(&req).send().await {
                Ok(resp) => {
                    let resp_data: ReaderResponse = match resp.json().await {
                        Ok(data) => data,
                        Err(e) => {
                            retry_count += 1;
                            if retry_count >= max_retries {
                                tracing::error!(
                                    "reader_presign: {}, max_retries: {}",
                                    reader_presign,
                                    e
                                );
                                return Err("reader_presign: max_retries".into());
                            }
                            tokio::time::sleep(tokio::time::Duration::from_millis(
                                1000 * retry_count,
                            ))
                            .await;
                            continue;
                        }
                    };

                    match resp_data.code {
                        0 => return Ok(resp_data),
                        _ => {
                            retry_count += 1;
                            if retry_count >= max_retries {
                                tracing::error!(
                                    "reader_presign: {}, retrying... {}",
                                    reader_presign,
                                    retry_count
                                );
                                return Err("reader_presign: status_code err".into());
                            }
                            tokio::time::sleep(tokio::time::Duration::from_millis(
                                1000 * retry_count,
                            ))
                            .await;
                            continue;
                        }
                    }
                }
                Err(e) => {
                    retry_count += 1;
                    if retry_count >= max_retries {
                        tracing::error!("reader_presign: {}, error: {}", reader_presign, e);
                        return Err(e.to_string().into());
                    }
                    tokio::time::sleep(tokio::time::Duration::from_millis(1000 * retry_count))
                        .await;
                }
            }
        }
    }

    pub async fn writer_get(&self, sign_data: String) -> Result<String, reqwest::Error> {
        Ok(sign_data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_ping() {
        let signature = SignatureClient::new(
            "http://127.0.0.1:30000/v1/storage/download/presign".to_string(),
            "public".to_string(),
        );
        let result = signature.ping_get().await.unwrap();
        println!("{}", result)
    }
}
