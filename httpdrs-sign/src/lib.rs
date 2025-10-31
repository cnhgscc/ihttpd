use crate::reader::ReaderRequest;

mod reader;

pub struct SignatureClient{
    client: reqwest::Client,
    reader_presign: String,
}


impl SignatureClient {
    pub fn new() -> Self {
        SignatureClient {
            client: reqwest::Client::new(),
            reader_presign: "http://127.0.0.1:30002/api/v1/provider/download/presign".to_string(),
        }
    }

    pub async fn ping_get(&self) -> Result<String, reqwest::Error> {
        let response = self.client.get("https://www.baidu.com")
            .send()
            .await?;
        let text = response.text().await?;
        Ok(text)
    }

    pub async fn reader_get(&self, _sign_data: String) -> Result<String, reqwest::Error> {
        let req = ReaderRequest::new("public", "".to_string());

        let reader_presign = self.reader_presign.as_str();
        let resp = self.client.post(reader_presign).json(&req).send().await?;
        let text = resp.text().await?;
        Ok(text)
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
        let signature = SignatureClient::new();
        let result = signature.ping_get().await.unwrap();
        println!("{}", result)
    }
}