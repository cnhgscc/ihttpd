pub struct DigitalSignatureAlgorithm{
    client: reqwest::Client,
    url: String,
}


impl DigitalSignatureAlgorithm {
    pub fn new() -> Self {
        DigitalSignatureAlgorithm {
            client: reqwest::Client::new(),
            url: "https://www.baidu.com".to_string(),
        }
    }

    pub async fn ping_get(&self) -> Result<String, reqwest::Error> {
        let response = self.client.get("https://www.baidu.com")
            .send()
            .await?;
        let text = response.text().await?;
        Ok(text)
    }

    pub async fn download_sign(&self) -> Result<String, reqwest::Error> {
        let response = self.client.get(self.url.clone()).send().await?;
        let text = response.text().await?;
        Ok(text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_ping() {
        let s = DigitalSignatureAlgorithm::new();
        let result = s.ping_get().await.unwrap();
        println!("{}", result)
    }
}