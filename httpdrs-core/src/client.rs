use std::sync::Arc;
use std::time::Duration;
use reqwest::Client;

pub fn new_client() -> Arc<Client>{

    let client = Client::builder()
        .pool_max_idle_per_host(20)
        .pool_idle_timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(300))
        .user_agent("WiSearch Downloader")
        .build()
        .expect("Failed to build reqwest client");


    Arc::new(client)

}