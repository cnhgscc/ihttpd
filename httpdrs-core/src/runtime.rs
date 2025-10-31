use std::sync::{Arc, Mutex, LazyLock};


use tokio::runtime;
use futures::future::join_all;
use crate::reader;

#[allow(dead_code)]
#[derive(Debug, Default)]
pub struct EventReader{
    pub total_size: i64,
}

static RUNTIME_STATS: LazyLock<Arc<Mutex<EventReader>>> =
    LazyLock::new(|| Arc::new(Mutex::new(EventReader::default())));


pub fn start_multi_thread() -> Result<(), Box<dyn std::error::Error>>{

    let rt = runtime::Builder::new_multi_thread()
        .worker_threads(100)
        .enable_all()
        .build()
        .unwrap();

    tracing::info!("Runtime initialized: baai-flagdataset-rs");

    let spawn_reader = rt.spawn(async {
        let mut csv_reader = reader::CSVMetaReader::new("/Users/hgshicc/test/flagdataset/AIM-500/meta".to_string());
        let _ = csv_reader.init().await;
        tracing::info!("CSVReader initialized: baai-flagdataset-rs: {}", csv_reader);
    });

    let event_tasks = vec![spawn_reader];

    let _ = rt.block_on(join_all(event_tasks));
    rt.shutdown_background();

   Ok(())
}