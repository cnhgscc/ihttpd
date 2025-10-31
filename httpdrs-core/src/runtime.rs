use tokio::runtime;

pub fn start_multi_thread() -> Result<(), Box<dyn std::error::Error>>{


    let rt = runtime::Builder::new_multi_thread()
        .worker_threads(100)
        .enable_all()
        .build()
        .unwrap();

    tracing::info!("Runtime initialized: baai-flagdataset-rs");


    rt.block_on(async {
        println!("Hello, world!");
    });

   Ok(())
}