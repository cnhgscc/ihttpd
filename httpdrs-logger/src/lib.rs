pub fn try_logger_init() {
    tracing_subscriber::fmt()
        .with_line_number(true)
        .with_file(false)
        .with_thread_ids(true)       // 显示线程ID
        .init();
    tracing::info!("Logger initialized: baai-flagdatset-rs");
}

