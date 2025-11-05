use tracing_appender::rolling;
pub fn try_logger_init() {

    let _appender = rolling::daily("logs", "app.log");

    let subscriber = tracing_subscriber::fmt()
        .with_line_number(true)
        .with_file(true)
        .with_thread_ids(true)
        // .with_ansi(false)
        // .with_writer(appender)// 显示线程ID
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("Failed to set subscriber");
    tracing::info!("Logger initialized: baai-flagdatset-rs");
}

