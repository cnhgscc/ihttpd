use tracing_appender::rolling;
pub fn try_logger_init(log_path: &str) {
    let appender = rolling::never(log_path, "httpdrs.log");

    let subscriber = tracing_subscriber::fmt()
        .with_line_number(true)
        .with_file(true)
        .with_thread_ids(true)
        .with_ansi(false)
        .with_writer(appender) // 显示线程ID
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(tracing::Level::WARN.into())
                .from_env_lossy()  // 优先使用环境变量，失败则回退到默认值
        )
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("Failed to set subscriber");
    tracing::info!("Logger initialized: baai-flagdatset-rs");
}
