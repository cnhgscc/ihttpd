use indicatif::HumanBytes;
use indicatif::{ProgressBar, ProgressStyle};

pub fn create() -> ProgressBar {
    let pbar = ProgressBar::new(0);
    let sty = ProgressStyle::with_template(
        "[{elapsed_precise}] {bar:100.cyan/blue} {pos:>7}/{len:7} {msg}",
    )
    .unwrap()
    .progress_chars("##-");
    pbar.set_style(sty);
    pbar
}

pub fn format(
    require_bytes: u64,
    download_speed: u64,
    download_precent: f64,
    download_bytes: u64,
) -> String {
    let require_bytes_human = HumanBytes(require_bytes);

    let download_speed_human = HumanBytes(download_speed);
    let download_bytes_human = HumanBytes(download_bytes);

    // 1. 下载大小的进度
    // 2. 下载速度
    format!(
        "| {}/{} | {:.2}% | {}/s",
        download_bytes_human,
        require_bytes_human,
        download_precent * 100.0,
        download_speed_human,
    )
}
