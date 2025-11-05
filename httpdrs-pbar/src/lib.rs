use indicatif::{ProgressBar, ProgressStyle};
use indicatif::{HumanBytes, HumanCount};


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
    require_count: u64,
    download_speed: u64,
    download_precent: f64,
)-> String{

    let require_bytes_human = HumanBytes(require_bytes);
    let require_count_human = HumanCount(require_count);

    let download_speed_human = HumanBytes(download_speed);

    format!("|  {:.2}%   | {}/s | required({}/{})",download_precent*100.0, download_speed_human, require_count_human, require_bytes_human)
}
