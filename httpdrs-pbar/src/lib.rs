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
    require_count: i32,
    require_bytes: i32,
)-> String{
    format!("| download({}/{})", require_count,  require_bytes)
}
