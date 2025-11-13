use pyo3::prelude::*;

use httpdrs::prelude::*;

#[pyfunction]
fn multi_download(
    use_loc: String,
    presign_api: String,
    network: String,
    max_bandwidth: u64,
    max_parallel: u64,
) -> PyResult<()> {
    logger::try_logger_init(format!("{}/logs", use_loc).as_str());
    runtime::start_multi_thread(
        max_bandwidth,
        max_parallel as usize,
        use_loc,
        presign_api,
        network,
    )
    .expect("start multi thread runtime err");
    Ok(())
}

/// A Python module implemented in Rust.
#[pymodule]
fn baai_flagdataset_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(multi_download, m)?)?;
    Ok(())
}
