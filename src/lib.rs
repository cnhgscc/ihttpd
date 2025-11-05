use pyo3::prelude::*;

use httpdrs::prelude::*;


#[pyfunction]
fn multi_download(use_loc: String, presign_api: String, max_bandwidth: u64) -> PyResult<()> {
    logger::try_logger_init();
    runtime::start_multi_thread(max_bandwidth, use_loc, presign_api).expect("start multi thread runtime err");
    Ok(())
}


/// A Python module implemented in Rust.
#[pymodule]
fn baai_flagdataset_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(multi_download, m)?)?;
    Ok(())
}
