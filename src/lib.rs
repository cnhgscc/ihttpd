use pyo3::prelude::*;

use httpdrs::*;


#[pyfunction]
fn run_flagdataset() -> PyResult<()> {
    logger::try_logger_init();
    httpd::start_multi_thread().expect("start multi thread runtime err");

    Ok(())
}


/// A Python module implemented in Rust.
#[pymodule]
fn baai_flagdataset_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(run_flagdataset, m)?)?;
    Ok(())
}
