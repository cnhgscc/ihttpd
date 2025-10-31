use pyo3::prelude::*;
use httpdrs::logger;


#[pyfunction]
fn run_flagdataset() -> PyResult<()> {
    logger::try_logger_init();
    Ok(())
}


/// A Python module implemented in Rust.
#[pymodule]
fn baai_flagdataset_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(run_flagdataset, m)?)?;
    Ok(())
}
