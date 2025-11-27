mod read;
mod state;

use pyo3::prelude::*;

/// A Python module implemented in Rust.
#[pymodule]
#[pyo3(name = "_ihttpd")]
fn ihttpd(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read::multi_read, m)?)?;
    m.add_function(wrap_pyfunction!(read::push_read, m)?)?;
    m.add_function(wrap_pyfunction!(read::wait_read, m)?)?;
    Ok(())
}
