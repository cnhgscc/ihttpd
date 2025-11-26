mod state;

use std::sync::Arc;
use std::thread;

use pyo3::prelude::*;

use httpdrs::prelude::*;
use httpdrs::state::DATA;

#[pyfunction]
fn multi_download(
    use_loc: String,
    presign_api: String,
    network: String,
    max_bandwidth: u64,
    max_parallel: u64,
) -> PyResult<()> {
    let handle = thread::spawn(move || {
        logger::try_logger_init(format!("{}/logs", use_loc).as_str());

        runtime::start_multi_thread(
            max_bandwidth,
            max_parallel as usize,
            use_loc,
            presign_api,
            network,
        )
        .expect("start multi thread runtime err");
    });

    let manager = state::manager();
    let mut guard = manager.lock().unwrap();
    *guard = Some(handle);

    Ok(())
}

#[pyfunction]
fn wait() -> PyResult<()> {
    let manager = state::manager();
    let mut guard = manager.lock().unwrap();
    if let Some(handle) = guard.take() {
        handle.join().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Thread panicked: {:?}", e))
        })?;
    }
    Ok(())
}

#[pyfunction]
fn push(name: String) -> PyResult<()> {
    let current_data = DATA.load().clone();
    let new_string = format!("{}\n{}", current_data, name);
    DATA.store(Arc::new(new_string.trim().to_string()));
    Ok(())
}

/// A Python module implemented in Rust.
#[pymodule]
#[pyo3(name = "_ihttpd")]
fn ihttpd(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(multi_download, m)?)?;
    m.add_function(wrap_pyfunction!(push, m)?)?;
    m.add_function(wrap_pyfunction!(wait, m)?)?;
    Ok(())
}
