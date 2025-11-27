

use std::sync::Arc;
use std::thread;

use pyo3::prelude::*;

use httpdrs::prelude::*;
use httpdrs::read::state::DATA;
use crate::state;

#[pyfunction]
pub fn multi_read(
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
pub fn wait_read() -> PyResult<()> {
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
pub fn push_read(name: String) -> PyResult<()> {
    let current_data = DATA.load().clone();
    let new_string = format!("{}\n{}", current_data, name);
    DATA.store(Arc::new(new_string.trim().to_string()));
    Ok(())
}