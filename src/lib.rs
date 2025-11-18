use pyo3::prelude::*;
use std::sync::{Mutex, OnceLock};
use std::thread;

use httpdrs::prelude::*;
use httpdrs::state;

static DOWNLOAD_THREAD: OnceLock<Mutex<Option<thread::JoinHandle<()>>>> = OnceLock::new();

fn get_global_handle() -> &'static Mutex<Option<thread::JoinHandle<()>>> {
    DOWNLOAD_THREAD.get_or_init(|| Mutex::new(None))
}

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

    // 将线程句柄存储到全局变量
    let global_handle = get_global_handle();
    let mut guard = global_handle.lock().unwrap();
    *guard = Some(handle);

    Ok(())
}

#[pyfunction]
fn wait_for_completion() -> PyResult<()> {
    let global_handle = get_global_handle();
    let mut guard = global_handle.lock().unwrap();
    if let Some(handle) = guard.take() {
        handle.join().map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Thread panicked: {:?}", e))
        })?;
    }
    Ok(())
}

#[pyfunction]
fn meta_push(name: String) -> PyResult<()> {
    const MAX_RETRIES: usize = 5;
    const RETRY_DELAY_MS: u64 = 10;

    for attempt in 0..MAX_RETRIES {
        match state::META_FILE_LIST.try_write() {
            Ok(mut guard) => {
                guard.push(name.clone());
                return Ok(());
            }
            Err(_) if attempt < MAX_RETRIES - 1 => {
                std::thread::sleep(std::time::Duration::from_millis(RETRY_DELAY_MS * (attempt + 1) as u64));
            }
            Err(_) => {
                return Err(pyo3::exceptions::PyRuntimeError::new_err(
                    "无法获取写锁，可能被其他操作占用"
                ));
            }
        }
    }

    Ok(())
}

/// A Python module implemented in Rust.
#[pymodule]
fn baai_flagdataset_rs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(multi_download, m)?)?;
    m.add_function(wrap_pyfunction!(meta_push, m)?)?;
    m.add_function(wrap_pyfunction!(wait_for_completion, m)?)?;
    Ok(())
}
