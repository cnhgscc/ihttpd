use std::sync::{Mutex, OnceLock};
use std::thread;

static THREAD: OnceLock<Mutex<Option<thread::JoinHandle<()>>>> = OnceLock::new();

pub(crate) fn manager() -> &'static Mutex<Option<thread::JoinHandle<()>>> {
    THREAD.get_or_init(|| Mutex::new(None))
}
