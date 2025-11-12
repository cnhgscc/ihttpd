// use std::sync::Arc;
// use tokio_util::sync::CancellationToken;
// use httpdrs_core::httpd::Bandwidth;
//
// use crate::stats::RUNTIME;
//
// pub(crate) async fn reset_period(
//     reset_bandwidth: Arc<Bandwidth>,
//     token_bandwidth: CancellationToken
// ) {
//     loop {
//         tokio::select! {
//             _ = tokio::time::sleep(tokio::time::Duration::from_millis(1000)) => {
//                 let speed = reset_bandwidth.reset_period(1000);
//                 if speed > 0 {
//                     RUNTIME.lock().unwrap().download_speed = speed;
//                 }
//             }
//             _ = token_bandwidth.cancelled() => {
//                 break;
//             }
//         }
//     }
// }