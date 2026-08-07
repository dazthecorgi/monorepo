//! Bounded connect helpers shared by the proxy's monitors and drivers.
//!
//! Every dial in the proxy must be bounded: a black-holed target (a container
//! that never came up, a dropped veth) otherwise stalls its caller for the OS
//! TCP timeout — and the join driver, enrollment monitor and frame monitor all
//! sit on paths where one stalled call delays verification for every other
//! node. Three hand-rolled copies of this logic once existed, two of them
//! unbounded; this is the single shared one.

use std::future::Future;
use std::time::Duration;

use quil_types::proto::node::node_service_client::NodeServiceClient;
use tonic::transport::Channel;

/// Default dial bound for plaintext NodeService connects.
pub const DIAL_TIMEOUT: Duration = Duration::from_secs(5);

/// Run `fut` (a connect future) under `dur`; `None` on error or timeout,
/// logged with the caller's label.
pub async fn bounded_connect<T, E: std::fmt::Display>(
    what: &'static str,
    address: &str,
    dur: Duration,
    fut: impl Future<Output = Result<T, E>>,
) -> Option<T> {
    match tokio::time::timeout(dur, fut).await {
        Ok(Ok(c)) => Some(c),
        Ok(Err(e)) => {
            tracing::debug!(address, error = %e, "{}: connect failed", what);
            None
        }
        Err(_) => {
            tracing::debug!(address, "{}: connect timed out", what);
            None
        }
    }
}

/// Bounded plaintext NodeService dial (`http://<address>`).
pub async fn connect_node_service(
    what: &'static str,
    address: &str,
) -> Option<NodeServiceClient<Channel>> {
    let url = format!("http://{address}");
    let endpoint = match Channel::from_shared(url) {
        Ok(e) => e,
        Err(e) => {
            tracing::warn!(address, error = %e, "{}: bad node address", what);
            return None;
        }
    };
    bounded_connect(what, address, DIAL_TIMEOUT, endpoint.connect())
        .await
        .map(NodeServiceClient::new)
}
