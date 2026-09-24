//! Transports drive a [`crate::Session`] over a socket.

#[cfg(feature = "http")]
pub mod http;
#[cfg(feature = "ws")]
pub mod ws;

#[cfg(any(feature = "http", feature = "ws"))]
mod handle {
    use std::collections::hash_map::RandomState;
    use std::hash::{BuildHasher, Hasher};
    use std::sync::Arc;
    use std::time::Duration;

    use serde_json::Value;
    use tokio::sync::{mpsc, Mutex, Notify};

    use crate::session::{Event, Session};

    pub(crate) const BACKOFF_FLOOR: Duration = Duration::from_millis(250);
    pub(crate) const BACKOFF_CEILING: Duration = Duration::from_secs(30);
    pub(crate) const RECEIVE_TIMEOUT: Duration = Duration::from_secs(30);

    /// A handle to a running transport task.
    ///
    /// Dropping the handle stops the connection task.
    pub struct Client {
        pub(crate) session: Arc<Mutex<Session>>,
        pub(crate) wake: Arc<Notify>,
        pub(crate) _stop: mpsc::Sender<()>,
    }

    impl Client {
        /// Queues a command and wakes the connection to send it.
        pub async fn command(&self, protocol: &str, method: &str, params: Vec<Value>) -> u64 {
            let seq = self.session.lock().await.command(protocol, method, params);
            self.wake.notify_one();
            seq
        }

        /// Reads from the session under its lock.
        pub async fn with_session<T>(&self, read: impl FnOnce(&Session) -> T) -> T {
            read(&*self.session.lock().await)
        }

        /// The current value of a protocol's main document.
        pub async fn main_value(&self, protocol: &str) -> Option<Value> {
            self.with_session(|session| session.documents().main(protocol).map(|d| d.value.clone()))
                .await
        }
    }

    /// Generates a client id: 32 lowercase hexadecimal characters.
    pub fn generate_client_id() -> String {
        let mut id = String::with_capacity(32);
        for salt in 0u64..2 {
            let mut hasher = RandomState::new().build_hasher();
            hasher.write_u64(salt);
            hasher.write_u128(std::time::UNIX_EPOCH.elapsed().map_or(0, |d| d.as_nanos()));
            id.push_str(&format!("{:016x}", hasher.finish()));
        }
        id
    }

    pub(crate) fn forward(
        events: &mpsc::UnboundedSender<Event>,
        batch: Vec<Event>,
    ) -> Result<(), ()> {
        for event in batch {
            events.send(event).map_err(|_| ())?;
        }
        Ok(())
    }

    /// Exponential backoff with jitter in `[capped/2, capped]`.
    pub(crate) fn backoff_delay(attempt: u32) -> Duration {
        if attempt == 0 {
            return Duration::ZERO;
        }
        let exponent = attempt.saturating_sub(1).min(7);
        let base = BACKOFF_FLOOR * 2u32.pow(exponent);
        let capped = base.min(BACKOFF_CEILING);
        let mut hasher = RandomState::new().build_hasher();
        hasher.write_u32(attempt);
        let fraction = (hasher.finish() % 1000) as f64 / 1000.0;
        capped / 2 + Duration::from_secs_f64(capped.as_secs_f64() / 2.0 * fraction)
    }
}

#[cfg(any(feature = "http", feature = "ws"))]
pub(crate) use handle::{backoff_delay, forward, BACKOFF_CEILING, RECEIVE_TIMEOUT};
#[cfg(any(feature = "http", feature = "ws"))]
pub use handle::{generate_client_id, Client};
