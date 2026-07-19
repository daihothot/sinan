use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, RwLock,
    },
};

use sinan_types::{AccountId, ClientId, SessionId, TerminalId};
use tokio::sync::{Mutex, MutexGuard};

use crate::{OutboundFrame, OutboundSink, SinkWriteOutcome};

const FENCED_BIT: usize = 1 << (usize::BITS - 1);
const WRITER_MASK: usize = FENCED_BIT - 1;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct LiveSessionRoute {
    pub client_id: ClientId,
    pub account_id: AccountId,
    pub terminal_id: Option<TerminalId>,
}

pub struct LiveSessionHandle {
    session_id: SessionId,
    sink: Arc<dyn OutboundSink>,
    state: AtomicUsize,
}

impl LiveSessionHandle {
    fn new(session_id: SessionId, sink: Arc<dyn OutboundSink>) -> Self {
        Self {
            session_id,
            sink,
            state: AtomicUsize::new(0),
        }
    }

    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    pub fn is_fenced(&self) -> bool {
        self.state.load(Ordering::Acquire) & FENCED_BIT != 0
    }

    /// Fencing is linearized against write admission. An admitted write may
    /// finish, but every write that observes this fence is rejected.
    pub(crate) fn fence(&self) {
        self.state.fetch_or(FENCED_BIT, Ordering::AcqRel);
    }

    pub(crate) async fn write(&self, frame: OutboundFrame) -> SinkWriteOutcome {
        if frame.session_id != self.session_id {
            return SinkWriteOutcome::DefinitelyNotWritten {
                error: "outbound frame session differs from live transport epoch".to_owned(),
            };
        }
        let Some(_permit) = self.acquire_write() else {
            return SinkWriteOutcome::DefinitelyNotWritten {
                error: "session transport was fenced before write admission".to_owned(),
            };
        };
        self.sink.write(frame).await
    }

    fn acquire_write(&self) -> Option<WritePermit<'_>> {
        let mut current = self.state.load(Ordering::Acquire);
        loop {
            if current & FENCED_BIT != 0 {
                return None;
            }
            if current & WRITER_MASK == WRITER_MASK {
                return None;
            }
            match self.state.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(WritePermit { handle: self }),
                Err(observed) => current = observed,
            }
        }
    }
}

struct WritePermit<'a> {
    handle: &'a LiveSessionHandle,
}

impl Drop for WritePermit<'_> {
    fn drop(&mut self) {
        self.handle.state.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Default)]
struct LiveSessions {
    by_session: HashMap<SessionId, Arc<LiveSessionHandle>>,
    by_route: HashMap<LiveSessionRoute, SessionId>,
}

/// Process-local transport bindings; durable eligibility remains in Store.
#[derive(Default)]
pub struct LiveSessionRegistry {
    sessions: RwLock<LiveSessions>,
    activation: Mutex<()>,
}

impl LiveSessionRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) async fn activation_guard(&self) -> MutexGuard<'_, ()> {
        self.activation.lock().await
    }

    /// Fences the currently attached handle before durable replacement starts.
    pub(crate) fn fence_route(&self, route: &LiveSessionRoute) -> Option<SessionId> {
        let sessions = self
            .sessions
            .read()
            .unwrap_or_else(|error| error.into_inner());
        let session_id = sessions.by_route.get(route)?.clone();
        sessions.by_session.get(&session_id)?.fence();
        Some(session_id)
    }

    /// Publishes a durably active session and removes only the replaced epoch.
    pub(crate) fn activate(
        &self,
        route: LiveSessionRoute,
        session_id: SessionId,
        replaced_session_id: Option<&SessionId>,
        sink: Arc<dyn OutboundSink>,
    ) -> Arc<LiveSessionHandle> {
        let mut sessions = self
            .sessions
            .write()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(existing_id) = sessions.by_route.get(&route).cloned() {
            if let Some(existing) = sessions.by_session.remove(&existing_id) {
                existing.fence();
            }
        }
        if let Some(replaced_session_id) = replaced_session_id {
            if let Some(replaced) = sessions.by_session.remove(replaced_session_id) {
                replaced.fence();
            }
        }
        let handle = Arc::new(LiveSessionHandle::new(session_id.clone(), sink));
        sessions.by_route.insert(route, session_id.clone());
        sessions.by_session.insert(session_id, Arc::clone(&handle));
        handle
    }

    pub(crate) fn handle(&self, session_id: &SessionId) -> Option<Arc<LiveSessionHandle>> {
        self.sessions
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .by_session
            .get(session_id)
            .cloned()
    }

    pub(crate) fn disconnect(&self, session_id: &SessionId) -> bool {
        let mut sessions = self
            .sessions
            .write()
            .unwrap_or_else(|error| error.into_inner());
        let Some(handle) = sessions.by_session.remove(session_id) else {
            return false;
        };
        handle.fence();
        sessions
            .by_route
            .retain(|_, active_session_id| active_session_id != session_id);
        true
    }

    pub(crate) fn clear(&self) {
        let mut sessions = self
            .sessions
            .write()
            .unwrap_or_else(|error| error.into_inner());
        for handle in sessions.by_session.values() {
            handle.fence();
        }
        sessions.by_session.clear();
        sessions.by_route.clear();
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::ready,
        sync::atomic::{AtomicBool, Ordering},
    };

    use sinan_types::MessageId;

    use super::*;
    use crate::SinkWriteFuture;

    struct WrittenSink;

    impl OutboundSink for WrittenSink {
        fn write<'a>(&'a self, _frame: OutboundFrame) -> SinkWriteFuture<'a> {
            Box::pin(ready(SinkWriteOutcome::Written))
        }
    }

    struct ControlledSink {
        started: Arc<AtomicBool>,
        release: Arc<AtomicBool>,
    }

    impl OutboundSink for ControlledSink {
        fn write<'a>(&'a self, _frame: OutboundFrame) -> SinkWriteFuture<'a> {
            let started = Arc::clone(&self.started);
            let release = Arc::clone(&self.release);
            Box::pin(async move {
                started.store(true, Ordering::Release);
                while !release.load(Ordering::Acquire) {
                    tokio::task::yield_now().await;
                }
                SinkWriteOutcome::Written
            })
        }
    }

    fn route() -> LiveSessionRoute {
        LiveSessionRoute {
            client_id: ClientId::from("client_1"),
            account_id: AccountId::from("account_1"),
            terminal_id: Some(TerminalId::from("terminal_1")),
        }
    }

    fn frame(session_id: &str) -> OutboundFrame {
        OutboundFrame {
            session_id: SessionId::from(session_id),
            message_id: MessageId::from("message_1"),
            sequence: 2,
            wire_bytes: b"{}".to_vec(),
        }
    }

    #[test]
    fn stale_disconnect_callback_cannot_remove_replacement_session() {
        let registry = LiveSessionRegistry::new();
        let old_session = SessionId::from("session_old");
        let new_session = SessionId::from("session_new");
        registry.activate(route(), old_session.clone(), None, Arc::new(WrittenSink));
        registry.activate(
            route(),
            new_session.clone(),
            Some(&old_session),
            Arc::new(WrittenSink),
        );

        assert!(registry.handle(&old_session).is_none());
        assert!(registry.handle(&new_session).is_some());
        assert!(!registry.disconnect(&old_session));
        assert!(registry.handle(&new_session).is_some());
    }

    #[tokio::test]
    async fn replacement_fence_linearizes_against_transport_write() {
        let registry = Arc::new(LiveSessionRegistry::new());
        let old_session = SessionId::from("session_old");
        let started = Arc::new(AtomicBool::new(false));
        let release = Arc::new(AtomicBool::new(false));
        let old_handle = registry.activate(
            route(),
            old_session.clone(),
            None,
            Arc::new(ControlledSink {
                started: Arc::clone(&started),
                release: Arc::clone(&release),
            }),
        );
        let task = tokio::spawn({
            let old_handle = Arc::clone(&old_handle);
            async move { old_handle.write(frame("session_old")).await }
        });
        while !started.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }

        registry.activate(
            route(),
            SessionId::from("session_new"),
            Some(&old_session),
            Arc::new(WrittenSink),
        );
        release.store(true, Ordering::Release);

        assert_eq!(task.await.unwrap(), SinkWriteOutcome::Written);
        assert!(matches!(
            old_handle.write(frame("session_old")).await,
            SinkWriteOutcome::DefinitelyNotWritten { .. }
        ));
    }

    #[tokio::test]
    async fn live_handle_rejects_a_frame_for_another_session_epoch() {
        let registry = LiveSessionRegistry::new();
        let handle = registry.activate(
            route(),
            SessionId::from("session_1"),
            None,
            Arc::new(WrittenSink),
        );

        assert!(matches!(
            handle.write(frame("session_2")).await,
            SinkWriteOutcome::DefinitelyNotWritten { .. }
        ));
    }

    #[test]
    fn startup_fence_clears_all_process_local_sinks() {
        let registry = LiveSessionRegistry::new();
        let session = SessionId::from("session_1");
        let handle = registry.activate(route(), session.clone(), None, Arc::new(WrittenSink));

        registry.clear();

        assert!(registry.handle(&session).is_none());
        assert!(handle.is_fenced());
    }
}
