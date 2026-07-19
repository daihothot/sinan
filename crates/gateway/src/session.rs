use std::sync::Arc;

use sinan_execution::ServerClock;
use sinan_protocol::{ExecutionClientPlatform, HeartbeatPayload};
use sinan_store::{
    CanonicalJson, DeliveryStartupFenceReport, NewSessionRecord, SessionDisconnectOutcome,
    SessionHeartbeatUpdate, SessionReplacement, SessionStatusUpdate, SqliteStateStore, StoreError,
    StoredSessionRecord,
};
use sinan_types::{AccountId, ClientId, ClockSyncStatus, SessionId, SessionStatus, TerminalId};
use thiserror::Error;

use crate::{LiveSessionRegistry, LiveSessionRoute, OutboundSink};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionRegistration {
    pub session_id: SessionId,
    pub client_id: ClientId,
    pub account_id: AccountId,
    pub terminal_id: Option<TerminalId>,
    pub platform: ExecutionClientPlatform,
    pub capabilities: Vec<String>,
    pub remote_addr: Option<String>,
    pub max_inflight_commands: u64,
}

impl SessionRegistration {
    fn route(&self) -> LiveSessionRoute {
        LiveSessionRoute {
            client_id: self.client_id.clone(),
            account_id: self.account_id.clone(),
            terminal_id: self.terminal_id.clone(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GatewaySessionConfig {
    pub max_clock_offset_ms: u64,
}

impl GatewaySessionConfig {
    fn validate(self) -> Result<Self, SessionRegistryError> {
        if self.max_clock_offset_ms == 0 || self.max_clock_offset_ms > i64::MAX as u64 {
            Err(SessionRegistryError::InvalidConfig)
        } else {
            Ok(self)
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HeartbeatHealth {
    Healthy,
    ClockSkew,
    ClockUnhealthy,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeartbeatAssessment {
    pub session: StoredSessionRecord,
    pub health: HeartbeatHealth,
    pub clock_skew_ms: u64,
}

#[derive(Debug, Error)]
pub enum SessionRegistryError {
    #[error("invalid Gateway session configuration")]
    InvalidConfig,

    #[error("invalid session registration field: {0}")]
    InvalidRegistration(&'static str),

    #[error("invalid heartbeat evidence: {0}")]
    InvalidHeartbeat(&'static str),

    #[error("live session route and durable replacement disagree")]
    ReplacementFenceMismatch,

    #[error(transparent)]
    Store(#[from] StoreError),
}

#[derive(Clone)]
pub struct GatewaySessionRegistry {
    store: SqliteStateStore,
    live_sessions: Arc<LiveSessionRegistry>,
    clock: Arc<dyn ServerClock>,
    config: GatewaySessionConfig,
}

impl GatewaySessionRegistry {
    pub fn new(
        store: SqliteStateStore,
        live_sessions: Arc<LiveSessionRegistry>,
        clock: Arc<dyn ServerClock>,
        config: GatewaySessionConfig,
    ) -> Result<Self, SessionRegistryError> {
        Ok(Self {
            store,
            live_sessions,
            clock,
            config: config.validate()?,
        })
    }

    pub fn live_sessions(&self) -> &Arc<LiveSessionRegistry> {
        &self.live_sessions
    }

    pub async fn activate(
        &self,
        registration: SessionRegistration,
        sink: Arc<dyn OutboundSink>,
    ) -> Result<SessionReplacement, SessionRegistryError> {
        validate_registration(&registration)?;
        let capabilities = normalized_capabilities(&registration.capabilities)?;
        let _activation = self.live_sessions.activation_guard().await;
        let now = self.server_now()?;
        let route = registration.route();
        let pre_fenced = self.live_sessions.fence_route(&route);
        let replacement = self
            .store
            .replace_active_session(NewSessionRecord {
                session_id: registration.session_id.clone(),
                client_id: registration.client_id,
                account_id: registration.account_id,
                terminal_id: registration.terminal_id,
                platform: platform_name(registration.platform).to_owned(),
                status: SessionStatus::Active,
                capabilities,
                remote_addr: registration.remote_addr,
                connected_at: now,
                last_heartbeat_at: None,
                last_time_sync_at: None,
                clock_sync_status: None,
                disconnected_at: None,
                max_inflight_commands: registration.max_inflight_commands,
                updated_at: now,
            })
            .await?;
        let replaced_session_id = replacement
            .replaced_session
            .as_ref()
            .map(|session| &session.session_id);
        if pre_fenced
            .as_ref()
            .is_some_and(|session_id| Some(session_id) != replaced_session_id)
        {
            self.fail_closed_new_session(&replacement.session).await?;
            return Err(SessionRegistryError::ReplacementFenceMismatch);
        }
        self.live_sessions.activate(
            route,
            replacement.session.session_id.clone(),
            replaced_session_id,
            sink,
        );
        Ok(replacement)
    }

    /// Fences every process-local transport before atomically recovering
    /// durable sessions and interrupted writes after process startup.
    pub async fn fence_startup(
        &self,
        error: impl Into<String>,
    ) -> Result<DeliveryStartupFenceReport, SessionRegistryError> {
        let _activation = self.live_sessions.activation_guard().await;
        self.live_sessions.clear();
        Ok(self
            .store
            .fence_interrupted_writes(self.server_now()?, error)
            .await?)
    }

    pub async fn assess_heartbeat(
        &self,
        session_id: &SessionId,
        heartbeat: &HeartbeatPayload,
    ) -> Result<HeartbeatAssessment, SessionRegistryError> {
        let now = self.server_now()?;
        let current =
            self.store
                .get_session(session_id)
                .await?
                .ok_or_else(|| StoreError::NotFound {
                    entity: "execution_client_session",
                    key: session_id.to_string(),
                })?;
        if heartbeat.effective_server_now < 0 {
            self.persist_clock_unhealthy(&current, now).await?;
            return Err(SessionRegistryError::InvalidHeartbeat(
                "effective_server_now",
            ));
        }
        let clock_skew_ms = now.abs_diff(heartbeat.effective_server_now);
        let effective_status = if clock_skew_ms > self.config.max_clock_offset_ms {
            ClockSyncStatus::Unsynced
        } else {
            heartbeat.clock_sync_status
        };
        if heartbeat
            .last_time_sync_at_server_ms
            .is_some_and(|at| at < current.connected_at || at > now)
        {
            self.persist_clock_unhealthy(&current, now).await?;
            return Err(SessionRegistryError::InvalidHeartbeat(
                "last_time_sync_at_server_ms",
            ));
        }
        if effective_status == ClockSyncStatus::Synced
            && heartbeat.last_time_sync_at_server_ms.is_none()
            && current.last_time_sync_at.is_none()
        {
            self.persist_clock_unhealthy(&current, now).await?;
            return Err(SessionRegistryError::InvalidHeartbeat("clock_sync_status"));
        }
        let session = self
            .store
            .update_session_heartbeat(SessionHeartbeatUpdate {
                session_id: session_id.clone(),
                expected_revision: current.revision,
                heartbeat_at: now,
                clock_sync_status: effective_status,
                last_time_sync_at: heartbeat.last_time_sync_at_server_ms,
                updated_at: now.max(current.updated_at),
            })
            .await?;
        let health = if clock_skew_ms > self.config.max_clock_offset_ms {
            HeartbeatHealth::ClockSkew
        } else if session.clock_sync_status == Some(ClockSyncStatus::Synced) {
            HeartbeatHealth::Healthy
        } else {
            HeartbeatHealth::ClockUnhealthy
        };
        Ok(HeartbeatAssessment {
            session,
            health,
            clock_skew_ms,
        })
    }

    async fn persist_clock_unhealthy(
        &self,
        current: &StoredSessionRecord,
        heartbeat_at: i64,
    ) -> Result<(), SessionRegistryError> {
        self.store
            .update_session_heartbeat(SessionHeartbeatUpdate {
                session_id: current.session_id.clone(),
                expected_revision: current.revision,
                heartbeat_at,
                clock_sync_status: ClockSyncStatus::Unsynced,
                last_time_sync_at: None,
                updated_at: heartbeat_at.max(current.updated_at),
            })
            .await?;
        Ok(())
    }

    pub async fn mark_stale(
        &self,
        session_id: &SessionId,
        error: impl Into<String>,
    ) -> Result<SessionDisconnectOutcome, SessionRegistryError> {
        self.close(session_id, error.into(), true).await
    }

    pub async fn disconnect(
        &self,
        session_id: &SessionId,
        error: impl Into<String>,
    ) -> Result<SessionDisconnectOutcome, SessionRegistryError> {
        self.close(session_id, error.into(), false).await
    }

    async fn close(
        &self,
        session_id: &SessionId,
        error: String,
        stale: bool,
    ) -> Result<SessionDisconnectOutcome, SessionRegistryError> {
        if error.trim().is_empty() {
            return Err(SessionRegistryError::InvalidRegistration("delivery_error"));
        }
        let _activation = self.live_sessions.activation_guard().await;
        if let Some(handle) = self.live_sessions.handle(session_id) {
            handle.fence();
        }
        let current =
            self.store
                .get_session(session_id)
                .await?
                .ok_or_else(|| StoreError::NotFound {
                    entity: "execution_client_session",
                    key: session_id.to_string(),
                })?;
        let now = self.server_now()?.max(current.updated_at);
        let update = SessionStatusUpdate {
            session_id: session_id.clone(),
            expected_revision: current.revision,
            changed_at: now,
            delivery_error: error,
        };
        let outcome = if stale {
            self.store.mark_session_stale(update).await?
        } else {
            self.store.disconnect_session(update).await?
        };
        self.live_sessions.disconnect(session_id);
        Ok(outcome)
    }

    async fn fail_closed_new_session(
        &self,
        session: &StoredSessionRecord,
    ) -> Result<(), SessionRegistryError> {
        self.store
            .mark_session_stale(SessionStatusUpdate {
                session_id: session.session_id.clone(),
                expected_revision: session.revision,
                changed_at: self.server_now()?.max(session.updated_at),
                delivery_error: "LIVE_SESSION_REPLACEMENT_FENCE_MISMATCH".to_owned(),
            })
            .await?;
        Ok(())
    }

    fn server_now(&self) -> Result<i64, SessionRegistryError> {
        let now = self.clock.now_ms();
        if now < 0 {
            Err(SessionRegistryError::InvalidHeartbeat("server_clock"))
        } else {
            Ok(now)
        }
    }
}

fn validate_registration(registration: &SessionRegistration) -> Result<(), SessionRegistryError> {
    for (field, value) in [
        ("session_id", registration.session_id.as_str()),
        ("client_id", registration.client_id.as_str()),
        ("account_id", registration.account_id.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(SessionRegistryError::InvalidRegistration(field));
        }
    }
    if registration
        .terminal_id
        .as_deref()
        .is_some_and(|value| value.trim().is_empty())
    {
        return Err(SessionRegistryError::InvalidRegistration("terminal_id"));
    }
    if registration.max_inflight_commands == 0
        || registration.max_inflight_commands > i64::MAX as u64
    {
        return Err(SessionRegistryError::InvalidRegistration(
            "max_inflight_commands",
        ));
    }
    Ok(())
}

fn normalized_capabilities(capabilities: &[String]) -> Result<CanonicalJson, SessionRegistryError> {
    let mut normalized = capabilities.to_vec();
    if normalized.iter().any(|value| value.trim().is_empty()) {
        return Err(SessionRegistryError::InvalidRegistration("capabilities"));
    }
    normalized.sort();
    if normalized.windows(2).any(|values| values[0] == values[1]) {
        return Err(SessionRegistryError::InvalidRegistration("capabilities"));
    }
    CanonicalJson::from_serializable(&normalized)
        .map_err(StoreError::from)
        .map_err(SessionRegistryError::from)
}

const fn platform_name(platform: ExecutionClientPlatform) -> &'static str {
    match platform {
        ExecutionClientPlatform::Mt5 => "MT5",
        ExecutionClientPlatform::Binance => "BINANCE",
        ExecutionClientPlatform::Okx => "OKX",
        ExecutionClientPlatform::Ibkr => "IBKR",
        ExecutionClientPlatform::Paper => "PAPER",
        ExecutionClientPlatform::Backtest => "BACKTEST",
        ExecutionClientPlatform::Exchange => "EXCHANGE",
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        sync::{
            atomic::{AtomicI64, AtomicU64, Ordering},
            Arc,
        },
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use sinan_store::{SessionRouteQuery, SessionRouteResolution, StoreOptions};

    use super::*;
    use crate::{OutboundFrame, SinkWriteFuture, SinkWriteOutcome};

    static NEXT_DATABASE: AtomicU64 = AtomicU64::new(0);

    struct TestDatabase(PathBuf);

    impl TestDatabase {
        fn unique() -> Self {
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("test clock should be after Unix epoch")
                .as_nanos();
            let sequence = NEXT_DATABASE.fetch_add(1, Ordering::Relaxed);
            Self(std::env::temp_dir().join(format!(
                "sinan-gateway-session-{}-{timestamp}-{sequence}.sqlite",
                std::process::id()
            )))
        }

        fn url(&self) -> String {
            format!("sqlite://{}", self.0.display())
        }
    }

    impl Drop for TestDatabase {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
            let _ = fs::remove_file(format!("{}-wal", self.0.display()));
            let _ = fs::remove_file(format!("{}-shm", self.0.display()));
        }
    }

    struct ManualClock(AtomicI64);

    impl ManualClock {
        fn new(now: i64) -> Self {
            Self(AtomicI64::new(now))
        }

        fn set(&self, now: i64) {
            self.0.store(now, Ordering::Release);
        }
    }

    impl ServerClock for ManualClock {
        fn now_ms(&self) -> i64 {
            self.0.load(Ordering::Acquire)
        }
    }

    struct WrittenSink;

    impl OutboundSink for WrittenSink {
        fn write<'a>(&'a self, _frame: OutboundFrame) -> SinkWriteFuture<'a> {
            Box::pin(async { SinkWriteOutcome::Written })
        }
    }

    async fn test_store() -> (TestDatabase, SqliteStateStore) {
        let database = TestDatabase::unique();
        let mut options = StoreOptions::new(database.url());
        options.max_connections = 4;
        options.busy_timeout = Duration::from_secs(5);
        let store = SqliteStateStore::connect(options)
            .await
            .expect("gateway test store should connect");
        (database, store)
    }

    fn registration(session_id: &str) -> SessionRegistration {
        SessionRegistration {
            session_id: SessionId::from(session_id),
            client_id: ClientId::from("client_1"),
            account_id: AccountId::from("account_1"),
            terminal_id: Some(TerminalId::from("terminal_1")),
            platform: ExecutionClientPlatform::Mt5,
            capabilities: vec!["orders".to_owned(), "snapshots".to_owned()],
            remote_addr: Some("127.0.0.1:5000".to_owned()),
            max_inflight_commands: 8,
        }
    }

    fn registry(
        store: SqliteStateStore,
        live: Arc<LiveSessionRegistry>,
        clock: Arc<ManualClock>,
    ) -> GatewaySessionRegistry {
        GatewaySessionRegistry::new(
            store,
            live,
            clock,
            GatewaySessionConfig {
                max_clock_offset_ms: 250,
            },
        )
        .expect("gateway session config should be valid")
    }

    #[tokio::test]
    async fn concurrent_facades_publish_the_same_epoch_that_is_durably_active() {
        let (_database, store) = test_store().await;
        let live = Arc::new(LiveSessionRegistry::new());
        let clock = Arc::new(ManualClock::new(1_000));
        let first = registry(store.clone(), Arc::clone(&live), Arc::clone(&clock));
        let second = registry(store.clone(), Arc::clone(&live), Arc::clone(&clock));
        let barrier = Arc::new(tokio::sync::Barrier::new(3));

        let first_task = tokio::spawn({
            let barrier = Arc::clone(&barrier);
            async move {
                barrier.wait().await;
                first
                    .activate(registration("session_a"), Arc::new(WrittenSink))
                    .await
            }
        });
        let second_task = tokio::spawn({
            let barrier = Arc::clone(&barrier);
            async move {
                barrier.wait().await;
                second
                    .activate(registration("session_b"), Arc::new(WrittenSink))
                    .await
            }
        });
        barrier.wait().await;
        first_task.await.unwrap().unwrap();
        second_task.await.unwrap().unwrap();

        let first = store
            .get_session(&SessionId::from("session_a"))
            .await
            .unwrap()
            .unwrap();
        let second = store
            .get_session(&SessionId::from("session_b"))
            .await
            .unwrap()
            .unwrap();
        let (active, stale) = if first.status == SessionStatus::Active {
            (first, second)
        } else {
            (second, first)
        };

        assert_eq!(active.status, SessionStatus::Active);
        assert_eq!(stale.status, SessionStatus::Stale);
        assert!(live.handle(&active.session_id).is_some());
        assert!(live.handle(&stale.session_id).is_none());
    }

    #[tokio::test]
    async fn invalid_replacement_does_not_fence_the_healthy_active_transport() {
        let (_database, store) = test_store().await;
        let live = Arc::new(LiveSessionRegistry::new());
        let clock = Arc::new(ManualClock::new(1_000));
        let registry = registry(store.clone(), Arc::clone(&live), clock);
        registry
            .activate(registration("session_1"), Arc::new(WrittenSink))
            .await
            .unwrap();
        let handle = live.handle(&SessionId::from("session_1")).unwrap();
        let mut invalid = registration("session_2");
        invalid.capabilities = vec!["orders".to_owned(), "orders".to_owned()];

        assert!(matches!(
            registry.activate(invalid, Arc::new(WrittenSink)).await,
            Err(SessionRegistryError::InvalidRegistration("capabilities"))
        ));
        assert!(!handle.is_fenced());
        assert!(live.handle(&SessionId::from("session_1")).is_some());
        assert_eq!(
            store
                .get_session(&SessionId::from("session_1"))
                .await
                .unwrap()
                .unwrap()
                .status,
            SessionStatus::Active
        );
        assert!(store
            .get_session(&SessionId::from("session_2"))
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn concurrent_activation_and_disconnect_leave_live_and_durable_state_aligned() {
        let (_database, store) = test_store().await;
        let live = Arc::new(LiveSessionRegistry::new());
        let clock = Arc::new(ManualClock::new(1_000));
        let activator = registry(store.clone(), Arc::clone(&live), Arc::clone(&clock));
        let closer = registry(store.clone(), Arc::clone(&live), clock);
        let barrier = Arc::new(tokio::sync::Barrier::new(3));
        let session_id = SessionId::from("session_1");
        let close_session_id = session_id.clone();

        let activate_task = tokio::spawn({
            let barrier = Arc::clone(&barrier);
            async move {
                barrier.wait().await;
                activator
                    .activate(registration("session_1"), Arc::new(WrittenSink))
                    .await
            }
        });
        let close_task = tokio::spawn({
            let barrier = Arc::clone(&barrier);
            async move {
                barrier.wait().await;
                closer
                    .disconnect(&close_session_id, "TRANSPORT_CLOSED")
                    .await
            }
        });
        barrier.wait().await;
        activate_task.await.unwrap().unwrap();
        let close = close_task.await.unwrap();
        let durable = store.get_session(&session_id).await.unwrap().unwrap();

        match close {
            Ok(_) => {
                assert_eq!(durable.status, SessionStatus::Disconnected);
                assert!(live.handle(&session_id).is_none());
            }
            Err(SessionRegistryError::Store(StoreError::NotFound { .. })) => {
                assert_eq!(durable.status, SessionStatus::Active);
                assert!(live.handle(&session_id).is_some());
            }
            Err(error) => panic!("unexpected concurrent close error: {error}"),
        }
    }

    #[tokio::test]
    async fn invalid_time_evidence_persists_unsynced_before_returning_error() {
        let (_database, store) = test_store().await;
        let live = Arc::new(LiveSessionRegistry::new());
        let clock = Arc::new(ManualClock::new(1_000));
        let registry = registry(store.clone(), live, Arc::clone(&clock));
        registry
            .activate(registration("session_1"), Arc::new(WrittenSink))
            .await
            .unwrap();

        clock.set(1_100);
        registry
            .assess_heartbeat(
                &SessionId::from("session_1"),
                &HeartbeatPayload {
                    effective_server_now: 1_100,
                    clock_sync_status: ClockSyncStatus::Synced,
                    last_time_sync_at_server_ms: Some(1_050),
                    last_time_sync_rtt_ms: Some(10),
                    server_time_offset_ms: Some(0),
                    send_queue_depth: None,
                    command_inbox_depth: None,
                },
            )
            .await
            .unwrap();

        clock.set(1_200);
        let error = registry
            .assess_heartbeat(
                &SessionId::from("session_1"),
                &HeartbeatPayload {
                    effective_server_now: -1,
                    clock_sync_status: ClockSyncStatus::Synced,
                    last_time_sync_at_server_ms: None,
                    last_time_sync_rtt_ms: None,
                    server_time_offset_ms: None,
                    send_queue_depth: None,
                    command_inbox_depth: None,
                },
            )
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            SessionRegistryError::InvalidHeartbeat("effective_server_now")
        ));

        let stored = store
            .get_session(&SessionId::from("session_1"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.clock_sync_status, Some(ClockSyncStatus::Unsynced));
        let mut transaction = store.begin_write().await.unwrap();
        let route = transaction
            .resolve_session_route(SessionRouteQuery {
                account_id: AccountId::from("account_1"),
                client_id: Some(ClientId::from("client_1")),
                terminal_id: Some(TerminalId::from("terminal_1")),
                fresh_after: 1_000,
                require_synced_clock: true,
            })
            .await
            .unwrap();
        transaction.rollback().await.unwrap();
        assert!(matches!(
            route,
            SessionRouteResolution::ClockUnhealthy { candidate_count: 1 }
        ));
    }

    #[tokio::test]
    async fn startup_fence_clears_live_sinks_and_stales_durable_sessions() {
        let (_database, store) = test_store().await;
        let live = Arc::new(LiveSessionRegistry::new());
        let clock = Arc::new(ManualClock::new(1_000));
        let registry = registry(store.clone(), Arc::clone(&live), Arc::clone(&clock));
        registry
            .activate(registration("session_1"), Arc::new(WrittenSink))
            .await
            .unwrap();
        let handle = live.handle(&SessionId::from("session_1")).unwrap();

        clock.set(1_100);
        let report = registry.fence_startup("PROCESS_RESTART").await.unwrap();

        assert_eq!(report.sessions_staled, 1);
        assert!(handle.is_fenced());
        assert!(live.handle(&SessionId::from("session_1")).is_none());
        assert_eq!(
            store
                .get_session(&SessionId::from("session_1"))
                .await
                .unwrap()
                .unwrap()
                .status,
            SessionStatus::Stale
        );
    }
}
