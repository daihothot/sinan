use std::{
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};

use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use sinan_events::{EventStreamManager, EventStreamManagerConfig};
use sinan_http::{
    control_plane_router, AuthorizedControlPlaneQuery, CommandPayloadDisclosure,
    ControlPlaneFuture, ControlPlaneHttpState, ControlPlanePortError, ControlPlanePrincipal,
    ControlPlaneQueryPort, ControlPlaneScope, ControlPlaneTokenGrant, EventWebSocketConfig,
    FixedBearerTokenRegistry, ScopedExecutionCommandStatus, ScopedTradeIntentStatus,
    SubmitTradeIntentCommand, TradeIntentApplicationPort, TradeIntentIntakeOutcome,
    TradingCoreStateResponse, TradingCoreTimePolicy,
};
use sinan_store::{CanonicalJson, NewEventStreamRecord, SqliteStateStore, StoreOptions};
use sinan_types::{AccountId, CommandId, EventStreamTopic, IntentId};
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, Message};

const TOKEN: &str = "event-token";
static NEXT_DATABASE: AtomicU64 = AtomicU64::new(0);

struct TestDatabase(PathBuf);

impl TestDatabase {
    fn new() -> Self {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let sequence = NEXT_DATABASE.fetch_add(1, Ordering::Relaxed);
        Self(std::env::temp_dir().join(format!(
            "sinan-http-events-{}-{nanos}-{sequence}.sqlite",
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

struct UnusedPorts;

impl TradeIntentApplicationPort for UnusedPorts {
    fn submit_trade_intent(
        &self,
        _command: SubmitTradeIntentCommand,
    ) -> ControlPlaneFuture<'_, Result<TradeIntentIntakeOutcome, ControlPlanePortError>> {
        Box::pin(async { Err(ControlPlanePortError::Internal) })
    }
}

impl ControlPlaneQueryPort for UnusedPorts {
    fn get_state(
        &self,
        _query: AuthorizedControlPlaneQuery,
    ) -> ControlPlaneFuture<'_, Result<TradingCoreStateResponse, ControlPlanePortError>> {
        Box::pin(async { Err(ControlPlanePortError::Internal) })
    }

    fn get_time_policy(
        &self,
        _query: AuthorizedControlPlaneQuery,
    ) -> ControlPlaneFuture<'_, Result<TradingCoreTimePolicy, ControlPlanePortError>> {
        Box::pin(async { Err(ControlPlanePortError::Internal) })
    }

    fn get_trade_intent_status(
        &self,
        _query: AuthorizedControlPlaneQuery,
        _intent_id: IntentId,
    ) -> ControlPlaneFuture<'_, Result<Option<ScopedTradeIntentStatus>, ControlPlanePortError>>
    {
        Box::pin(async { Err(ControlPlanePortError::Internal) })
    }

    fn get_execution_command_status(
        &self,
        _query: AuthorizedControlPlaneQuery,
        _command_id: CommandId,
        _disclosure: CommandPayloadDisclosure,
    ) -> ControlPlaneFuture<'_, Result<Option<ScopedExecutionCommandStatus>, ControlPlanePortError>>
    {
        Box::pin(async { Err(ControlPlanePortError::Internal) })
    }
}

fn event(event_id: &str, account_id: Option<&str>, value: i64) -> NewEventStreamRecord {
    NewEventStreamRecord {
        event_id: event_id.to_owned(),
        topic: EventStreamTopic::SystemEvent,
        account_id: account_id.map(AccountId::from),
        event_type: "system.test".to_owned(),
        payload: CanonicalJson::from_value(json!({"value": value})).unwrap(),
        created_at: value,
    }
}

async fn server(
    scopes: impl IntoIterator<Item = ControlPlaneScope>,
) -> (
    TestDatabase,
    Arc<EventStreamManager>,
    String,
    tokio::task::JoinHandle<()>,
) {
    server_with_manager_config(scopes, EventStreamManagerConfig::default()).await
}

async fn server_with_manager_config(
    scopes: impl IntoIterator<Item = ControlPlaneScope>,
    manager_config: EventStreamManagerConfig,
) -> (
    TestDatabase,
    Arc<EventStreamManager>,
    String,
    tokio::task::JoinHandle<()>,
) {
    let database = TestDatabase::new();
    let store = SqliteStateStore::connect(StoreOptions::new(database.url()))
        .await
        .unwrap();
    let manager = Arc::new(EventStreamManager::new(store, manager_config).unwrap());
    let principal =
        ControlPlanePrincipal::new("event-test", scopes, [AccountId::from("account-a")]);
    let registry =
        FixedBearerTokenRegistry::new([ControlPlaneTokenGrant::new(TOKEN, principal)]).unwrap();
    let ports = Arc::new(UnusedPorts);
    let state = ControlPlaneHttpState::new(registry, ports.clone(), ports)
        .with_event_stream(Arc::clone(&manager), EventWebSocketConfig::default())
        .unwrap();
    let app = control_plane_router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (database, manager, format!("ws://{address}/events"), handle)
}

fn request(url: &str, with_token: bool) -> tokio_tungstenite::tungstenite::http::Request<()> {
    let mut request = url.into_client_request().unwrap();
    request
        .headers_mut()
        .insert("x-request-id", "event-request-1".parse().unwrap());
    if with_token {
        request
            .headers_mut()
            .insert("authorization", format!("Bearer {TOKEN}").parse().unwrap());
    }
    request
}

async fn recv_json<S>(socket: &mut tokio_tungstenite::WebSocketStream<S>) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let message = tokio::time::timeout(std::time::Duration::from_secs(2), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    serde_json::from_str(message.to_text().unwrap()).unwrap()
}

#[tokio::test]
async fn event_websocket_replays_then_continues_live_with_account_filtering() {
    let (_database, manager, url, server) = server([ControlPlaneScope::SubscribeEvents]).await;
    manager
        .publish(event("anchor", Some("account-a"), 1))
        .await
        .unwrap();
    manager
        .publish(event("hidden", Some("account-b"), 2))
        .await
        .unwrap();
    manager.publish(event("replayed", None, 3)).await.unwrap();

    let (mut socket, _) = tokio_tungstenite::connect_async(request(&url, true))
        .await
        .unwrap();
    socket
        .send(Message::Text(
            json!({
                "op": "subscribe",
                "topics": ["system.event"],
                "last_event_id": "anchor"
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
    let subscribed = recv_json(&mut socket).await;
    assert_eq!(subscribed["status"], "SUBSCRIBED");
    assert_eq!(subscribed["requires_state_reload"], false);

    let replayed = recv_json(&mut socket).await;
    assert_eq!(replayed["op"], "event");
    assert_eq!(replayed["event_id"], "replayed");
    assert_eq!(replayed["payload"]["value"], 3);

    manager
        .publish(event("live", Some("account-a"), 4))
        .await
        .unwrap();
    let live = recv_json(&mut socket).await;
    assert_eq!(live["event_id"], "live");
    socket.close(None).await.unwrap();
    server.abort();
}

#[tokio::test]
async fn event_websocket_rejects_auth_and_scope_before_upgrade() {
    let (_database, _manager, url, server) = server([]).await;
    let missing = tokio_tungstenite::connect_async(request(&url, false))
        .await
        .expect_err("missing bearer token must reject the HTTP upgrade");
    assert_eq!(http_error_status(missing), 401);
    let forbidden = tokio_tungstenite::connect_async(request(&url, true))
        .await
        .expect_err("missing event:subscribe scope must reject the HTTP upgrade");
    assert_eq!(http_error_status(forbidden), 403);
    server.abort();
}

#[tokio::test]
async fn replay_limit_gap_sends_recovery_response_then_closes_connection() {
    let (_database, manager, url, server) = server_with_manager_config(
        [ControlPlaneScope::SubscribeEvents],
        EventStreamManagerConfig {
            live_capacity: 8,
            replay_limit: 1,
        },
    )
    .await;
    manager
        .publish(event("anchor", Some("account-a"), 1))
        .await
        .unwrap();
    manager
        .publish(event("first-missed", Some("account-a"), 2))
        .await
        .unwrap();
    manager
        .publish(event("second-missed", Some("account-a"), 3))
        .await
        .unwrap();

    let (mut socket, _) = tokio_tungstenite::connect_async(request(&url, true))
        .await
        .unwrap();
    socket
        .send(Message::Text(
            json!({
                "op": "subscribe",
                "topics": ["system.event"],
                "account_id": "account-a",
                "last_event_id": "anchor"
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();

    let response = recv_json(&mut socket).await;
    assert_eq!(response["status"], "RESUME_FAILED");
    assert_eq!(response["reason"], "GAP_DETECTED");
    assert_eq!(response["requires_state_reload"], true);

    let close = tokio::time::timeout(std::time::Duration::from_secs(2), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let Message::Close(Some(frame)) = close else {
        panic!("expected a close frame after GAP_DETECTED, got {close:?}");
    };
    assert_eq!(u16::from(frame.code), 1013);
    assert_eq!(frame.reason, "event cursor recovery required");
    server.abort();
}

#[tokio::test]
async fn unauthorized_account_subscription_cannot_expand_principal_scope() {
    let (_database, _manager, url, server) = server([ControlPlaneScope::SubscribeEvents]).await;
    let (mut socket, _) = tokio_tungstenite::connect_async(request(&url, true))
        .await
        .unwrap();
    socket
        .send(Message::Text(
            json!({
                "op": "subscribe",
                "topics": ["system.event"],
                "account_id": "account-b"
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();

    let rejected = recv_json(&mut socket).await;
    assert_eq!(rejected["status"], "RESUME_FAILED");
    assert_eq!(rejected["reason"], "UNAUTHORIZED");
    assert_eq!(rejected["requires_state_reload"], true);

    socket
        .send(Message::Text(
            json!({
                "op": "subscribe",
                "topics": ["system.event"],
                "account_id": "account-a"
            })
            .to_string()
            .into(),
        ))
        .await
        .unwrap();
    let subscribed = recv_json(&mut socket).await;
    assert_eq!(subscribed["status"], "SUBSCRIBED");
    socket.close(None).await.unwrap();
    server.abort();
}

fn http_error_status(error: tokio_tungstenite::tungstenite::Error) -> u16 {
    match error {
        tokio_tungstenite::tungstenite::Error::Http(response) => response.status().as_u16(),
        other => panic!("expected an HTTP upgrade rejection, got {other}"),
    }
}
