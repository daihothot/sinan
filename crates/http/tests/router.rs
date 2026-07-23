use std::sync::{Arc, Mutex};

use axum::{
    body::{to_bytes, Body},
    http::{header, Method, Request, StatusCode},
    Router,
};
use serde_json::{json, Value};
use sinan_http::{
    control_plane_router, AuthorizedControlPlaneQuery, ClockHealth, CommandPayloadDisclosure,
    ControlPlaneFuture, ControlPlaneHttpState, ControlPlanePortError, ControlPlanePrincipal,
    ControlPlaneQueryPort, ControlPlaneScope, ControlPlaneTokenGrant,
    ExecutionCommandStatusResponse, ExecutionStateSummary, FixedBearerTokenRegistry,
    HttpErrorResponse, HttpServerClock, RiskStateSummary, ScopedExecutionCommandStatus,
    ScopedTradeIntentStatus, SubmitTradeIntentCommand, TradeIntentApplicationPort,
    TradeIntentIntakeOutcome, TradeIntentIntakeRecord, TradeIntentStatusResponse,
    TradingCoreStateResponse, TradingCoreTimePolicy,
};
use sinan_types::{
    AccountId, AccountSnapshot, CommandId, ErrorCode, ExecutionCommand, ExecutionCommandState,
    ExecutionCommandStatus, IntentId, TradeIntentStatus,
};
use tower::ServiceExt;

const TOKEN: &str = "control-plane-token";

#[derive(Clone)]
struct FixedClock(i64);

impl HttpServerClock for FixedClock {
    fn now_ms(&self) -> i64 {
        self.0
    }
}

#[derive(Clone)]
struct MockPorts {
    intake: Arc<Mutex<Result<TradeIntentIntakeOutcome, ControlPlanePortError>>>,
    submissions: Arc<Mutex<Vec<SubmitTradeIntentCommand>>>,
    state: Arc<Mutex<Result<TradingCoreStateResponse, ControlPlanePortError>>>,
    intent_status: Arc<Mutex<Result<Option<ScopedTradeIntentStatus>, ControlPlanePortError>>>,
    command_status: Arc<Mutex<Result<Option<ScopedExecutionCommandStatus>, ControlPlanePortError>>>,
    disclosures: Arc<Mutex<Vec<CommandPayloadDisclosure>>>,
    saw_authorized_account: Arc<Mutex<bool>>,
}

impl MockPorts {
    fn new() -> Self {
        Self {
            intake: Arc::new(Mutex::new(Ok(TradeIntentIntakeOutcome::Inserted(
                TradeIntentIntakeRecord {
                    accepted_at: 1_700_000_000_000,
                    state_ref: None,
                },
            )))),
            submissions: Arc::new(Mutex::new(Vec::new())),
            state: Arc::new(Mutex::new(Ok(empty_state()))),
            intent_status: Arc::new(Mutex::new(Ok(None))),
            command_status: Arc::new(Mutex::new(Ok(None))),
            disclosures: Arc::new(Mutex::new(Vec::new())),
            saw_authorized_account: Arc::new(Mutex::new(false)),
        }
    }
}

impl TradeIntentApplicationPort for MockPorts {
    fn submit_trade_intent(
        &self,
        command: SubmitTradeIntentCommand,
    ) -> ControlPlaneFuture<'_, Result<TradeIntentIntakeOutcome, ControlPlanePortError>> {
        self.submissions.lock().unwrap().push(command);
        let outcome = self.intake.lock().unwrap().clone();
        Box::pin(async move { outcome })
    }
}

impl ControlPlaneQueryPort for MockPorts {
    fn get_state(
        &self,
        query: AuthorizedControlPlaneQuery,
    ) -> ControlPlaneFuture<'_, Result<TradingCoreStateResponse, ControlPlanePortError>> {
        *self.saw_authorized_account.lock().unwrap() = query
            .account_scope()
            .contains(&AccountId::from("account-a"));
        let state = self.state.lock().unwrap().clone();
        Box::pin(async move { state })
    }

    fn get_time_policy(
        &self,
        query: AuthorizedControlPlaneQuery,
    ) -> ControlPlaneFuture<'_, Result<TradingCoreTimePolicy, ControlPlanePortError>> {
        *self.saw_authorized_account.lock().unwrap() = query
            .account_scope()
            .contains(&AccountId::from("account-a"));
        Box::pin(async {
            Ok(TradingCoreTimePolicy {
                clock_health: ClockHealth::Healthy,
                max_internal_server_skew_ms: 10,
                max_decision_time_skew_ms: 20,
                max_decision_time_sync_age_ms: 30,
                max_decision_time_sync_rtt_ms: 40,
                control_plane_time_sync_interval_ms: 50,
                max_decision_intent_age_ms: 60,
            })
        })
    }

    fn get_trade_intent_status(
        &self,
        query: AuthorizedControlPlaneQuery,
        _intent_id: IntentId,
    ) -> ControlPlaneFuture<'_, Result<Option<ScopedTradeIntentStatus>, ControlPlanePortError>>
    {
        *self.saw_authorized_account.lock().unwrap() = query
            .account_scope()
            .contains(&AccountId::from("account-a"));
        let response = self.intent_status.lock().unwrap().clone();
        Box::pin(async move { response })
    }

    fn get_execution_command_status(
        &self,
        query: AuthorizedControlPlaneQuery,
        _command_id: CommandId,
        disclosure: CommandPayloadDisclosure,
    ) -> ControlPlaneFuture<'_, Result<Option<ScopedExecutionCommandStatus>, ControlPlanePortError>>
    {
        *self.saw_authorized_account.lock().unwrap() = query
            .account_scope()
            .contains(&AccountId::from("account-a"));
        self.disclosures.lock().unwrap().push(disclosure);
        let response = self.command_status.lock().unwrap().clone();
        Box::pin(async move { response })
    }
}

fn app(ports: Arc<MockPorts>, scopes: impl IntoIterator<Item = ControlPlaneScope>) -> Router {
    let principal =
        ControlPlanePrincipal::new("control-ui", scopes, [AccountId::from("account-a")]);
    let registry =
        FixedBearerTokenRegistry::new([ControlPlaneTokenGrant::new(TOKEN, principal)]).unwrap();
    let application: Arc<dyn TradeIntentApplicationPort> = ports.clone();
    let queries: Arc<dyn ControlPlaneQueryPort> = ports;
    control_plane_router(
        ControlPlaneHttpState::new(registry, application, queries)
            .with_clock(Arc::new(FixedClock(1_800_000_000_000))),
    )
}

fn empty_state() -> TradingCoreStateResponse {
    TradingCoreStateResponse {
        server_time: 1_800_000_000_000,
        clock_health: ClockHealth::Healthy,
        accounts: Vec::new(),
        positions: Vec::new(),
        orders: Vec::new(),
        symbols: Vec::new(),
        sessions: Vec::new(),
        execution: ExecutionStateSummary {
            open_plans: Vec::new(),
            pending_commands: Vec::new(),
            recent_events: Vec::new(),
        },
        risk: RiskStateSummary {
            latest_results: Vec::new(),
            circuit_breaker_active: false,
            circuit_breaker: None,
        },
    }
}

fn intent_body() -> Value {
    json!({
        "intent": {
            "intent_id": "intent-1",
            "decision_id": "decision-1",
            "strategy_id": "strategy-1",
            "correlation_id": "correlation-1",
            "idempotency_key": "idempotency-1",
            "account_id": "account-a",
            "symbol": "EURUSD",
            "timeframe": "M5",
            "action": "BUY",
            "confidence": 0.8,
            "reason": "test",
            "proposed_risk_pct": 0.01,
            "proposed_legs": [{
                "leg_id": "leg-1",
                "symbol": "EURUSD",
                "action": "BUY",
                "ratio": 1.0
            }],
            "decision_timestamp": 1_799_999_999_000_i64,
            "signal_expires_at": 1_900_000_000_000_i64,
            "requested_at": 1_800_000_000_000_i64
        }
    })
}

fn request(method: Method, uri: &str, body: Body) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
        .header("x-request-id", "request-1")
        .body(body)
        .unwrap()
}

fn post_intent(body: Value) -> Request<Body> {
    let mut request = request(Method::POST, "/trade-intents", Body::from(body.to_string()));
    request.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/json"),
    );
    request.headers_mut().insert(
        "x-idempotency-key",
        header::HeaderValue::from_static("idempotency-1"),
    );
    request
}

async fn json_body(response: axum::response::Response) -> Value {
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn inserted_and_duplicate_intake_have_distinct_http_semantics() {
    let ports = Arc::new(MockPorts::new());
    let router = app(ports.clone(), [ControlPlaneScope::WriteIntent]);

    let response = router
        .clone()
        .oneshot(post_intent(intent_body()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    assert_eq!(response.headers()["x-request-id"], "request-1");
    let body = json_body(response).await;
    assert_eq!(body["status"], "ACCEPTED");
    assert_eq!(body["reason"], "OK");
    assert_eq!(ports.submissions.lock().unwrap().len(), 1);

    *ports.intake.lock().unwrap() = Ok(TradeIntentIntakeOutcome::Duplicate(
        TradeIntentIntakeRecord {
            accepted_at: 1_700_000_000_000,
            state_ref: None,
        },
    ));
    let response = router.oneshot(post_intent(intent_body())).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = json_body(response).await;
    assert_eq!(body["status"], "DUPLICATE");
    assert_eq!(body["reason"], "DUPLICATE_TRADE_INTENT");
}

#[tokio::test]
async fn authentication_and_scope_failures_are_uniform_json() {
    let ports = Arc::new(MockPorts::new());
    let router = app(ports.clone(), []);

    let request = Request::builder()
        .method(Method::GET)
        .uri("/state")
        .header("x-request-id", "request-1")
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(
        response.headers()[header::WWW_AUTHENTICATE],
        "Bearer realm=\"sinan-control-plane\""
    );
    let error: HttpErrorResponse = serde_json::from_value(json_body(response).await).unwrap();
    assert_eq!(error.error_code, ErrorCode::AuthenticationFailed);
    assert_eq!(error.request_id, "request-1");

    let response = router.oneshot(post_intent(intent_body())).await.unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let error: HttpErrorResponse = serde_json::from_value(json_body(response).await).unwrap();
    assert_eq!(error.error_code, ErrorCode::Forbidden);
    assert!(ports.submissions.lock().unwrap().is_empty());
}

#[tokio::test]
async fn request_id_and_idempotency_headers_are_enforced() {
    let ports = Arc::new(MockPorts::new());
    let router = app(ports.clone(), [ControlPlaneScope::WriteIntent]);

    let request = Request::builder()
        .method(Method::GET)
        .uri("/state")
        .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"))
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let error: HttpErrorResponse = serde_json::from_value(json_body(response).await).unwrap();
    assert_eq!(error.error_code, ErrorCode::MissingRequiredField);

    let mut request = post_intent(intent_body());
    request.headers_mut().insert(
        "x-idempotency-key",
        header::HeaderValue::from_static("different-key"),
    );
    let response = router.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::CONFLICT);
    let error: HttpErrorResponse = serde_json::from_value(json_body(response).await).unwrap();
    assert_eq!(error.error_code, ErrorCode::IdempotencyKeyConflict);
    assert!(ports.submissions.lock().unwrap().is_empty());
}

#[tokio::test]
async fn strict_json_rejects_missing_decision_time_and_unknown_fields() {
    let ports = Arc::new(MockPorts::new());
    let router = app(ports.clone(), [ControlPlaneScope::WriteIntent]);

    let mut wrapper_unknown = intent_body();
    wrapper_unknown["unknown"] = json!(true);
    let response = router
        .clone()
        .oneshot(post_intent(wrapper_unknown))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let mut missing_decision_time = intent_body();
    missing_decision_time["intent"]
        .as_object_mut()
        .unwrap()
        .remove("decision_timestamp");
    let response = router
        .clone()
        .oneshot(post_intent(missing_decision_time))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let error: HttpErrorResponse = serde_json::from_value(json_body(response).await).unwrap();
    assert_eq!(error.error_code, ErrorCode::SchemaValidationFailed);

    let mut leg_unknown = intent_body();
    leg_unknown["intent"]["proposed_legs"][0]["unknown"] = json!(true);
    let response = router.oneshot(post_intent(leg_unknown)).await.unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let error: HttpErrorResponse = serde_json::from_value(json_body(response).await).unwrap();
    assert_eq!(error.error_code, ErrorCode::SchemaValidationFailed);
    assert!(ports.submissions.lock().unwrap().is_empty());
}

#[tokio::test]
async fn state_is_plural_account_scoped_and_time_has_send_timestamp_invariant() {
    let ports = Arc::new(MockPorts::new());
    let mut state = empty_state();
    state.accounts = vec![account("account-a"), account("account-b")];
    state.risk.circuit_breaker_active = true;
    *ports.state.lock().unwrap() = Ok(state);
    let router = app(ports.clone(), [ControlPlaneScope::ReadState]);

    let response = router
        .clone()
        .oneshot(request(Method::GET, "/state", Body::empty()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = json_body(response).await;
    assert_eq!(body["accounts"].as_array().unwrap().len(), 1);
    assert_eq!(body["accounts"][0]["account_id"], "account-a");
    assert_eq!(body["risk"]["circuit_breaker_active"], true);
    assert!(*ports.saw_authorized_account.lock().unwrap());

    let response = router
        .oneshot(request(Method::GET, "/time", Body::empty()))
        .await
        .unwrap();
    let body = json_body(response).await;
    assert_eq!(body["server_now_ms"], body["server_send_at"]);
    assert_eq!(body["server_receive_at"], 1_800_000_000_000_i64);
}

#[tokio::test]
async fn detail_queries_do_not_disclose_objects_outside_account_scope() {
    let ports = Arc::new(MockPorts::new());
    *ports.intent_status.lock().unwrap() = Ok(Some(ScopedTradeIntentStatus {
        account_id: AccountId::from("account-b"),
        response: TradeIntentStatusResponse {
            intent_id: IntentId::from("intent-1"),
            status: TradeIntentStatus::Accepted,
            reason: None,
            risk_id: None,
            plan_id: None,
            command_ids: Vec::new(),
            created_at: 10,
            updated_at: 10,
        },
    }));
    let router = app(ports.clone(), [ControlPlaneScope::ReadState]);

    let response = router
        .oneshot(request(
            Method::GET,
            "/trade-intents/intent-1",
            Body::empty(),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let error: HttpErrorResponse = serde_json::from_value(json_body(response).await).unwrap();
    assert_eq!(error.error_code, ErrorCode::NotFound);
    assert!(*ports.saw_authorized_account.lock().unwrap());
}

#[tokio::test]
async fn command_payload_is_hidden_without_both_debug_scopes() {
    let ports = Arc::new(MockPorts::new());
    let command: ExecutionCommand = serde_json::from_value(json!({
        "command_id": "command-1",
        "strategy_id": "strategy-1",
        "account_id": "account-a",
        "symbol": "EURUSD",
        "action": "BUY",
        "magic": 42,
        "expires_at": 1_900_000_000_000_i64,
        "idempotency_key": "command-idempotency-1",
        "hmac": "secret-signature"
    }))
    .unwrap();
    *ports.command_status.lock().unwrap() = Ok(Some(ScopedExecutionCommandStatus {
        account_id: AccountId::from("account-a"),
        response: ExecutionCommandStatusResponse {
            command_id: CommandId::from("command-1"),
            state: ExecutionCommandState {
                command_id: CommandId::from("command-1"),
                account_id: AccountId::from("account-a"),
                plan_id: None,
                leg_id: None,
                status: ExecutionCommandStatus::Created,
                delivery_attempts: 0,
                last_delivery_error: None,
                created_at: 10,
                dispatched_at: None,
                command_received_at: None,
                reconciling_at: None,
                completed_at: None,
                updated_at: 10,
            },
            command: Some(command),
            events: Vec::new(),
        },
    }));
    let router = app(
        ports.clone(),
        [ControlPlaneScope::ReadState, ControlPlaneScope::DebugRead],
    );

    let response = router
        .oneshot(request(
            Method::GET,
            "/execution/commands/command-1",
            Body::empty(),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = json_body(response).await;
    assert!(body.get("command").is_none());
    assert_eq!(
        ports.disclosures.lock().unwrap().as_slice(),
        [CommandPayloadDisclosure::SummaryOnly]
    );
}

#[tokio::test]
async fn port_failures_map_to_422_429_and_503() {
    let ports = Arc::new(MockPorts::new());
    let router = app(ports.clone(), [ControlPlaneScope::WriteIntent]);
    let cases = [
        (
            ControlPlanePortError::Unprocessable {
                code: ErrorCode::TradeIntentExpired,
                message: "expired".to_owned(),
                details: None,
            },
            StatusCode::UNPROCESSABLE_ENTITY,
        ),
        (
            ControlPlanePortError::RateLimited {
                code: ErrorCode::RateLimited,
                message: "busy".to_owned(),
                details: None,
            },
            StatusCode::TOO_MANY_REQUESTS,
        ),
        (
            ControlPlanePortError::Unavailable {
                code: ErrorCode::StateStoreUnavailable,
                message: "store unavailable".to_owned(),
                details: None,
            },
            StatusCode::SERVICE_UNAVAILABLE,
        ),
    ];

    for (error, expected) in cases {
        *ports.intake.lock().unwrap() = Err(error);
        let response = router
            .clone()
            .oneshot(post_intent(intent_body()))
            .await
            .unwrap();
        assert_eq!(response.status(), expected);
        let _: HttpErrorResponse = serde_json::from_value(json_body(response).await).unwrap();
    }
}

#[tokio::test]
async fn unknown_routes_and_methods_keep_the_error_envelope() {
    let ports = Arc::new(MockPorts::new());
    let router = app(ports, [ControlPlaneScope::ReadState]);

    let response = router
        .clone()
        .oneshot(request(Method::GET, "/unknown", Body::empty()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let error: HttpErrorResponse = serde_json::from_value(json_body(response).await).unwrap();
    assert_eq!(error.error_code, ErrorCode::NotFound);

    let response = router
        .oneshot(request(Method::DELETE, "/state", Body::empty()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    let error: HttpErrorResponse = serde_json::from_value(json_body(response).await).unwrap();
    assert_eq!(error.error_code, ErrorCode::MethodNotAllowed);
}

fn account(account_id: &str) -> AccountSnapshot {
    AccountSnapshot {
        account_id: AccountId::from(account_id),
        balance: 10_000.0,
        equity: 10_000.0,
        margin: 0.0,
        free_margin: 10_000.0,
        currency: "USD".to_owned(),
        observed_at: 1_800_000_000_000,
    }
}
