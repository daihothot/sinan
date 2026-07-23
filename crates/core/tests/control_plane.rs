use std::{
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};

use sinan_core::{SqliteControlPlaneService, SqliteControlPlaneServiceConfig, TradingCoreClock};
use sinan_http::{
    AuthorizedControlPlaneQuery, ControlPlanePortError, ControlPlanePrincipal,
    ControlPlaneQueryPort, ControlPlaneScope, SubmitTradeIntentCommand, TradeIntentApplicationPort,
    TradeIntentIntakeOutcome,
};
use sinan_store::{SqliteStateStore, StoreOptions};
use sinan_types::{
    AccountId, CorrelationId, DecisionId, IdempotencyKey, IntentId, StrategyId, SymbolCode,
    TimeframeCode, TradeIntent, TradeIntentAction,
};

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
            "sinan-core-control-plane-{}-{nanos}-{sequence}.sqlite",
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

#[derive(Clone)]
struct FixedClock(i64);

impl TradingCoreClock for FixedClock {
    fn now_ms(&self) -> i64 {
        self.0
    }
}

fn principal(account_id: &str) -> ControlPlanePrincipal {
    ControlPlanePrincipal::new(
        "control-plane-test",
        [ControlPlaneScope::WriteIntent, ControlPlaneScope::ReadState],
        [AccountId::from(account_id)],
    )
}

fn intent(intent_id: &str, idempotency_key: &str) -> TradeIntent {
    TradeIntent {
        intent_id: IntentId::from(intent_id),
        decision_id: DecisionId::from("decision-1"),
        strategy_id: StrategyId::from("strategy-1"),
        correlation_id: CorrelationId::from("correlation-1"),
        idempotency_key: IdempotencyKey::from(idempotency_key),
        account_id: AccountId::from("account-1"),
        symbol: SymbolCode::from("EURUSD"),
        timeframe: TimeframeCode::from("M1"),
        action: TradeIntentAction::Buy,
        confidence: 0.8,
        reason: "test".to_owned(),
        proposed_risk_pct: 0.01,
        proposed_sl: Some(1.09),
        proposed_tp: Some(1.12),
        proposed_legs: None,
        decision_timestamp: 900,
        signal_expires_at: 2_000,
        requested_at: 1_000,
    }
}

async fn service() -> (TestDatabase, SqliteControlPlaneService) {
    let database = TestDatabase::new();
    let store = SqliteStateStore::connect(StoreOptions::new(database.url()))
        .await
        .unwrap();
    let service = SqliteControlPlaneService::new(
        store,
        Arc::new(FixedClock(1_500)),
        SqliteControlPlaneServiceConfig::default(),
    )
    .unwrap();
    (database, service)
}

fn submit(intent: TradeIntent) -> SubmitTradeIntentCommand {
    SubmitTradeIntentCommand {
        principal: principal("account-1"),
        request_id: "request-1".to_owned(),
        correlation_id: None,
        intent,
    }
}

#[tokio::test]
async fn durable_intake_is_idempotent_without_claiming_risk_or_execution_completion() {
    let (_database, service) = service().await;
    let first = service
        .submit_trade_intent(submit(intent("intent-1", "key-1")))
        .await
        .unwrap();
    assert!(matches!(first, TradeIntentIntakeOutcome::Inserted(_)));

    let duplicate = service
        .submit_trade_intent(submit(intent("intent-1", "key-1")))
        .await
        .unwrap();
    let TradeIntentIntakeOutcome::Duplicate(record) = duplicate else {
        panic!("same durable intent should be a duplicate");
    };
    assert!(record.state_ref.is_none());

    let conflict = service
        .submit_trade_intent(submit(intent("intent-2", "key-1")))
        .await
        .unwrap_err();
    assert!(matches!(conflict, ControlPlanePortError::Conflict { .. }));
}

#[tokio::test]
async fn state_and_status_queries_apply_the_principal_scope() {
    let (_database, service) = service().await;
    service
        .submit_trade_intent(submit(intent("intent-1", "key-1")))
        .await
        .unwrap();

    let authorized = AuthorizedControlPlaneQuery {
        principal: principal("account-1"),
        request_id: "request-2".to_owned(),
        correlation_id: None,
    };
    assert!(service
        .get_trade_intent_status(authorized.clone(), IntentId::from("intent-1"))
        .await
        .unwrap()
        .is_some());

    let unauthorized = AuthorizedControlPlaneQuery {
        principal: principal("account-2"),
        request_id: "request-3".to_owned(),
        correlation_id: None,
    };
    assert!(service
        .get_trade_intent_status(unauthorized, IntentId::from("intent-1"))
        .await
        .unwrap()
        .is_none());

    let state = service.get_state(authorized).await.unwrap();
    assert!(state.accounts.is_empty());
    assert!(state.execution.open_plans.is_empty());
    assert!(state.risk.latest_results.is_empty());
}
