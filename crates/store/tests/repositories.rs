use serde_json::json;
use sinan_store::{
    CanonicalJson, CommandStateUpdate, CoreEventMetadata, NewCoreEvent, NewExecutionCommand,
    NewExecutionEvent, NewSessionRecord, NewTradeIntent, NewWireInbox, NewWireOutbox,
    SqliteStateStore, StoreError, WriteOutcome,
};
use sinan_types::{
    AccountId, ClientId, ClockSyncStatus, CommandId, CorrelationId, DecisionId, ExecutionAction,
    ExecutionCommand, ExecutionCommandState, ExecutionCommandStatus, ExecutionEvent,
    ExecutionEventStatus, ExecutionId, FillingPolicy, IdempotencyKey, IntentId, MessageId,
    OrderType, RiskId, SessionId, SessionStatus, StrategyId, SymbolCode, TerminalId, TimePolicy,
    TimeframeCode, TradeIntent, TradeIntentAction, TradeIntentStatus, WireInboxStatus,
    WireOutboxStatus,
};
use sqlx::SqlitePool;

mod common;

use common::test_store;

fn core_event(event_id: &str, message_id: &str, value: i64) -> NewCoreEvent {
    NewCoreEvent {
        metadata: CoreEventMetadata {
            event_id: event_id.to_owned(),
            event_type: "trade.intent.accepted".to_owned(),
            aggregate_type: "trade_intent".to_owned(),
            aggregate_id: "intent_1".to_owned(),
            message_id: Some(MessageId::from(message_id)),
            schema_version: "ecp.v1.0".to_owned(),
            correlation_id: Some(CorrelationId::from("corr_1")),
            causation_id: None,
            account_id: Some(AccountId::from("account_1")),
            client_id: None,
            terminal_id: None,
            strategy_id: Some(StrategyId::from("strategy_1")),
            intent_id: Some(IntentId::from("intent_1")),
            plan_id: None,
            leg_id: None,
            command_id: None,
            idempotency_key: Some(IdempotencyKey::from("intent_key_1")),
            event_at: 1_000,
            received_at: 1_001,
            created_at: 1_002,
            source: "http".to_owned(),
        },
        payload: CanonicalJson::from_value(json!({"value": value})).unwrap(),
    }
}

fn trade_intent() -> TradeIntent {
    TradeIntent {
        intent_id: IntentId::from("intent_1"),
        decision_id: DecisionId::from("decision_1"),
        strategy_id: StrategyId::from("strategy_1"),
        correlation_id: CorrelationId::from("corr_1"),
        idempotency_key: IdempotencyKey::from("intent_key_1"),
        account_id: AccountId::from("account_1"),
        symbol: SymbolCode::from("XAUUSD"),
        timeframe: TimeframeCode::from("H4"),
        action: TradeIntentAction::Buy,
        confidence: 0.8,
        reason: "breakout".to_owned(),
        proposed_risk_pct: 1.0,
        proposed_sl: Some(2_320.5),
        proposed_tp: Some(2_365.5),
        proposed_legs: None,
        signal_expires_at: 5_000,
        requested_at: 1_000,
    }
}

fn new_trade_intent(intent: TradeIntent) -> NewTradeIntent {
    NewTradeIntent {
        intent,
        initial_status: TradeIntentStatus::Accepted,
        recorded_at: 1_010,
    }
}

async fn seed_risk(store: &SqliteStateStore, pool: &SqlitePool) {
    store
        .insert_trade_intent(new_trade_intent(trade_intent()))
        .await
        .expect("intent should insert");
    let payload = CanonicalJson::from_value(json!({"risk_id": "risk_1"})).unwrap();
    sqlx::query(
        "INSERT INTO risk_results (\
            risk_id, intent_id, account_id, approved, reason, snapshot_age_ms, \
            symbol_metadata_age_ms, evaluated_at, valid_until, payload_json, payload_hash\
         ) VALUES (?, ?, ?, 1, 'OK', 0, 0, 1100, 5000, ?, ?)",
    )
    .bind("risk_1")
    .bind("intent_1")
    .bind("account_1")
    .bind(payload.as_str())
    .bind(payload.sha256_hex())
    .execute(pool)
    .await
    .expect("risk fixture should insert");
}

fn execution_command() -> ExecutionCommand {
    ExecutionCommand {
        command_id: CommandId::from("command_1"),
        plan_id: None,
        leg_id: None,
        strategy_id: StrategyId::from("strategy_1"),
        account_id: AccountId::from("account_1"),
        terminal_id: None,
        client_id: None,
        symbol: SymbolCode::from("XAUUSD"),
        broker_symbol: Some("XAUUSD".to_owned()),
        action: ExecutionAction::Buy,
        order_type: Some(OrderType::Market),
        lots: Some(0.1),
        price: None,
        sl: Some(2_320.5),
        tp: Some(2_365.5),
        deviation_points: Some(20),
        magic: 26_052_601,
        comment: Some("strategy_1".to_owned()),
        position_ticket: None,
        broker_order_id: None,
        filling_policy: Some(FillingPolicy::Ioc),
        time_policy: Some(TimePolicy::Gtc),
        expiration_time: None,
        expires_at: 5_000,
        idempotency_key: IdempotencyKey::from("command_key_1"),
        hmac: "a".repeat(64),
    }
}

fn new_execution_command(command: ExecutionCommand) -> NewExecutionCommand {
    NewExecutionCommand {
        command,
        risk_id: RiskId::from("risk_1"),
        created_at: 1_100,
    }
}

fn execution_event() -> ExecutionEvent {
    ExecutionEvent {
        execution_id: ExecutionId::from("execution_1"),
        command_id: CommandId::from("command_1"),
        plan_id: None,
        leg_id: None,
        account_id: AccountId::from("account_1"),
        terminal_id: None,
        client_id: None,
        symbol: SymbolCode::from("XAUUSD"),
        broker_symbol: Some("XAUUSD".to_owned()),
        status: ExecutionEventStatus::Accepted,
        broker_order_id: None,
        broker_deal_id: None,
        position_ticket: None,
        idempotency_key: Some(IdempotencyKey::from("command_key_1")),
        requested_lots: Some(0.1),
        fill_price: None,
        filled_lots: None,
        remaining_lots: Some(0.1),
        event_at: 1_200,
        filled_at: None,
        broker_filled_at: None,
        error_code: None,
        message: None,
    }
}

fn session(session_id: &str) -> NewSessionRecord {
    NewSessionRecord {
        session_id: SessionId::from(session_id),
        client_id: ClientId::from("client_1"),
        account_id: AccountId::from("account_1"),
        terminal_id: Some(TerminalId::from("terminal_1")),
        platform: "MT5".to_owned(),
        status: SessionStatus::Active,
        capabilities: CanonicalJson::from_value(json!(["orders", "snapshots"])).unwrap(),
        remote_addr: Some("127.0.0.1:5000".to_owned()),
        connected_at: 1_000,
        last_heartbeat_at: None,
        last_time_sync_at: None,
        clock_sync_status: Some(ClockSyncStatus::Synced),
        disconnected_at: None,
    }
}

#[tokio::test]
async fn core_event_replay_ignores_only_ingest_timestamps() {
    let (_database, store, _) = test_store().await;
    let event = core_event("event_1", "message_1", 1);

    assert!(matches!(
        store.append_core_event(event.clone()).await.unwrap(),
        WriteOutcome::Inserted(_)
    ));

    let mut retry = event;
    retry.metadata.received_at += 100;
    retry.metadata.created_at += 100;
    assert!(matches!(
        store.append_core_event(retry).await.unwrap(),
        WriteOutcome::Duplicate(_)
    ));
}

#[tokio::test]
async fn core_event_requires_both_unique_ids_and_payload_to_match() {
    let (_database, store, pool) = test_store().await;
    store
        .append_core_event(core_event("event_1", "message_1", 1))
        .await
        .unwrap();

    let changed_message = store
        .append_core_event(core_event("event_1", "message_2", 1))
        .await
        .expect_err("same event id with another message id must conflict");
    assert!(matches!(
        changed_message,
        StoreError::IdentityConflict { .. }
    ));

    let changed_event = store
        .append_core_event(core_event("event_2", "message_1", 1))
        .await
        .expect_err("same message id with another event id must conflict");
    assert!(matches!(changed_event, StoreError::IdentityConflict { .. }));

    let changed_payload = store
        .append_core_event(core_event("event_1", "message_1", 2))
        .await
        .expect_err("same identities with another payload must conflict");
    assert!(matches!(
        changed_payload,
        StoreError::IdentityConflict { .. }
    ));

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM core_events")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn concurrent_core_event_replay_inserts_once() {
    let (_database, store, pool) = test_store().await;
    let event = core_event("event_1", "message_1", 1);
    let (left, right) = tokio::join!(
        store.append_core_event(event.clone()),
        store.append_core_event(event)
    );
    let outcomes = [left.unwrap(), right.unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| outcome.was_inserted())
            .count(),
        1
    );
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM core_events")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn trade_intent_enforces_primary_and_idempotency_keys() {
    let (_database, store, pool) = test_store().await;
    let intent = trade_intent();
    assert!(matches!(
        store
            .insert_trade_intent(new_trade_intent(intent.clone()))
            .await
            .unwrap(),
        WriteOutcome::Inserted(_)
    ));
    let mut retry = new_trade_intent(intent.clone());
    retry.recorded_at += 100;
    assert!(matches!(
        store.insert_trade_intent(retry).await.unwrap(),
        WriteOutcome::Duplicate(_)
    ));

    let mut same_key_changed_payload = intent.clone();
    same_key_changed_payload.reason = "changed".to_owned();
    assert!(matches!(
        store
            .insert_trade_intent(new_trade_intent(same_key_changed_payload))
            .await,
        Err(StoreError::IdentityConflict { .. })
    ));

    let mut same_id_changed_key = intent;
    same_id_changed_key.idempotency_key = IdempotencyKey::from("another_key");
    assert!(matches!(
        store
            .insert_trade_intent(new_trade_intent(same_id_changed_key))
            .await,
        Err(StoreError::IdentityConflict { .. })
    ));

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM trade_intents")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn typed_read_detects_denormalized_column_corruption() {
    let (_database, store, pool) = test_store().await;
    let intent = trade_intent();
    store
        .insert_trade_intent(new_trade_intent(intent.clone()))
        .await
        .unwrap();
    sqlx::query("UPDATE trade_intents SET symbol = 'OTHER' WHERE intent_id = ?")
        .bind(intent.intent_id.as_str())
        .execute(&pool)
        .await
        .unwrap();

    assert!(matches!(
        store.get_trade_intent(&intent.intent_id).await,
        Err(StoreError::CorruptData { .. })
    ));
}

#[tokio::test]
async fn command_and_execution_event_are_typed_idempotent_facts() {
    let (_database, store, pool) = test_store().await;
    seed_risk(&store, &pool).await;
    let command = execution_command();
    assert!(matches!(
        store
            .insert_execution_command(new_execution_command(command.clone()))
            .await
            .unwrap(),
        WriteOutcome::Inserted(_)
    ));
    let mut command_retry = new_execution_command(command.clone());
    command_retry.created_at += 10;
    assert!(matches!(
        store.insert_execution_command(command_retry).await.unwrap(),
        WriteOutcome::Duplicate(_)
    ));

    let mut conflict = command;
    conflict.idempotency_key = IdempotencyKey::from("changed_command_key");
    assert!(matches!(
        store
            .insert_execution_command(new_execution_command(conflict))
            .await,
        Err(StoreError::IdentityConflict { .. })
    ));

    let event = execution_event();
    let new_event = NewExecutionEvent {
        event: event.clone(),
        created_at: 1_210,
    };
    assert!(matches!(
        store
            .append_execution_event(new_event.clone())
            .await
            .unwrap(),
        WriteOutcome::Inserted(_)
    ));
    assert!(matches!(
        store.append_execution_event(new_event).await.unwrap(),
        WriteOutcome::Duplicate(_)
    ));
    let mut changed_event = event;
    changed_event.message = Some("changed".to_owned());
    assert!(matches!(
        store
            .append_execution_event(NewExecutionEvent {
                event: changed_event,
                created_at: 1_220,
            })
            .await,
        Err(StoreError::IdentityConflict { .. })
    ));
}

#[tokio::test]
async fn command_state_uses_compare_and_swap_without_owning_transitions() {
    let (_database, store, pool) = test_store().await;
    seed_risk(&store, &pool).await;
    let command = execution_command();
    store
        .insert_execution_command(new_execution_command(command.clone()))
        .await
        .unwrap();
    let created = ExecutionCommandState {
        command_id: command.command_id.clone(),
        account_id: command.account_id.clone(),
        plan_id: None,
        leg_id: None,
        status: ExecutionCommandStatus::Created,
        delivery_attempts: 0,
        last_delivery_error: None,
        created_at: 1_100,
        dispatched_at: None,
        command_received_at: None,
        reconciling_at: None,
        completed_at: None,
        updated_at: 1_100,
    };
    let mut identity_drift_on_insert = created.clone();
    identity_drift_on_insert.created_at += 1;
    assert!(matches!(
        store
            .insert_execution_command_state(identity_drift_on_insert)
            .await,
        Err(StoreError::IdentityConflict { .. })
    ));
    store
        .insert_execution_command_state(created.clone())
        .await
        .unwrap();

    let mut dispatched = created.clone();
    dispatched.status = ExecutionCommandStatus::Dispatched;
    dispatched.delivery_attempts = 1;
    dispatched.dispatched_at = Some(1_200);
    dispatched.updated_at = 1_200;
    let updated = store
        .update_execution_command_state(CommandStateUpdate {
            expected_status: ExecutionCommandStatus::Created,
            expected_updated_at: 1_100,
            state: dispatched.clone(),
        })
        .await
        .unwrap();
    assert_eq!(updated, dispatched);

    let stale = store
        .update_execution_command_state(CommandStateUpdate {
            expected_status: ExecutionCommandStatus::Created,
            expected_updated_at: 1_100,
            state: created,
        })
        .await;
    assert!(matches!(stale, Err(StoreError::StaleWrite { .. })));

    let mut identity_drift = dispatched;
    identity_drift.account_id = AccountId::from("another_account");
    let identity_error = store
        .update_execution_command_state(CommandStateUpdate {
            expected_status: ExecutionCommandStatus::Dispatched,
            expected_updated_at: 1_200,
            state: identity_drift,
        })
        .await;
    assert!(matches!(
        identity_error,
        Err(StoreError::IdentityConflict { .. })
    ));
}

#[tokio::test]
async fn command_state_insert_preserves_a_newer_projection_but_rejects_other_version_conflicts() {
    let (_database, store, pool) = test_store().await;
    seed_risk(&store, &pool).await;
    let command = execution_command();
    store
        .insert_execution_command(new_execution_command(command.clone()))
        .await
        .unwrap();
    let created = ExecutionCommandState {
        command_id: command.command_id.clone(),
        account_id: command.account_id.clone(),
        plan_id: None,
        leg_id: None,
        status: ExecutionCommandStatus::Created,
        delivery_attempts: 0,
        last_delivery_error: None,
        created_at: 1_100,
        dispatched_at: None,
        command_received_at: None,
        reconciling_at: None,
        completed_at: None,
        updated_at: 1_100,
    };
    store
        .insert_execution_command_state(created.clone())
        .await
        .unwrap();

    let mut dispatched = created.clone();
    dispatched.status = ExecutionCommandStatus::Dispatched;
    dispatched.delivery_attempts = 1;
    dispatched.dispatched_at = Some(1_200);
    dispatched.updated_at = 1_200;
    store
        .update_execution_command_state(CommandStateUpdate {
            expected_status: ExecutionCommandStatus::Created,
            expected_updated_at: 1_100,
            state: dispatched.clone(),
        })
        .await
        .unwrap();

    assert_eq!(
        store.insert_execution_command_state(created).await.unwrap(),
        WriteOutcome::Duplicate(dispatched.clone())
    );

    let mut same_version_conflict = dispatched.clone();
    same_version_conflict.last_delivery_error = Some("different content".to_owned());
    assert!(matches!(
        store
            .insert_execution_command_state(same_version_conflict)
            .await,
        Err(StoreError::IdentityConflict { .. })
    ));

    let mut incoming_newer = dispatched.clone();
    incoming_newer.status = ExecutionCommandStatus::Reconciling;
    incoming_newer.reconciling_at = Some(1_300);
    incoming_newer.updated_at = 1_300;
    assert!(matches!(
        store.insert_execution_command_state(incoming_newer).await,
        Err(StoreError::IdentityConflict { .. })
    ));
    assert_eq!(
        store
            .get_execution_command_state(&command.command_id)
            .await
            .unwrap(),
        Some(dispatched)
    );
}

#[tokio::test]
async fn command_state_compare_and_swap_requires_a_strictly_newer_version() {
    let (_database, store, pool) = test_store().await;
    seed_risk(&store, &pool).await;
    let command = execution_command();
    store
        .insert_execution_command(new_execution_command(command.clone()))
        .await
        .unwrap();
    let created = ExecutionCommandState {
        command_id: command.command_id.clone(),
        account_id: command.account_id.clone(),
        plan_id: None,
        leg_id: None,
        status: ExecutionCommandStatus::Created,
        delivery_attempts: 0,
        last_delivery_error: None,
        created_at: 1_100,
        dispatched_at: None,
        command_received_at: None,
        reconciling_at: None,
        completed_at: None,
        updated_at: 1_100,
    };
    store
        .insert_execution_command_state(created.clone())
        .await
        .unwrap();

    let mut same_version = created.clone();
    same_version.delivery_attempts = 1;
    same_version.last_delivery_error = Some("timeout".to_owned());
    let same_version_result = store
        .update_execution_command_state(CommandStateUpdate {
            expected_status: ExecutionCommandStatus::Created,
            expected_updated_at: 1_100,
            state: same_version,
        })
        .await;
    assert!(matches!(
        same_version_result,
        Err(StoreError::StaleWrite { .. })
    ));

    let mut older_version = created.clone();
    older_version.delivery_attempts = 2;
    older_version.updated_at = 1_099;
    let older_version_result = store
        .update_execution_command_state(CommandStateUpdate {
            expected_status: ExecutionCommandStatus::Created,
            expected_updated_at: 1_100,
            state: older_version,
        })
        .await;
    assert!(matches!(
        older_version_result,
        Err(StoreError::StaleWrite { .. })
    ));
    assert_eq!(
        store
            .get_execution_command_state(&command.command_id)
            .await
            .unwrap(),
        Some(created.clone())
    );

    let mut advanced = created;
    advanced.delivery_attempts = 1;
    advanced.last_delivery_error = Some("timeout".to_owned());
    advanced.updated_at = 1_200;
    let update = CommandStateUpdate {
        expected_status: ExecutionCommandStatus::Created,
        expected_updated_at: 1_100,
        state: advanced.clone(),
    };
    assert_eq!(
        store
            .update_execution_command_state(update.clone())
            .await
            .unwrap(),
        advanced
    );
    assert_eq!(
        store.update_execution_command_state(update).await.unwrap(),
        advanced
    );
}

#[tokio::test]
async fn concurrent_command_state_compare_and_swap_uses_version_when_status_is_unchanged() {
    let (_database, store, pool) = test_store().await;
    seed_risk(&store, &pool).await;
    let command = execution_command();
    store
        .insert_execution_command(new_execution_command(command.clone()))
        .await
        .unwrap();
    let created = ExecutionCommandState {
        command_id: command.command_id.clone(),
        account_id: command.account_id.clone(),
        plan_id: None,
        leg_id: None,
        status: ExecutionCommandStatus::Created,
        delivery_attempts: 0,
        last_delivery_error: None,
        created_at: 1_100,
        dispatched_at: None,
        command_received_at: None,
        reconciling_at: None,
        completed_at: None,
        updated_at: 1_100,
    };
    store
        .insert_execution_command_state(created.clone())
        .await
        .unwrap();

    let mut first_attempt = created.clone();
    first_attempt.delivery_attempts = 1;
    first_attempt.last_delivery_error = Some("first".to_owned());
    first_attempt.updated_at = 1_200;
    let mut second_attempt = created;
    second_attempt.delivery_attempts = 2;
    second_attempt.last_delivery_error = Some("second".to_owned());
    second_attempt.updated_at = 1_200;

    let (left, right) = tokio::join!(
        store.update_execution_command_state(CommandStateUpdate {
            expected_status: ExecutionCommandStatus::Created,
            expected_updated_at: 1_100,
            state: first_attempt,
        }),
        store.update_execution_command_state(CommandStateUpdate {
            expected_status: ExecutionCommandStatus::Created,
            expected_updated_at: 1_100,
            state: second_attempt,
        })
    );
    let results = [left, right];
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, Err(StoreError::StaleWrite { .. })))
            .count(),
        1
    );
}

#[tokio::test]
async fn wire_and_session_primitives_deduplicate_without_registry_behavior() {
    let (_database, store, _) = test_store().await;
    let session_record = session("session_1");
    assert!(matches!(
        store.insert_session(session_record.clone()).await.unwrap(),
        WriteOutcome::Inserted(_)
    ));
    assert!(matches!(
        store.insert_session(session_record.clone()).await.unwrap(),
        WriteOutcome::Duplicate(_)
    ));
    let mut status_drift = session_record.clone();
    status_drift.status = SessionStatus::Stale;
    assert!(matches!(
        store.insert_session(status_drift).await,
        Err(StoreError::IdentityConflict { .. })
    ));
    assert!(matches!(
        store.insert_session(session("session_2")).await,
        Err(StoreError::IdentityConflict { .. })
    ));

    let inbox = NewWireInbox {
        message_id: MessageId::from("in_1"),
        session_id: Some(session_record.session_id.clone()),
        message_type: "heartbeat".to_owned(),
        sequence: Some(1),
        received_at: 1_100,
        handled_at: None,
        status: WireInboxStatus::Received,
        wire_message: CanonicalJson::from_value(json!({"type": "heartbeat"})).unwrap(),
    };
    store.record_wire_inbox(inbox.clone()).await.unwrap();
    let mut inbox_retry = inbox;
    inbox_retry.received_at += 10;
    assert!(matches!(
        store.record_wire_inbox(inbox_retry).await.unwrap(),
        WriteOutcome::Duplicate(_)
    ));

    let outbox = NewWireOutbox {
        message_id: MessageId::from("out_1"),
        session_id: Some(session_record.session_id),
        message_type: "heartbeat".to_owned(),
        sequence: Some(1),
        command_id: None,
        payload: CanonicalJson::from_value(json!({"type": "heartbeat"})).unwrap(),
        status: WireOutboxStatus::Pending,
        created_at: 1_100,
        sent_at: None,
        acked_at: None,
        last_error: None,
    };
    store.enqueue_wire_outbox(outbox.clone()).await.unwrap();
    assert!(matches!(
        store.enqueue_wire_outbox(outbox).await.unwrap(),
        WriteOutcome::Duplicate(_)
    ));
}

#[tokio::test]
async fn wire_sequence_rejects_zero_and_sqlite_overflow() {
    let (_database, store, _) = test_store().await;
    let base = NewWireInbox {
        message_id: MessageId::from("in_1"),
        session_id: None,
        message_type: "session.hello".to_owned(),
        sequence: Some(0),
        received_at: 1_100,
        handled_at: None,
        status: WireInboxStatus::Received,
        wire_message: CanonicalJson::from_value(json!({"type": "session.hello"})).unwrap(),
    };
    assert!(matches!(
        store.record_wire_inbox(base.clone()).await,
        Err(StoreError::InvalidSequence { .. })
    ));
    let mut overflow = base;
    overflow.sequence = Some(u64::MAX);
    assert!(matches!(
        store.record_wire_inbox(overflow).await,
        Err(StoreError::InvalidInteger { .. })
    ));
}

#[tokio::test]
async fn explicit_transaction_rolls_back_all_prior_fact_writes() {
    let (_database, store, pool) = test_store().await;
    let intent = trade_intent();
    store
        .insert_trade_intent(new_trade_intent(intent.clone()))
        .await
        .unwrap();

    let mut conflicting = intent;
    conflicting.reason = "conflict".to_owned();
    let mut transaction = store.begin_write().await.unwrap();
    transaction
        .append_core_event(core_event("event_rollback", "message_rollback", 1))
        .await
        .unwrap();
    transaction
        .append_execution_event(NewExecutionEvent {
            event: execution_event(),
            created_at: 1_210,
        })
        .await
        .unwrap();
    let error = transaction
        .insert_trade_intent(new_trade_intent(conflicting))
        .await
        .expect_err("the final fact should conflict");
    assert!(matches!(error, StoreError::IdentityConflict { .. }));
    transaction.rollback().await.unwrap();

    assert!(store
        .get_core_event("event_rollback")
        .await
        .unwrap()
        .is_none());
    assert!(store
        .get_execution_event(&ExecutionId::from("execution_1"))
        .await
        .unwrap()
        .is_none());
    let intent_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM trade_intents")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(intent_count, 1);
}
