use serde_json::json;
use sinan_protocol::{ReconciliationReason, ReconciliationRequest, ReconciliationResult};
use sinan_store::{
    AuthorizedAccountScope, CanonicalJson, CoreEventMetadata, ManualReconciliationEvidence,
    NewCoreEvent, NewManualReconciliationEscalation, NewReconciliationResult, NewReconciliationRun,
    ProjectionWriteOutcome, ReconciliationCompleteness, ReconciliationDisposition,
    ReconciliationEvaluation, ReconciliationRunStatus, StoreError, WriteOutcome,
};
use sinan_types::{
    AccountId, AccountSnapshot, BrokerOrderId, ClientId, CommandId, OrderSnapshot,
    OrderSnapshotStatus, OrderType, PositionId, PositionSide, PositionSnapshot, RequestId,
    SymbolCode, SymbolMetadataSnapshot, SymbolTradeMode, TerminalId,
};

mod common;

use common::test_store;

const REQUESTED_AT: i64 = 1_000;
const OBSERVED_AT: i64 = 1_100;

fn request(request_id: &str, command_ids: Option<Vec<&str>>) -> ReconciliationRequest {
    ReconciliationRequest {
        request_id: RequestId::from(request_id),
        account_id: AccountId::from("account-1"),
        terminal_id: None,
        client_id: None,
        reason: ReconciliationReason::ManualRequest,
        command_ids: command_ids.map(|values| values.into_iter().map(CommandId::from).collect()),
        since_server_time: Some(900),
    }
}

fn event_metadata(
    event_id: &str,
    event_type: &str,
    request_id: &str,
    account_id: &str,
    event_at: i64,
) -> CoreEventMetadata {
    CoreEventMetadata {
        event_id: event_id.to_owned(),
        event_type: event_type.to_owned(),
        aggregate_type: "reconciliation".to_owned(),
        aggregate_id: request_id.to_owned(),
        message_id: None,
        schema_version: "ecp.v1.0".to_owned(),
        correlation_id: None,
        causation_id: None,
        account_id: Some(AccountId::from(account_id)),
        client_id: None,
        terminal_id: None,
        strategy_id: None,
        intent_id: None,
        plan_id: None,
        leg_id: None,
        command_id: None,
        idempotency_key: None,
        event_at,
        received_at: event_at + 1,
        created_at: event_at + 2,
        source: "reconciliation-test".to_owned(),
    }
}

fn new_run(request_id: &str, command_ids: Option<Vec<&str>>) -> NewReconciliationRun {
    NewReconciliationRun {
        request: request(request_id, command_ids),
        requested_at: REQUESTED_AT,
        event_metadata: event_metadata(
            &format!("request-event-{request_id}"),
            "reconciliation.request",
            request_id,
            "account-1",
            REQUESTED_AT,
        ),
    }
}

fn account(observed_at: i64, equity: f64) -> AccountSnapshot {
    AccountSnapshot {
        account_id: AccountId::from("account-1"),
        balance: equity - 100.0,
        equity,
        margin: 50.0,
        free_margin: equity - 50.0,
        currency: "USD".to_owned(),
        observed_at,
    }
}

fn position(position_id: &str, observed_at: i64, lots: f64) -> PositionSnapshot {
    PositionSnapshot {
        account_id: AccountId::from("account-1"),
        symbol: SymbolCode::from("EURUSD"),
        position_id: PositionId::from(position_id),
        side: PositionSide::Buy,
        lots,
        open_price: 1.1,
        sl: Some(1.09),
        tp: Some(1.12),
        floating_pnl: 12.0,
        observed_at,
    }
}

fn order(order_id: &str, observed_at: i64) -> OrderSnapshot {
    OrderSnapshot {
        account_id: AccountId::from("account-1"),
        terminal_id: None,
        client_id: None,
        symbol: SymbolCode::from("EURUSD"),
        broker_symbol: Some("EURUSD.a".to_owned()),
        broker_order_id: BrokerOrderId::from(order_id),
        position_ticket: None,
        command_id: None,
        plan_id: None,
        leg_id: None,
        idempotency_key: None,
        side: PositionSide::Buy,
        order_type: OrderType::Limit,
        status: OrderSnapshotStatus::Placed,
        requested_lots: 0.25,
        filled_lots: 0.0,
        remaining_lots: 0.25,
        price: Some(1.1),
        sl: Some(1.09),
        tp: Some(1.12),
        created_at: Some(observed_at - 100),
        updated_at: Some(observed_at),
        observed_at,
    }
}

fn symbol(observed_at: i64) -> SymbolMetadataSnapshot {
    SymbolMetadataSnapshot {
        account_id: AccountId::from("account-1"),
        symbol: SymbolCode::from("EURUSD"),
        broker_symbol: "EURUSD.a".to_owned(),
        digits: 5,
        point: 0.00001,
        tick_size: 0.00001,
        tick_value_loss: 1.0,
        contract_size: 100_000.0,
        volume_min: 0.01,
        volume_max: 100.0,
        volume_step: 0.01,
        stops_level_points: 10,
        freeze_level_points: 5,
        margin_initial: Some(1_000.0),
        margin_maintenance: Some(500.0),
        trade_mode: SymbolTradeMode::Full,
        observed_at,
    }
}

fn result(request_id: &str, observed_at: i64) -> ReconciliationResult {
    ReconciliationResult {
        request_id: RequestId::from(request_id),
        account_id: AccountId::from("account-1"),
        terminal_id: None,
        client_id: None,
        observed_at,
        account: None,
        positions: Vec::new(),
        orders: Vec::new(),
        symbol_metadata: Vec::new(),
        unresolved_command_ids: Vec::new(),
    }
}

fn evaluation(
    request_id: &str,
    observed_at: Option<i64>,
    disposition: ReconciliationDisposition,
    command_ids: Vec<&str>,
) -> ReconciliationEvaluation {
    ReconciliationEvaluation {
        request_id: RequestId::from(request_id),
        account_id: AccountId::from("account-1"),
        observed_at,
        disposition,
        command_ids: command_ids.into_iter().map(CommandId::from).collect(),
        findings: Vec::new(),
    }
}

fn result_bundle(
    request_id: &str,
    result: ReconciliationResult,
    disposition: ReconciliationDisposition,
    command_ids: Vec<&str>,
    symbol_metadata_complete: bool,
    command_scope_complete: bool,
) -> NewReconciliationResult {
    let observed_at = result.observed_at;
    NewReconciliationResult {
        result,
        evaluation: evaluation(request_id, Some(observed_at), disposition, command_ids),
        completeness: ReconciliationCompleteness {
            symbol_metadata_complete,
            command_scope_complete,
        },
        event_metadata: event_metadata(
            &format!("result-event-{request_id}"),
            "reconciliation.result",
            request_id,
            "account-1",
            observed_at,
        ),
    }
}

fn snapshot_metadata(event_id: &str, event_type: &str, event_at: i64) -> CoreEventMetadata {
    let mut metadata = event_metadata(event_id, event_type, "account-1", "account-1", event_at);
    metadata.aggregate_type = event_type.to_owned();
    metadata.aggregate_id = "account-1".to_owned();
    metadata
}

#[tokio::test]
async fn request_is_canonical_atomic_idempotent_and_transport_neutral() {
    let (_database, store, raw) = test_store().await;
    let run = new_run("run-create", Some(vec!["cmd-a", "cmd-b"]));

    assert!(matches!(
        store.create_reconciliation_run(run.clone()).await.unwrap(),
        WriteOutcome::Inserted(_)
    ));
    let mut replay = run.clone();
    replay.event_metadata.received_at += 10;
    replay.event_metadata.created_at += 10;
    assert!(matches!(
        store.create_reconciliation_run(replay).await.unwrap(),
        WriteOutcome::Duplicate(_)
    ));

    let stored = store
        .get_reconciliation_run(&RequestId::from("run-create"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.request, run.request);
    assert_eq!(stored.status, ReconciliationRunStatus::Requested);
    let outbox: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM wire_outbox")
        .fetch_one(&raw)
        .await
        .unwrap();
    let request_events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM core_events WHERE event_type = 'reconciliation.request'",
    )
    .fetch_one(&raw)
    .await
    .unwrap();
    assert_eq!(outbox, 0);
    assert_eq!(request_events, 1);
}

#[tokio::test]
async fn transaction_owned_reconciliation_run_follows_owner_commit_or_rollback() {
    let (_database, store, raw) = test_store().await;

    let mut rolled_back = store.begin_write().await.unwrap();
    assert!(matches!(
        rolled_back
            .create_reconciliation_run(new_run("run-owner-rollback", None))
            .await
            .unwrap(),
        WriteOutcome::Inserted(_)
    ));
    assert!(rolled_back
        .get_reconciliation_run(&RequestId::from("run-owner-rollback"))
        .await
        .unwrap()
        .is_some());
    rolled_back.rollback().await.unwrap();

    assert!(store
        .get_reconciliation_run(&RequestId::from("run-owner-rollback"))
        .await
        .unwrap()
        .is_none());
    let rolled_back_events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM core_events WHERE aggregate_id = 'run-owner-rollback'",
    )
    .fetch_one(&raw)
    .await
    .unwrap();
    assert_eq!(rolled_back_events, 0);

    let mut committed = store.begin_write().await.unwrap();
    committed
        .create_reconciliation_run(new_run("run-owner-commit", None))
        .await
        .unwrap();
    committed.commit().await.unwrap();

    assert!(store
        .get_reconciliation_run(&RequestId::from("run-owner-commit"))
        .await
        .unwrap()
        .is_some());
    let committed_events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM core_events WHERE aggregate_id = 'run-owner-commit'",
    )
    .fetch_one(&raw)
    .await
    .unwrap();
    assert_eq!(committed_events, 1);
}

#[tokio::test]
async fn request_and_event_time_boundaries_fail_before_writing() {
    let (_database, store, raw) = test_store().await;
    let mut unsorted = new_run("run-unsorted", Some(vec!["cmd-b", "cmd-a"]));
    assert!(matches!(
        store.create_reconciliation_run(unsorted.clone()).await,
        Err(StoreError::InvalidRecord { .. })
    ));
    unsorted.request.command_ids = Some(vec![CommandId::from("cmd-a")]);
    unsorted.request.since_server_time = Some(REQUESTED_AT + 1);
    assert!(matches!(
        store.create_reconciliation_run(unsorted).await,
        Err(StoreError::InvalidRecord { .. })
    ));

    let mut bad_time = new_run("run-bad-time", None);
    bad_time.event_metadata.received_at = REQUESTED_AT - 1;
    assert!(matches!(
        store.create_reconciliation_run(bad_time).await,
        Err(StoreError::IdentityConflict { .. })
    ));
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM reconciliation_runs")
        .fetch_one(&raw)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn unresolved_commands_require_pending_attention_scope() {
    let (_database, store, raw) = test_store().await;
    store
        .create_reconciliation_run(new_run("run-unresolved", Some(vec!["cmd-a"])))
        .await
        .unwrap();
    let mut unresolved = result("run-unresolved", OBSERVED_AT);
    unresolved
        .unresolved_command_ids
        .push(CommandId::from("cmd-a"));

    assert!(matches!(
        store
            .commit_reconciliation_result(result_bundle(
                "run-unresolved",
                unresolved.clone(),
                ReconciliationDisposition::Completed,
                Vec::new(),
                false,
                true,
            ))
            .await,
        Err(StoreError::InvalidRecord { .. })
    ));
    assert!(matches!(
        store
            .commit_reconciliation_result(result_bundle(
                "run-unresolved",
                unresolved.clone(),
                ReconciliationDisposition::PendingEvidence,
                Vec::new(),
                false,
                true,
            ))
            .await,
        Err(StoreError::InvalidRecord { .. })
    ));
    store
        .commit_reconciliation_result(result_bundle(
            "run-unresolved",
            unresolved,
            ReconciliationDisposition::PendingEvidence,
            vec!["cmd-a"],
            false,
            false,
        ))
        .await
        .unwrap();
    let result_events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM core_events WHERE event_id = 'result-event-run-unresolved'",
    )
    .fetch_one(&raw)
    .await
    .unwrap();
    assert_eq!(result_events, 1);

    store
        .create_reconciliation_run(new_run("run-pending-empty", None))
        .await
        .unwrap();
    assert!(matches!(
        store
            .commit_reconciliation_result(result_bundle(
                "run-pending-empty",
                result("run-pending-empty", OBSERVED_AT),
                ReconciliationDisposition::PendingEvidence,
                Vec::new(),
                false,
                true,
            ))
            .await,
        Err(StoreError::InvalidRecord { .. })
    ));
}

#[tokio::test]
async fn typed_run_read_rejects_self_consistent_evaluation_and_completeness_drift() {
    let (_database, store, raw) = test_store().await;
    store
        .create_reconciliation_run(new_run("run-typed-pending", None))
        .await
        .unwrap();
    store
        .commit_reconciliation_result(result_bundle(
            "run-typed-pending",
            result("run-typed-pending", OBSERVED_AT),
            ReconciliationDisposition::PendingEvidence,
            vec!["cmd-a"],
            false,
            true,
        ))
        .await
        .unwrap();
    let empty_pending = CanonicalJson::from_serializable(&evaluation(
        "run-typed-pending",
        Some(OBSERVED_AT),
        ReconciliationDisposition::PendingEvidence,
        Vec::new(),
    ))
    .unwrap();
    sqlx::query(
        "UPDATE reconciliation_runs SET result_evaluation_json = ?, result_evaluation_hash = ? \
         WHERE request_id = 'run-typed-pending'",
    )
    .bind(empty_pending.as_str())
    .bind(empty_pending.sha256_hex())
    .execute(&raw)
    .await
    .unwrap();
    assert!(matches!(
        store
            .get_reconciliation_run(&RequestId::from("run-typed-pending"))
            .await,
        Err(StoreError::CorruptData { .. })
    ));

    let (_database, store, raw) = test_store().await;
    store
        .create_reconciliation_run(new_run("run-typed-scope", Some(vec!["cmd-a"])))
        .await
        .unwrap();
    store
        .commit_reconciliation_result(result_bundle(
            "run-typed-scope",
            result("run-typed-scope", OBSERVED_AT),
            ReconciliationDisposition::Completed,
            Vec::new(),
            false,
            false,
        ))
        .await
        .unwrap();
    let forged_completeness = CanonicalJson::from_serializable(&ReconciliationCompleteness {
        symbol_metadata_complete: false,
        command_scope_complete: true,
    })
    .unwrap();
    sqlx::query(
        "UPDATE reconciliation_runs SET completeness_json = ?, completeness_hash = ?, \
            command_scope_complete = 1 WHERE request_id = 'run-typed-scope'",
    )
    .bind(forged_completeness.as_str())
    .bind(forged_completeness.sha256_hex())
    .execute(&raw)
    .await
    .unwrap();
    assert!(matches!(
        store
            .get_reconciliation_run(&RequestId::from("run-typed-scope"))
            .await,
        Err(StoreError::CorruptData { .. })
    ));
}

#[tokio::test]
async fn empty_full_sets_delete_old_rows_preserve_future_rows_and_reject_equal_facts() {
    let (_database, store, raw) = test_store().await;
    store
        .ingest_position_snapshot(
            snapshot_metadata("old-position", "position.snapshot", 1_050),
            &position("old", 1_050, 0.1),
        )
        .await
        .unwrap();
    store
        .ingest_position_snapshot(
            snapshot_metadata("future-position", "position.snapshot", 1_200),
            &position("future", 1_200, 0.2),
        )
        .await
        .unwrap();
    store
        .ingest_order_snapshot(
            snapshot_metadata("old-order", "order.snapshot", 1_050),
            &order("old-order", 1_050),
        )
        .await
        .unwrap();
    store
        .create_reconciliation_run(new_run("run-empty", None))
        .await
        .unwrap();
    let mut observed = result("run-empty", OBSERVED_AT);
    observed.account = Some(account(OBSERVED_AT, 10_000.0));
    store
        .commit_reconciliation_result(result_bundle(
            "run-empty",
            observed,
            ReconciliationDisposition::Completed,
            Vec::new(),
            false,
            true,
        ))
        .await
        .unwrap();

    let state = store
        .load_latest_state(&AuthorizedAccountScope::new([AccountId::from("account-1")]))
        .await
        .unwrap();
    assert_eq!(
        state
            .positions
            .iter()
            .map(|value| value.position_id.as_str())
            .collect::<Vec<_>>(),
        vec!["future"]
    );
    assert!(state.orders.is_empty());
    let checkpoint = store
        .get_account_reconciliation_checkpoint(&AccountId::from("account-1"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(checkpoint.positions_observed_at, OBSERVED_AT);
    assert_eq!(checkpoint.account_refreshed_at, Some(OBSERVED_AT));
    assert_eq!(checkpoint.symbol_metadata_refreshed_at, None);
    assert_eq!(checkpoint.pending_commands_reconciled_at, Some(OBSERVED_AT));

    let ignored = store
        .ingest_position_snapshot(
            snapshot_metadata("late-old-position", "position.snapshot", 1_075),
            &position("resurrect", 1_075, 0.3),
        )
        .await
        .unwrap();
    assert_eq!(
        ignored,
        ProjectionWriteOutcome::FactAppendedProjectionIgnored
    );
    let equal_conflict = store
        .ingest_position_snapshot(
            snapshot_metadata("equal-tombstone", "position.snapshot", OBSERVED_AT),
            &position("resurrect", OBSERVED_AT, 0.3),
        )
        .await
        .expect_err("same-watermark presence conflicts with an authoritative empty full set");
    assert!(matches!(
        equal_conflict,
        StoreError::ObservationConflict { .. }
    ));
    let equal_events: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM core_events WHERE event_id = 'equal-tombstone'")
            .fetch_one(&raw)
            .await
            .unwrap();
    assert_eq!(equal_events, 0);

    store.rebuild_reconciliation_projections().await.unwrap();
    let rebuilt = store
        .load_latest_state(&AuthorizedAccountScope::new([AccountId::from("account-1")]))
        .await
        .unwrap();
    assert_eq!(
        rebuilt
            .positions
            .iter()
            .map(|value| value.position_id.as_str())
            .collect::<Vec<_>>(),
        vec!["future"]
    );
}

#[tokio::test]
async fn same_watermark_single_rows_before_full_set_conflict_and_roll_back_result() {
    let (_database, store, raw) = test_store().await;
    store
        .ingest_position_snapshot(
            snapshot_metadata("position-before-set", "position.snapshot", OBSERVED_AT),
            &position("position-before", OBSERVED_AT, 0.1),
        )
        .await
        .unwrap();
    store
        .ingest_order_snapshot(
            snapshot_metadata("order-before-set", "order.snapshot", OBSERVED_AT),
            &order("order-before", OBSERVED_AT),
        )
        .await
        .unwrap();

    store
        .create_reconciliation_run(new_run("run-missing-position", None))
        .await
        .unwrap();
    let mut missing_position = result("run-missing-position", OBSERVED_AT);
    missing_position
        .orders
        .push(order("order-before", OBSERVED_AT));
    assert!(matches!(
        store
            .commit_reconciliation_result(result_bundle(
                "run-missing-position",
                missing_position,
                ReconciliationDisposition::Completed,
                Vec::new(),
                false,
                true,
            ))
            .await,
        Err(StoreError::ObservationConflict { .. })
    ));

    store
        .create_reconciliation_run(new_run("run-missing-order", None))
        .await
        .unwrap();
    let mut missing_order = result("run-missing-order", OBSERVED_AT);
    missing_order
        .positions
        .push(position("position-before", OBSERVED_AT, 0.1));
    assert!(matches!(
        store
            .commit_reconciliation_result(result_bundle(
                "run-missing-order",
                missing_order,
                ReconciliationDisposition::Completed,
                Vec::new(),
                false,
                true,
            ))
            .await,
        Err(StoreError::ObservationConflict { .. })
    ));

    for request_id in ["run-missing-position", "run-missing-order"] {
        let stored = store
            .get_reconciliation_run(&RequestId::from(request_id))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.status, ReconciliationRunStatus::Requested);
    }
    let result_events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM core_events WHERE event_id IN (\
            'result-event-run-missing-position', 'result-event-run-missing-order'\
         )",
    )
    .fetch_one(&raw)
    .await
    .unwrap();
    assert_eq!(result_events, 0);
}

#[tokio::test]
async fn same_watermark_full_set_before_single_rows_conflicts_and_rolls_back_facts() {
    let (_database, store, raw) = test_store().await;
    store
        .create_reconciliation_run(new_run("run-empty-before-facts", None))
        .await
        .unwrap();
    store
        .commit_reconciliation_result(result_bundle(
            "run-empty-before-facts",
            result("run-empty-before-facts", OBSERVED_AT),
            ReconciliationDisposition::Completed,
            Vec::new(),
            false,
            true,
        ))
        .await
        .unwrap();

    assert!(matches!(
        store
            .ingest_position_snapshot(
                snapshot_metadata("position-after-set", "position.snapshot", OBSERVED_AT),
                &position("position-after", OBSERVED_AT, 0.1),
            )
            .await,
        Err(StoreError::ObservationConflict { .. })
    ));
    assert!(matches!(
        store
            .ingest_order_snapshot(
                snapshot_metadata("order-after-set", "order.snapshot", OBSERVED_AT),
                &order("order-after", OBSERVED_AT),
            )
            .await,
        Err(StoreError::ObservationConflict { .. })
    ));
    let fact_events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM core_events WHERE event_id IN ('position-after-set', 'order-after-set')",
    )
    .fetch_one(&raw)
    .await
    .unwrap();
    assert_eq!(fact_events, 0);
}

#[tokio::test]
async fn rebuild_preflight_rejects_durable_conflicts_regardless_of_replay_order() {
    for (case, received_at) in [("single-first", OBSERVED_AT), ("result-first", 1_300)] {
        let (_database, store, _raw) = test_store().await;
        let request_id = format!("run-rebuild-{case}");
        store
            .create_reconciliation_run(new_run(&request_id, None))
            .await
            .unwrap();
        store
            .commit_reconciliation_result(result_bundle(
                &request_id,
                result(&request_id, OBSERVED_AT),
                ReconciliationDisposition::Completed,
                Vec::new(),
                false,
                true,
            ))
            .await
            .unwrap();
        let checkpoint_before = store
            .get_account_reconciliation_checkpoint(&AccountId::from("account-1"))
            .await
            .unwrap()
            .unwrap();

        let event_id = format!("raw-conflict-{case}");
        let mut metadata = snapshot_metadata(&event_id, "position.snapshot", OBSERVED_AT);
        metadata.received_at = received_at;
        metadata.created_at = received_at;
        store
            .append_core_event(NewCoreEvent {
                metadata,
                payload: CanonicalJson::from_serializable(&position(
                    "raw-conflict",
                    OBSERVED_AT,
                    0.1,
                ))
                .unwrap(),
            })
            .await
            .unwrap();

        assert!(matches!(
            store.rebuild_ingest_projections().await,
            Err(StoreError::ObservationConflict { .. })
        ));
        assert!(matches!(
            store.rebuild_reconciliation_projections().await,
            Err(StoreError::ObservationConflict { .. })
        ));
        let checkpoint_after = store
            .get_account_reconciliation_checkpoint(&AccountId::from("account-1"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(checkpoint_after, checkpoint_before);
    }
}

#[tokio::test]
async fn standalone_ingest_rebuild_restores_full_set_only_members() {
    let (_database, store, _raw) = test_store().await;
    store
        .create_reconciliation_run(new_run("run-full-set-only", None))
        .await
        .unwrap();
    let mut observed = result("run-full-set-only", OBSERVED_AT);
    observed
        .positions
        .push(position("position-only", OBSERVED_AT, 0.1));
    observed.orders.push(order("order-only", OBSERVED_AT));
    store
        .commit_reconciliation_result(result_bundle(
            "run-full-set-only",
            observed,
            ReconciliationDisposition::Completed,
            Vec::new(),
            false,
            true,
        ))
        .await
        .unwrap();

    let report = store.rebuild_ingest_projections().await.unwrap();
    assert_eq!(report.replayed_facts, 0);
    let state = store
        .load_latest_state(&AuthorizedAccountScope::new([AccountId::from("account-1")]))
        .await
        .unwrap();
    assert_eq!(state.positions.len(), 1);
    assert_eq!(state.positions[0].position_id.as_str(), "position-only");
    assert_eq!(state.orders.len(), 1);
    assert_eq!(state.orders[0].broker_order_id.as_str(), "order-only");
    store
        .get_account_reconciliation_checkpoint(&AccountId::from("account-1"))
        .await
        .unwrap()
        .expect("combined rebuild must restore the reconciliation checkpoint");
}

#[tokio::test]
async fn standalone_ingest_rebuild_self_heals_partial_membership_loss() {
    let (_database, store, raw) = test_store().await;
    store
        .create_reconciliation_run(new_run("run-partial-membership", None))
        .await
        .unwrap();
    let mut observed = result("run-partial-membership", OBSERVED_AT);
    observed
        .positions
        .push(position("position-member", OBSERVED_AT, 0.1));
    observed.orders.push(order("order-member", OBSERVED_AT));
    store
        .commit_reconciliation_result(result_bundle(
            "run-partial-membership",
            observed,
            ReconciliationDisposition::Completed,
            Vec::new(),
            false,
            true,
        ))
        .await
        .unwrap();
    store
        .ingest_position_snapshot(
            snapshot_metadata("matching-position-fact", "position.snapshot", OBSERVED_AT),
            &position("position-member", OBSERVED_AT, 0.1),
        )
        .await
        .unwrap();
    store
        .ingest_order_snapshot(
            snapshot_metadata("matching-order-fact", "order.snapshot", OBSERVED_AT),
            &order("order-member", OBSERVED_AT),
        )
        .await
        .unwrap();

    sqlx::query("DELETE FROM reconciliation_position_set_members")
        .execute(&raw)
        .await
        .unwrap();
    sqlx::query("DELETE FROM reconciliation_order_set_members")
        .execute(&raw)
        .await
        .unwrap();

    let report = store.rebuild_ingest_projections().await.unwrap();
    assert_eq!(report.replayed_facts, 2);
    let position_members: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM reconciliation_position_set_members")
            .fetch_one(&raw)
            .await
            .unwrap();
    let order_members: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM reconciliation_order_set_members")
            .fetch_one(&raw)
            .await
            .unwrap();
    assert_eq!(position_members, 1);
    assert_eq!(order_members, 1);
    store
        .get_account_reconciliation_checkpoint(&AccountId::from("account-1"))
        .await
        .unwrap()
        .expect("combined rebuild must restore a typed-valid checkpoint");
}

#[tokio::test]
async fn equal_watermark_different_full_set_conflicts_and_rolls_back_result_event() {
    let (_database, store, raw) = test_store().await;
    store
        .create_reconciliation_run(new_run("run-set-a", None))
        .await
        .unwrap();
    let mut first = result("run-set-a", OBSERVED_AT);
    first
        .positions
        .push(position("position-a", OBSERVED_AT, 0.1));
    store
        .commit_reconciliation_result(result_bundle(
            "run-set-a",
            first,
            ReconciliationDisposition::Completed,
            Vec::new(),
            false,
            true,
        ))
        .await
        .unwrap();
    let member_conflict = store
        .ingest_position_snapshot(
            snapshot_metadata("equal-member-conflict", "position.snapshot", OBSERVED_AT),
            &position("position-a", OBSERVED_AT, 9.0),
        )
        .await
        .expect_err("same member at the same watermark must retain hash conflict semantics");
    assert!(matches!(
        member_conflict,
        StoreError::ObservationConflict { .. }
    ));

    store
        .create_reconciliation_run(new_run("run-set-b", None))
        .await
        .unwrap();
    let error = store
        .commit_reconciliation_result(result_bundle(
            "run-set-b",
            result("run-set-b", OBSERVED_AT),
            ReconciliationDisposition::Completed,
            Vec::new(),
            false,
            true,
        ))
        .await
        .expect_err("same watermark with a different set must conflict");
    assert!(matches!(error, StoreError::ObservationConflict { .. }));
    let stored = store
        .get_reconciliation_run(&RequestId::from("run-set-b"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.status, ReconciliationRunStatus::Requested);
    let result_event: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM core_events WHERE event_id = 'result-event-run-set-b'",
    )
    .fetch_one(&raw)
    .await
    .unwrap();
    assert_eq!(result_event, 0);
}

#[tokio::test]
async fn equal_watermark_source_tie_break_is_stable_across_online_commit_and_rebuild() {
    let (_database, store, raw) = test_store().await;
    for request_id in ["run-z", "run-a"] {
        store
            .create_reconciliation_run(new_run(request_id, None))
            .await
            .unwrap();
        let mut bundle = result_bundle(
            request_id,
            result(request_id, OBSERVED_AT),
            ReconciliationDisposition::Completed,
            Vec::new(),
            false,
            true,
        );
        if request_id == "run-z" {
            bundle.event_metadata.received_at = 1_200;
            bundle.event_metadata.created_at = 1_201;
        }
        store.commit_reconciliation_result(bundle).await.unwrap();
    }
    let online = store
        .get_account_reconciliation_checkpoint(&AccountId::from("account-1"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(online.source_request_id.as_str(), "run-a");

    for table in [
        "reconciliation_position_set_members",
        "reconciliation_order_set_members",
        "account_reconciliation_checkpoints",
        "position_snapshots_latest",
        "order_snapshots_latest",
    ] {
        sqlx::query(&format!("DELETE FROM {table}"))
            .execute(&raw)
            .await
            .unwrap();
    }
    store.rebuild_reconciliation_projections().await.unwrap();
    let rebuilt = store
        .get_account_reconciliation_checkpoint(&AccountId::from("account-1"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rebuilt.source_request_id.as_str(), "run-a");
    assert_eq!(rebuilt, online);
}

#[tokio::test]
async fn scoped_or_incomplete_completed_results_do_not_advance_readiness() {
    let (_database, store, raw) = test_store().await;
    store
        .create_reconciliation_run(new_run("run-scoped", Some(vec!["cmd-a"])))
        .await
        .unwrap();
    let mut observed = result("run-scoped", OBSERVED_AT);
    observed.symbol_metadata.push(symbol(OBSERVED_AT));
    assert!(matches!(
        store
            .commit_reconciliation_result(result_bundle(
                "run-scoped",
                observed.clone(),
                ReconciliationDisposition::Completed,
                Vec::new(),
                false,
                true,
            ))
            .await,
        Err(StoreError::InvalidRecord { .. })
    ));
    store
        .commit_reconciliation_result(result_bundle(
            "run-scoped",
            observed,
            ReconciliationDisposition::Completed,
            Vec::new(),
            false,
            false,
        ))
        .await
        .unwrap();
    let checkpoint = store
        .get_account_reconciliation_checkpoint(&AccountId::from("account-1"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(checkpoint.pending_commands_reconciled_at, None);
    assert_eq!(checkpoint.symbol_metadata_refreshed_at, None);
    sqlx::query(
        "UPDATE account_reconciliation_checkpoints \
         SET pending_commands_reconciled_at = ? WHERE account_id = 'account-1'",
    )
    .bind(OBSERVED_AT)
    .execute(&raw)
    .await
    .unwrap();
    assert!(matches!(
        store
            .get_account_reconciliation_checkpoint(&AccountId::from("account-1"))
            .await,
        Err(StoreError::CorruptData { .. })
    ));
    sqlx::query(
        "UPDATE account_reconciliation_checkpoints \
         SET pending_commands_reconciled_at = NULL WHERE account_id = 'account-1'",
    )
    .execute(&raw)
    .await
    .unwrap();

    store
        .create_reconciliation_run(new_run("run-incomplete-scope", None))
        .await
        .unwrap();
    store
        .commit_reconciliation_result(result_bundle(
            "run-incomplete-scope",
            result("run-incomplete-scope", 1_200),
            ReconciliationDisposition::Completed,
            Vec::new(),
            false,
            false,
        ))
        .await
        .unwrap();
    let checkpoint = store
        .get_account_reconciliation_checkpoint(&AccountId::from("account-1"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(checkpoint.result_observed_at, 1_200);
    assert_eq!(checkpoint.pending_commands_reconciled_at, None);

    let mut routed_run = new_run("run-route", None);
    routed_run.request.terminal_id = Some(TerminalId::from("terminal-1"));
    routed_run.request.client_id = Some(ClientId::from("client-1"));
    routed_run.event_metadata.terminal_id = routed_run.request.terminal_id.clone();
    routed_run.event_metadata.client_id = routed_run.request.client_id.clone();
    store.create_reconciliation_run(routed_run).await.unwrap();
    let mut routed = result_bundle(
        "run-route",
        result("run-route", 1_300),
        ReconciliationDisposition::Completed,
        Vec::new(),
        false,
        true,
    );
    routed.result.terminal_id = Some(TerminalId::from("terminal-1"));
    routed.result.client_id = Some(ClientId::from("client-1"));
    routed.event_metadata.terminal_id = routed.result.terminal_id.clone();
    routed.event_metadata.client_id = routed.result.client_id.clone();
    store.commit_reconciliation_result(routed).await.unwrap();
    let checkpoint = store
        .get_account_reconciliation_checkpoint(&AccountId::from("account-1"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(checkpoint.result_observed_at, 1_300);
    assert_eq!(checkpoint.pending_commands_reconciled_at, None);
}

#[tokio::test]
async fn result_replay_is_duplicate_drift_conflicts_and_projection_failure_rolls_back() {
    let (_database, store, raw) = test_store().await;
    store
        .create_reconciliation_run(new_run("run-replay", None))
        .await
        .unwrap();
    let bundle = result_bundle(
        "run-replay",
        result("run-replay", OBSERVED_AT),
        ReconciliationDisposition::Completed,
        Vec::new(),
        false,
        true,
    );
    assert!(matches!(
        store
            .commit_reconciliation_result(bundle.clone())
            .await
            .unwrap(),
        WriteOutcome::Inserted(_)
    ));
    assert!(matches!(
        store
            .commit_reconciliation_result(bundle.clone())
            .await
            .unwrap(),
        WriteOutcome::Duplicate(_)
    ));
    let mut drift = bundle;
    drift.completeness.symbol_metadata_complete = true;
    assert!(matches!(
        store.commit_reconciliation_result(drift).await,
        Err(StoreError::IdentityConflict { .. })
    ));

    store
        .create_reconciliation_run(new_run("run-rollback", None))
        .await
        .unwrap();
    store
        .ingest_account_snapshot(
            snapshot_metadata("account-conflict", "account.snapshot", 1_300),
            &account(1_300, 10_000.0),
        )
        .await
        .unwrap();
    let mut conflicting = result("run-rollback", 1_300);
    conflicting.account = Some(account(1_300, 20_000.0));
    assert!(matches!(
        store
            .commit_reconciliation_result(result_bundle(
                "run-rollback",
                conflicting,
                ReconciliationDisposition::Completed,
                Vec::new(),
                false,
                true,
            ))
            .await,
        Err(StoreError::ObservationConflict { .. })
    ));
    let event_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM core_events WHERE event_id = 'result-event-run-rollback'",
    )
    .fetch_one(&raw)
    .await
    .unwrap();
    assert_eq!(event_count, 0);
}

#[tokio::test]
async fn exact_result_replay_self_heals_missing_latest_membership_and_checkpoint() {
    let (_database, store, raw) = test_store().await;
    store
        .create_reconciliation_run(new_run("run-self-heal", None))
        .await
        .unwrap();
    let mut observed = result("run-self-heal", OBSERVED_AT);
    observed
        .positions
        .push(position("position-a", OBSERVED_AT, 0.1));
    let bundle = result_bundle(
        "run-self-heal",
        observed,
        ReconciliationDisposition::Completed,
        Vec::new(),
        false,
        true,
    );
    store
        .commit_reconciliation_result(bundle.clone())
        .await
        .unwrap();
    store
        .ingest_position_snapshot(
            snapshot_metadata("self-heal-future", "position.snapshot", 1_200),
            &position("position-future", 1_200, 0.2),
        )
        .await
        .unwrap();
    let mut unrelated = snapshot_metadata("unrelated-malformed", "position.snapshot", 1_150);
    unrelated.account_id = Some(AccountId::from("account-2"));
    unrelated.aggregate_id = "account-2".to_owned();
    store
        .append_core_event(NewCoreEvent {
            metadata: unrelated,
            payload: CanonicalJson::from_value(json!({})).unwrap(),
        })
        .await
        .unwrap();
    for table in [
        "reconciliation_position_set_members",
        "reconciliation_order_set_members",
        "account_reconciliation_checkpoints",
        "position_snapshots_latest",
    ] {
        sqlx::query(&format!("DELETE FROM {table}"))
            .execute(&raw)
            .await
            .unwrap();
    }

    assert!(matches!(
        store.commit_reconciliation_result(bundle).await.unwrap(),
        WriteOutcome::Duplicate(_)
    ));
    let state = store
        .load_latest_state(&AuthorizedAccountScope::new([AccountId::from("account-1")]))
        .await
        .unwrap();
    assert_eq!(state.positions.len(), 2);
    assert_eq!(state.positions[0].position_id.as_str(), "position-a");
    assert_eq!(state.positions[1].position_id.as_str(), "position-future");
    store
        .get_account_reconciliation_checkpoint(&AccountId::from("account-1"))
        .await
        .unwrap()
        .expect("checkpoint should self-heal");
}

#[tokio::test]
async fn manual_required_needs_explicit_canonical_evidence_and_never_regresses_time() {
    let (_database, store, raw) = test_store().await;
    store
        .create_reconciliation_run(new_run("run-manual", Some(vec!["cmd-a"])))
        .await
        .unwrap();
    let direct = result_bundle(
        "run-manual",
        result("run-manual", OBSERVED_AT),
        ReconciliationDisposition::ManualRequired,
        vec!["cmd-a"],
        false,
        false,
    );
    assert!(matches!(
        store.commit_reconciliation_result(direct).await,
        Err(StoreError::InvalidRecord { .. })
    ));
    let result_events: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM core_events WHERE event_id = 'result-event-run-manual'",
    )
    .fetch_one(&raw)
    .await
    .unwrap();
    assert_eq!(result_events, 0);

    let escalation = NewManualReconciliationEscalation {
        evidence: ManualReconciliationEvidence {
            request_id: RequestId::from("run-manual"),
            escalated_at: 1_200,
            reason: "operator timeout evidence".to_owned(),
        },
        evaluation: ReconciliationEvaluation {
            findings: vec![json!({"kind":"RECONCILIATION_RESULT_MISSING"})],
            ..evaluation(
                "run-manual",
                None,
                ReconciliationDisposition::ManualRequired,
                vec!["cmd-a"],
            )
        },
        updated_at: 1_200,
    };
    assert!(matches!(
        store
            .escalate_reconciliation_manual(escalation.clone())
            .await
            .unwrap(),
        WriteOutcome::Inserted(_)
    ));
    assert!(matches!(
        store
            .escalate_reconciliation_manual(escalation.clone())
            .await
            .unwrap(),
        WriteOutcome::Duplicate(_)
    ));
    let mut timestamp_drift = escalation.clone();
    timestamp_drift.updated_at += 1;
    assert!(matches!(
        store.escalate_reconciliation_manual(timestamp_drift).await,
        Err(StoreError::IdentityConflict { .. })
    ));
    let mut drift = escalation;
    drift.evidence.reason = "different evidence".to_owned();
    assert!(matches!(
        store.escalate_reconciliation_manual(drift).await,
        Err(StoreError::IdentityConflict { .. })
    ));
    let stored = store
        .get_reconciliation_run(&RequestId::from("run-manual"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stored.status,
        ReconciliationRunStatus::ManualReconciliationRequired
    );

    store
        .create_reconciliation_run(new_run("run-manual-forged", Some(vec!["cmd-a"])))
        .await
        .unwrap();
    store
        .commit_reconciliation_result(result_bundle(
            "run-manual-forged",
            result("run-manual-forged", OBSERVED_AT),
            ReconciliationDisposition::PendingEvidence,
            vec!["cmd-a"],
            false,
            false,
        ))
        .await
        .unwrap();
    let forged = NewManualReconciliationEscalation {
        evidence: ManualReconciliationEvidence {
            request_id: RequestId::from("run-manual-forged"),
            escalated_at: 1_200,
            reason: "operator evidence".to_owned(),
        },
        evaluation: ReconciliationEvaluation {
            findings: vec![json!({"kind":"FORGED_FINDING"})],
            ..evaluation(
                "run-manual-forged",
                Some(OBSERVED_AT),
                ReconciliationDisposition::ManualRequired,
                vec!["cmd-a"],
            )
        },
        updated_at: 1_200,
    };
    assert!(matches!(
        store.escalate_reconciliation_manual(forged).await,
        Err(StoreError::InvalidRecord { .. })
    ));
    assert_eq!(
        store
            .get_reconciliation_run(&RequestId::from("run-manual-forged"))
            .await
            .unwrap()
            .unwrap()
            .status,
        ReconciliationRunStatus::PendingEvidence
    );
}

#[tokio::test]
async fn checkpoint_typed_read_detects_source_hash_and_membership_tampering() {
    let (_database, store, raw) = test_store().await;
    store
        .create_reconciliation_run(new_run("run-tamper", None))
        .await
        .unwrap();
    let mut observed = result("run-tamper", OBSERVED_AT);
    observed
        .positions
        .push(position("position-a", OBSERVED_AT, 0.1));
    store
        .commit_reconciliation_result(result_bundle(
            "run-tamper",
            observed,
            ReconciliationDisposition::Completed,
            Vec::new(),
            false,
            true,
        ))
        .await
        .unwrap();
    let tampered_position = position("position-a", OBSERVED_AT, 0.9);
    let tampered_payload = CanonicalJson::from_serializable(&tampered_position).unwrap();
    let tampered_set = CanonicalJson::from_serializable(&vec![tampered_position.clone()]).unwrap();
    sqlx::query(
        "UPDATE reconciliation_position_set_members \
         SET payload_json = ?, payload_hash = ? \
         WHERE account_id = 'account-1' AND position_id = 'position-a'",
    )
    .bind(tampered_payload.as_str())
    .bind(tampered_payload.sha256_hex())
    .execute(&raw)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE position_snapshots_latest SET payload_json = ?, payload_hash = ? \
         WHERE account_id = 'account-1' AND position_id = 'position-a'",
    )
    .bind(tampered_payload.as_str())
    .bind(tampered_payload.sha256_hex())
    .execute(&raw)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE account_reconciliation_checkpoints SET positions_set_hash = ? \
         WHERE account_id = 'account-1'",
    )
    .bind(tampered_set.sha256_hex())
    .execute(&raw)
    .await
    .unwrap();
    assert!(matches!(
        store
            .get_account_reconciliation_checkpoint(&AccountId::from("account-1"))
            .await,
        Err(StoreError::CorruptData { .. })
    ));
}

#[tokio::test]
async fn checkpoint_full_set_hashes_must_match_the_source_run_itself() {
    let (_database, store, raw) = test_store().await;
    store
        .create_reconciliation_run(new_run("run-source-conflict", None))
        .await
        .unwrap();
    store
        .create_reconciliation_run(new_run("run-matching-evidence", None))
        .await
        .unwrap();
    let mut matching = result("run-matching-evidence", OBSERVED_AT);
    matching
        .positions
        .push(position("position-a", OBSERVED_AT, 0.1));
    store
        .commit_reconciliation_result(result_bundle(
            "run-matching-evidence",
            matching,
            ReconciliationDisposition::Completed,
            Vec::new(),
            false,
            true,
        ))
        .await
        .unwrap();

    let mut conflicting = result("run-source-conflict", OBSERVED_AT);
    conflicting
        .positions
        .push(position("position-b", OBSERVED_AT, 0.9));
    let result_payload = CanonicalJson::from_serializable(&conflicting).unwrap();
    let result_evaluation = CanonicalJson::from_serializable(&evaluation(
        "run-source-conflict",
        Some(OBSERVED_AT),
        ReconciliationDisposition::Completed,
        Vec::new(),
    ))
    .unwrap();
    let completeness = ReconciliationCompleteness {
        symbol_metadata_complete: false,
        command_scope_complete: true,
    };
    let completeness_payload = CanonicalJson::from_serializable(&completeness).unwrap();
    let result_event_id = "raw-result-event-run-source-conflict";
    let result_metadata = event_metadata(
        result_event_id,
        "reconciliation.result",
        "run-source-conflict",
        "account-1",
        OBSERVED_AT,
    );
    store
        .append_core_event(NewCoreEvent {
            metadata: result_metadata.clone(),
            payload: result_payload.clone(),
        })
        .await
        .unwrap();
    sqlx::query(
        "UPDATE reconciliation_runs SET status = 'COMPLETED', result_event_id = ?, \
            result_observed_at = ?, result_payload_json = ?, result_payload_hash = ?, \
            result_evaluation_json = ?, result_evaluation_hash = ?, completeness_json = ?, \
            completeness_hash = ?, symbol_metadata_complete = 0, command_scope_complete = 1, \
            updated_at = ? WHERE request_id = 'run-source-conflict'",
    )
    .bind(result_event_id)
    .bind(OBSERVED_AT)
    .bind(result_payload.as_str())
    .bind(result_payload.sha256_hex())
    .bind(result_evaluation.as_str())
    .bind(result_evaluation.sha256_hex())
    .bind(completeness_payload.as_str())
    .bind(completeness_payload.sha256_hex())
    .bind(result_metadata.created_at)
    .execute(&raw)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE account_reconciliation_checkpoints SET source_request_id = 'run-source-conflict' \
         WHERE account_id = 'account-1'",
    )
    .execute(&raw)
    .await
    .unwrap();

    assert!(matches!(
        store
            .get_account_reconciliation_checkpoint(&AccountId::from("account-1"))
            .await,
        Err(StoreError::CorruptData { .. })
    ));
}

#[tokio::test]
async fn rebuild_restores_full_sets_checkpoint_leaves_market_and_rolls_back_on_tamper() {
    let (_database, store, raw) = test_store().await;
    store
        .create_reconciliation_run(new_run("run-rebuild", None))
        .await
        .unwrap();
    let mut observed = result("run-rebuild", OBSERVED_AT);
    observed
        .positions
        .push(position("position-a", OBSERVED_AT, 0.1));
    observed.orders.push(order("order-a", OBSERVED_AT));
    store
        .commit_reconciliation_result(result_bundle(
            "run-rebuild",
            observed,
            ReconciliationDisposition::Completed,
            Vec::new(),
            false,
            true,
        ))
        .await
        .unwrap();

    let mut late_old_metadata = snapshot_metadata("late-old", "position.snapshot", 1_050);
    late_old_metadata.received_at = 1_200;
    late_old_metadata.created_at = 1_201;
    assert_eq!(
        store
            .ingest_position_snapshot(late_old_metadata, &position("tombstoned", 1_050, 0.2),)
            .await
            .unwrap(),
        ProjectionWriteOutcome::FactAppendedProjectionIgnored
    );
    let market = sinan_types::MarketSnapshot {
        symbol: SymbolCode::from("EURUSD"),
        broker_symbol: Some("EURUSD.a".to_owned()),
        bid: 1.1,
        ask: 1.2,
        spread: 0.1,
        observed_at: 1_300,
    };
    store
        .update_market_snapshot(&AccountId::from("account-1"), &market, 1_301)
        .await
        .unwrap();

    for table in [
        "reconciliation_position_set_members",
        "reconciliation_order_set_members",
        "account_reconciliation_checkpoints",
        "position_snapshots_latest",
        "order_snapshots_latest",
    ] {
        sqlx::query(&format!("DELETE FROM {table}"))
            .execute(&raw)
            .await
            .unwrap();
    }
    let report = store.rebuild_reconciliation_projections().await.unwrap();
    assert_eq!(report.replayed_reconciliation_results, 1);
    let state = store
        .load_latest_state(&AuthorizedAccountScope::new([AccountId::from("account-1")]))
        .await
        .unwrap();
    assert_eq!(state.positions.len(), 1);
    assert_eq!(state.positions[0].position_id.as_str(), "position-a");
    assert_eq!(state.orders.len(), 1);
    assert_eq!(state.markets.len(), 1);
    store
        .get_account_reconciliation_checkpoint(&AccountId::from("account-1"))
        .await
        .unwrap()
        .expect("checkpoint rebuilt");

    sqlx::query(
        "UPDATE reconciliation_runs SET result_payload_hash = ? WHERE request_id = 'run-rebuild'",
    )
    .bind("0".repeat(64))
    .execute(&raw)
    .await
    .unwrap();
    let before: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM position_snapshots_latest WHERE account_id = 'account-1'",
    )
    .fetch_one(&raw)
    .await
    .unwrap();
    assert!(matches!(
        store.rebuild_reconciliation_projections().await,
        Err(StoreError::CorruptData { .. })
    ));
    let after: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM position_snapshots_latest WHERE account_id = 'account-1'",
    )
    .fetch_one(&raw)
    .await
    .unwrap();
    assert_eq!(before, after);
}
