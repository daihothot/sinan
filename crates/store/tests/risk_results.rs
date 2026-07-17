mod common;

use common::test_store;
use sinan_store::{
    CanonicalJson, NewRiskResult, NewTradeIntent, SqliteStateStore, StoreError, WriteOutcome,
};
use sinan_types::{
    single_leg_id, AccountId, AdjustedRiskLeg, AdjustedRiskLegAction, CorrelationId, DecisionId,
    ErrorCode, ErrorCodeOrString, IdempotencyKey, IntentId, LegId, RiskId, RiskResult,
    SizingCandidateProvenance, StrategyId, SymbolCode, TimeframeCode, TradeIntent,
    TradeIntentAction, TradeIntentLeg, TradeIntentLegAction, TradeIntentStatus,
};

fn trade_intent() -> TradeIntent {
    TradeIntent {
        intent_id: IntentId::from("intent_1"),
        decision_id: DecisionId::from("decision_1"),
        strategy_id: StrategyId::from("strategy_1"),
        correlation_id: CorrelationId::from("correlation_1"),
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

fn new_trade_intent() -> NewTradeIntent {
    NewTradeIntent {
        intent: trade_intent(),
        initial_status: TradeIntentStatus::Accepted,
        recorded_at: 1_010,
    }
}

fn risk_result(risk_id: impl Into<RiskId>) -> RiskResult {
    RiskResult {
        risk_id: risk_id.into(),
        request_id: "risk_request_1".into(),
        intent_id: IntentId::from("intent_1"),
        account_id: AccountId::from("account_1"),
        risk_request_hash: "a".repeat(64),
        approved: true,
        reason: ErrorCodeOrString::from("OK"),
        message: None,
        sizing_version: Some("fixed-risk-at-stop.v1".to_owned()),
        risk_base_amount: Some(10_000.0),
        risk_budget_amount: Some(100.0),
        adjusted_risk_pct: Some(0.98),
        sizing_candidates: Some(vec![SizingCandidateProvenance {
            leg_id: single_leg_id(&IntentId::from("intent_1")),
            symbol: SymbolCode::from("XAUUSD"),
            action: AdjustedRiskLegAction::Buy,
            ratio: 1.0,
            worst_entry_price: 2_350.0,
            stop_loss_price: 2_320.5,
            estimated_cost_per_lot: 0.0,
        }]),
        adjusted_legs: Some(vec![AdjustedRiskLeg {
            leg_id: single_leg_id(&IntentId::from("intent_1")),
            symbol: SymbolCode::from("XAUUSD"),
            action: AdjustedRiskLegAction::Buy,
            lots: 0.07,
            risk_amount: 98.0,
            risk_pct: 0.98,
            sizing_entry_price: 2_350.0,
            approved_sl: 2_320.5,
            loss_per_lot: 1_400.0,
            reason: Some(ErrorCodeOrString::from("OK")),
        }]),
        decision_id: DecisionId::from("decision_1"),
        snapshot_age_ms: 125,
        market_snapshot_age_ms: 75,
        symbol_metadata_age_ms: 250,
        capacity_age_ms: 100,
        evaluated_at: 1_100,
        valid_until: 5_000,
    }
}

fn new_risk_result(risk_id: impl Into<RiskId>) -> NewRiskResult {
    NewRiskResult {
        result: risk_result(risk_id),
    }
}

fn multi_leg_trade_intent() -> NewTradeIntent {
    let mut intent = new_trade_intent();
    intent.intent.symbol = SymbolCode::from("PAIR");
    intent.intent.proposed_sl = None;
    intent.intent.proposed_tp = None;
    intent.intent.proposed_legs = Some(vec![
        TradeIntentLeg {
            leg_id: LegId::from("leg_1"),
            symbol: SymbolCode::from("XAUUSD"),
            action: TradeIntentLegAction::Buy,
            ratio: 1.0,
            proposed_sl: Some(2_320.5),
            proposed_tp: None,
        },
        TradeIntentLeg {
            leg_id: LegId::from("leg_2"),
            symbol: SymbolCode::from("EURUSD"),
            action: TradeIntentLegAction::Sell,
            ratio: 2.0,
            proposed_sl: Some(1.2),
            proposed_tp: None,
        },
    ]);
    intent
}

fn multi_leg_risk_result(risk_id: impl Into<RiskId>) -> NewRiskResult {
    let mut result = new_risk_result(risk_id);
    result.result.sizing_candidates.as_mut().unwrap()[0].leg_id = LegId::from("leg_1");
    result.result.adjusted_legs.as_mut().unwrap()[0].leg_id = LegId::from("leg_1");
    result
        .result
        .sizing_candidates
        .as_mut()
        .unwrap()
        .push(SizingCandidateProvenance {
            leg_id: LegId::from("leg_2"),
            symbol: SymbolCode::from("EURUSD"),
            action: AdjustedRiskLegAction::Sell,
            ratio: 2.0,
            worst_entry_price: 1.1,
            stop_loss_price: 1.2,
            estimated_cost_per_lot: 0.0,
        });
    let first_leg = &mut result.result.adjusted_legs.as_mut().unwrap()[0];
    first_leg.lots = 0.035;
    first_leg.risk_amount = 49.0;
    first_leg.risk_pct = 0.49;
    result
        .result
        .adjusted_legs
        .as_mut()
        .unwrap()
        .push(AdjustedRiskLeg {
            leg_id: LegId::from("leg_2"),
            symbol: SymbolCode::from("EURUSD"),
            action: AdjustedRiskLegAction::Sell,
            lots: 0.49,
            risk_amount: 49.0,
            risk_pct: 0.49,
            sizing_entry_price: 1.1,
            approved_sl: 1.2,
            loss_per_lot: 100.0,
            reason: Some(ErrorCodeOrString::from("OK")),
        });
    result
}

fn clear_sizing(result: &mut RiskResult) {
    result.sizing_version = None;
    result.risk_base_amount = None;
    result.risk_budget_amount = None;
    result.adjusted_risk_pct = None;
    result.sizing_candidates = None;
    result.adjusted_legs = None;
}

fn reject(result: &mut RiskResult, reason: ErrorCode) {
    clear_sizing(result);
    result.approved = false;
    result.reason = reason.into();
}

async fn seed_intent_with_action(store: &SqliteStateStore, action: TradeIntentAction) {
    let mut intent = new_trade_intent();
    intent.intent.action = action;
    store
        .insert_trade_intent(intent)
        .await
        .expect("trade intent should insert");
}

async fn seed_intent(store: &SqliteStateStore) {
    store
        .insert_trade_intent(new_trade_intent())
        .await
        .expect("trade intent should insert");
}

async fn allow_risk_corruption(pool: &sqlx::SqlitePool) {
    sqlx::query("DROP TRIGGER trg_risk_results_no_update")
        .execute(pool)
        .await
        .expect("test should disable the immutable-update guard before injecting corruption");
}

#[tokio::test]
async fn risk_result_repository_round_trips_canonical_idempotent_facts() {
    let (_database, store, pool) = test_store().await;
    seed_intent(&store).await;
    let result = new_risk_result("risk_1");
    let expected_payload = CanonicalJson::from_serializable(&result.result).unwrap();

    let inserted = store.insert_risk_result(result.clone()).await.unwrap();
    assert!(matches!(inserted, WriteOutcome::Inserted(_)));
    assert_eq!(inserted.record().result.intent_id, result.result.intent_id);
    assert_eq!(inserted.record().result, result.result);
    assert_eq!(inserted.record().payload, expected_payload);
    assert_eq!(
        store.get_risk_result(&result.result.risk_id).await.unwrap(),
        Some(inserted.record().clone())
    );

    let stored: (String, String) =
        sqlx::query_as("SELECT payload_json, payload_hash FROM risk_results WHERE risk_id = ?")
            .bind(result.result.risk_id.as_str())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(stored.0, expected_payload.as_str());
    assert_eq!(stored.1, expected_payload.sha256_hex());

    assert!(matches!(
        store.insert_risk_result(result.clone()).await.unwrap(),
        WriteOutcome::Duplicate(_)
    ));

    let mut changed_payload = result.clone();
    changed_payload.result.message = Some("different but valid payload".to_owned());
    assert!(matches!(
        store.insert_risk_result(changed_payload).await,
        Err(StoreError::IdentityConflict { .. })
    ));

    assert!(matches!(
        store
            .insert_risk_result(new_risk_result("risk_2"))
            .await
            .unwrap(),
        WriteOutcome::Inserted(_)
    ));
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM risk_results")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 2, "one intent may be evaluated more than once");
}

#[tokio::test]
async fn risk_result_repository_validates_parent_and_replay_identity() {
    let (_database, store, _) = test_store().await;
    let valid = new_risk_result("risk_1");
    assert!(matches!(
        store.insert_risk_result(valid.clone()).await,
        Err(StoreError::NotFound { .. })
    ));

    seed_intent(&store).await;
    let mut wrong_account = new_risk_result("risk_wrong_account");
    wrong_account.result.account_id = AccountId::from("account_2");
    assert!(matches!(
        store.insert_risk_result(wrong_account).await,
        Err(StoreError::IdentityConflict { .. })
    ));

    let mut wrong_decision = new_risk_result("risk_wrong_decision");
    wrong_decision.result.decision_id = DecisionId::from("decision_2");
    assert!(matches!(
        store.insert_risk_result(wrong_decision).await,
        Err(StoreError::IdentityConflict { .. })
    ));

    store.insert_risk_result(valid.clone()).await.unwrap();
    let mut replay_with_another_parent = valid;
    replay_with_another_parent.result.intent_id = IntentId::from("missing_intent");
    assert!(matches!(
        store.insert_risk_result(replay_with_another_parent).await,
        Err(StoreError::IdentityConflict { .. })
    ));
}

#[tokio::test]
async fn risk_result_repository_enforces_parent_action_contract() {
    for action in [TradeIntentAction::Buy, TradeIntentAction::Sell] {
        let (_database, store, _) = test_store().await;
        seed_intent_with_action(&store, action).await;
        let mut approved_noop = new_risk_result(format!("risk_{action}_noop"));
        clear_sizing(&mut approved_noop.result);

        assert!(matches!(
            store.insert_risk_result(approved_noop).await,
            Err(StoreError::InvalidRecord { .. })
        ));
    }

    let (_database, store, _) = test_store().await;
    seed_intent_with_action(&store, TradeIntentAction::Hold).await;
    assert!(matches!(
        store
            .insert_risk_result(new_risk_result("risk_hold_with_sizing"))
            .await,
        Err(StoreError::InvalidRecord { .. })
    ));
    let mut hold_noop = new_risk_result("risk_hold_noop");
    clear_sizing(&mut hold_noop.result);
    assert!(matches!(
        store.insert_risk_result(hold_noop).await.unwrap(),
        WriteOutcome::Inserted(_)
    ));

    let (_database, store, _) = test_store().await;
    seed_intent_with_action(&store, TradeIntentAction::Close).await;
    assert!(matches!(
        store
            .insert_risk_result(new_risk_result("risk_close_with_sizing"))
            .await,
        Err(StoreError::InvalidRecord { .. })
    ));
    let mut approved_close_noop = new_risk_result("risk_close_noop");
    clear_sizing(&mut approved_close_noop.result);
    assert!(matches!(
        store.insert_risk_result(approved_close_noop).await,
        Err(StoreError::InvalidRecord { .. })
    ));
    let mut rejected_close = new_risk_result("risk_close_rejected");
    reject(
        &mut rejected_close.result,
        ErrorCode::RiskReductionNotProvable,
    );
    assert!(matches!(
        store.insert_risk_result(rejected_close).await.unwrap(),
        WriteOutcome::Inserted(_)
    ));
}

#[tokio::test]
async fn risk_result_repository_binds_single_leg_sizing_to_parent_intent() {
    let (_database, store, _) = test_store().await;
    seed_intent(&store).await;

    let mut reverse_action = new_risk_result("risk_reverse_action");
    let candidate = &mut reverse_action.result.sizing_candidates.as_mut().unwrap()[0];
    candidate.action = AdjustedRiskLegAction::Sell;
    candidate.stop_loss_price = 2_360.0;
    let leg = &mut reverse_action.result.adjusted_legs.as_mut().unwrap()[0];
    leg.action = AdjustedRiskLegAction::Sell;
    leg.approved_sl = 2_360.0;
    assert!(matches!(
        store.insert_risk_result(reverse_action).await,
        Err(StoreError::InvalidRecord { .. })
    ));

    let mut wrong_symbol = new_risk_result("risk_wrong_symbol");
    wrong_symbol.result.sizing_candidates.as_mut().unwrap()[0].symbol = SymbolCode::from("EURUSD");
    wrong_symbol.result.adjusted_legs.as_mut().unwrap()[0].symbol = SymbolCode::from("EURUSD");
    assert!(matches!(
        store.insert_risk_result(wrong_symbol).await,
        Err(StoreError::InvalidRecord { .. })
    ));

    let mut wrong_ratio = new_risk_result("risk_wrong_ratio");
    wrong_ratio.result.sizing_candidates.as_mut().unwrap()[0].ratio = 2.0;
    assert!(matches!(
        store.insert_risk_result(wrong_ratio).await,
        Err(StoreError::InvalidRecord { .. })
    ));

    let mut wrong_leg_id = new_risk_result("risk_wrong_leg_id");
    wrong_leg_id.result.sizing_candidates.as_mut().unwrap()[0].leg_id = LegId::from("leg_1");
    wrong_leg_id.result.adjusted_legs.as_mut().unwrap()[0].leg_id = LegId::from("leg_1");
    assert!(matches!(
        store.insert_risk_result(wrong_leg_id).await,
        Err(StoreError::InvalidRecord { .. })
    ));

    let mut wrong_stop = new_risk_result("risk_wrong_stop");
    wrong_stop.result.sizing_candidates.as_mut().unwrap()[0].stop_loss_price = 2_319.0;
    wrong_stop.result.adjusted_legs.as_mut().unwrap()[0].approved_sl = 2_319.0;
    assert!(matches!(
        store.insert_risk_result(wrong_stop).await,
        Err(StoreError::InvalidRecord { .. })
    ));

    let mut one_ulp_stop_drift = new_risk_result("risk_one_ulp_stop_drift");
    let drifted_stop = f64::from_bits(2_320.5_f64.to_bits() + 1);
    one_ulp_stop_drift
        .result
        .sizing_candidates
        .as_mut()
        .unwrap()[0]
        .stop_loss_price = drifted_stop;
    one_ulp_stop_drift.result.adjusted_legs.as_mut().unwrap()[0].approved_sl = drifted_stop;
    assert!(matches!(
        store.insert_risk_result(one_ulp_stop_drift).await,
        Err(StoreError::InvalidRecord { .. })
    ));
}

#[tokio::test]
async fn risk_result_repository_binds_multi_leg_sizing_by_leg_id() {
    let (_database, store, _) = test_store().await;
    store
        .insert_trade_intent(multi_leg_trade_intent())
        .await
        .unwrap();

    assert!(matches!(
        store
            .insert_risk_result(multi_leg_risk_result("risk_multi_valid"))
            .await
            .unwrap(),
        WriteOutcome::Inserted(_)
    ));

    let mut wrong_leg = multi_leg_risk_result("risk_multi_wrong_leg");
    wrong_leg.result.sizing_candidates.as_mut().unwrap()[1].leg_id = LegId::from("unknown_leg");
    wrong_leg.result.adjusted_legs.as_mut().unwrap()[1].leg_id = LegId::from("unknown_leg");
    assert!(matches!(
        store.insert_risk_result(wrong_leg).await,
        Err(StoreError::InvalidRecord { .. })
    ));

    let (_database, store, _) = test_store().await;
    let mut close_leg_intent = multi_leg_trade_intent();
    close_leg_intent.intent.proposed_legs.as_mut().unwrap()[1].action = TradeIntentLegAction::Close;
    store.insert_trade_intent(close_leg_intent).await.unwrap();
    assert!(matches!(
        store
            .insert_risk_result(multi_leg_risk_result("risk_multi_close_leg"))
            .await,
        Err(StoreError::InvalidRecord { .. })
    ));

    let (_database, store, _) = test_store().await;
    let mut duplicate_leg_intent = multi_leg_trade_intent();
    duplicate_leg_intent.intent.proposed_legs.as_mut().unwrap()[1].leg_id = LegId::from("leg_1");
    store
        .insert_trade_intent(duplicate_leg_intent)
        .await
        .unwrap();
    assert!(matches!(
        store
            .insert_risk_result(multi_leg_risk_result("risk_multi_duplicate_leg"))
            .await,
        Err(StoreError::InvalidRecord { .. })
    ));
}

#[tokio::test]
async fn risk_result_repository_enforces_approved_signal_time_contract() {
    let (_database, store, _) = test_store().await;
    seed_intent(&store).await;

    let mut before_request = new_risk_result("risk_before_request");
    before_request.result.evaluated_at = 999;
    assert!(matches!(
        store.insert_risk_result(before_request).await,
        Err(StoreError::InvalidRecord { .. })
    ));

    let mut at_expiry = new_risk_result("risk_at_expiry");
    at_expiry.result.evaluated_at = 5_000;
    at_expiry.result.valid_until = 5_001;
    assert!(matches!(
        store.insert_risk_result(at_expiry).await,
        Err(StoreError::InvalidRecord { .. })
    ));

    let mut extends_past_signal = new_risk_result("risk_past_signal");
    extends_past_signal.result.valid_until = 5_001;
    assert!(matches!(
        store.insert_risk_result(extends_past_signal).await,
        Err(StoreError::InvalidRecord { .. })
    ));

    let mut exact_boundaries = new_risk_result("risk_exact_boundaries");
    exact_boundaries.result.evaluated_at = 1_000;
    exact_boundaries.result.valid_until = 5_000;
    assert!(matches!(
        store.insert_risk_result(exact_boundaries).await.unwrap(),
        WriteOutcome::Inserted(_)
    ));

    let mut rejected_after_expiry = new_risk_result("risk_rejected_after_expiry");
    reject(
        &mut rejected_after_expiry.result,
        ErrorCode::TradeIntentExpired,
    );
    rejected_after_expiry.result.evaluated_at = 5_000;
    rejected_after_expiry.result.valid_until = 5_000;
    assert!(matches!(
        store
            .insert_risk_result(rejected_after_expiry)
            .await
            .unwrap(),
        WriteOutcome::Inserted(_)
    ));
}

#[tokio::test]
async fn typed_risk_result_read_detects_parent_action_and_time_contract_drift() {
    let (_database, store, pool) = test_store().await;
    seed_intent(&store).await;
    let result = new_risk_result("risk_parent_contract_drift");
    store.insert_risk_result(result.clone()).await.unwrap();

    let original_hash: String =
        sqlx::query_scalar("SELECT payload_hash FROM trade_intents WHERE intent_id = 'intent_1'")
            .fetch_one(&pool)
            .await
            .unwrap();
    sqlx::query("UPDATE trade_intents SET payload_hash = ? WHERE intent_id = 'intent_1'")
        .bind("0".repeat(64))
        .execute(&pool)
        .await
        .unwrap();
    assert!(matches!(
        store.get_risk_result(&result.result.risk_id).await,
        Err(StoreError::CorruptData { .. })
    ));
    sqlx::query("UPDATE trade_intents SET payload_hash = ? WHERE intent_id = 'intent_1'")
        .bind(original_hash)
        .execute(&pool)
        .await
        .unwrap();

    let drift_cases = [
        (
            "strategy_id alias",
            "UPDATE trade_intents SET strategy_id = 'strategy_2' WHERE intent_id = 'intent_1'",
            "UPDATE trade_intents SET strategy_id = 'strategy_1' WHERE intent_id = 'intent_1'",
        ),
        (
            "action",
            "UPDATE trade_intents SET action = 'HOLD' WHERE intent_id = 'intent_1'",
            "UPDATE trade_intents SET action = 'BUY' WHERE intent_id = 'intent_1'",
        ),
        (
            "requested_at",
            "UPDATE trade_intents SET requested_at = 1101 WHERE intent_id = 'intent_1'",
            "UPDATE trade_intents SET requested_at = 1000 WHERE intent_id = 'intent_1'",
        ),
        (
            "signal_expires_at evaluation boundary",
            "UPDATE trade_intents SET signal_expires_at = 1100 WHERE intent_id = 'intent_1'",
            "UPDATE trade_intents SET signal_expires_at = 5000 WHERE intent_id = 'intent_1'",
        ),
        (
            "signal_expires_at validity boundary",
            "UPDATE trade_intents SET signal_expires_at = 4999 WHERE intent_id = 'intent_1'",
            "UPDATE trade_intents SET signal_expires_at = 5000 WHERE intent_id = 'intent_1'",
        ),
    ];

    for (field, corrupt, restore) in drift_cases {
        sqlx::query(corrupt).execute(&pool).await.unwrap();
        assert!(
            matches!(
                store.get_risk_result(&result.result.risk_id).await,
                Err(StoreError::CorruptData { .. })
            ),
            "parent {field} drift should be detected"
        );
        sqlx::query(restore).execute(&pool).await.unwrap();
    }
}

#[tokio::test]
async fn risk_result_repository_rejects_corrupt_typed_parent_on_insert() {
    let (_database, store, pool) = test_store().await;
    seed_intent(&store).await;
    sqlx::query("UPDATE trade_intents SET strategy_id = 'strategy_2' WHERE intent_id = 'intent_1'")
        .execute(&pool)
        .await
        .unwrap();

    assert!(matches!(
        store
            .insert_risk_result(new_risk_result("risk_corrupt_parent"))
            .await,
        Err(StoreError::CorruptData { .. })
    ));
}

#[tokio::test]
async fn risk_result_repository_rejects_semantically_invalid_payloads_before_insert() {
    let (_database, store, pool) = test_store().await;
    seed_intent(&store).await;

    let mut rejected_with_sizing = new_risk_result("risk_rejected_with_sizing");
    rejected_with_sizing.result.approved = false;
    rejected_with_sizing.result.reason = ErrorCode::RiskLimitExceeded.into();
    assert!(matches!(
        store.insert_risk_result(rejected_with_sizing).await,
        Err(StoreError::InvalidRecord { .. })
    ));

    let mut non_finite_lots = new_risk_result("risk_non_finite_lots");
    non_finite_lots.result.adjusted_legs.as_mut().unwrap()[0].lots = f64::INFINITY;
    assert!(matches!(
        store.insert_risk_result(non_finite_lots).await,
        Err(StoreError::InvalidRecord { .. })
    ));

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM risk_results")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn write_transaction_exposes_atomic_risk_result_primitives() {
    let (_database, store, _) = test_store().await;
    let result = new_risk_result("risk_1");
    let mut transaction = store.begin_write().await.unwrap();
    transaction
        .insert_trade_intent(new_trade_intent())
        .await
        .unwrap();
    transaction
        .insert_risk_result(result.clone())
        .await
        .unwrap();
    assert_eq!(
        transaction
            .get_risk_result(&result.result.risk_id)
            .await
            .unwrap()
            .unwrap()
            .result,
        result.result
    );
    transaction.rollback().await.unwrap();

    assert!(store
        .get_trade_intent(&IntentId::from("intent_1"))
        .await
        .unwrap()
        .is_none());
    assert!(store
        .get_risk_result(&RiskId::from("risk_1"))
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn typed_risk_result_read_detects_every_denormalized_column_drift() {
    let (_database, store, pool) = test_store().await;
    seed_intent(&store).await;
    allow_risk_corruption(&pool).await;

    let cases = [
        (
            "risk_id",
            "UPDATE risk_results SET risk_id = 'risk_id_drift' WHERE risk_id = ?",
            Some(RiskId::from("risk_id_drift")),
        ),
        (
            "intent_id",
            "UPDATE risk_results SET intent_id = 'missing_intent' WHERE risk_id = ?",
            None,
        ),
        (
            "account_id",
            "UPDATE risk_results SET account_id = 'account_2' WHERE risk_id = ?",
            None,
        ),
        (
            "approved",
            "UPDATE risk_results SET approved = 0 WHERE risk_id = ?",
            None,
        ),
        (
            "reason",
            "UPDATE risk_results SET reason = 'BAD_REQUEST' WHERE risk_id = ?",
            None,
        ),
        (
            "snapshot_age_ms",
            "UPDATE risk_results SET snapshot_age_ms = 126 WHERE risk_id = ?",
            None,
        ),
        (
            "symbol_metadata_age_ms",
            "UPDATE risk_results SET symbol_metadata_age_ms = 251 WHERE risk_id = ?",
            None,
        ),
        (
            "evaluated_at",
            "UPDATE risk_results SET evaluated_at = 1101 WHERE risk_id = ?",
            None,
        ),
        (
            "valid_until",
            "UPDATE risk_results SET valid_until = 4999 WHERE risk_id = ?",
            None,
        ),
    ];

    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&pool)
        .await
        .unwrap();
    for (index, (column, update, alternate_lookup)) in cases.into_iter().enumerate() {
        let risk_id = RiskId::from(format!("risk_corrupt_{index}"));
        store
            .insert_risk_result(new_risk_result(risk_id.clone()))
            .await
            .unwrap();
        sqlx::query(update)
            .bind(risk_id.as_str())
            .execute(&pool)
            .await
            .unwrap();
        let lookup = alternate_lookup.as_ref().unwrap_or(&risk_id);
        assert!(
            matches!(
                store.get_risk_result(lookup).await,
                Err(StoreError::CorruptData { .. })
            ),
            "{column} drift should be detected"
        );
    }
}

#[tokio::test]
async fn typed_risk_result_read_detects_noncanonical_payload_and_hash_drift() {
    let (_database, store, pool) = test_store().await;
    seed_intent(&store).await;
    allow_risk_corruption(&pool).await;

    for risk_id in ["risk_bad_hash", "risk_noncanonical"] {
        store
            .insert_risk_result(new_risk_result(risk_id))
            .await
            .unwrap();
    }
    sqlx::query("UPDATE risk_results SET payload_hash = ? WHERE risk_id = 'risk_bad_hash'")
        .bind("0".repeat(64))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE risk_results \
         SET payload_json = ' {\"risk_id\":\"risk_noncanonical\"}', payload_hash = ? \
         WHERE risk_id = 'risk_noncanonical'",
    )
    .bind("0".repeat(64))
    .execute(&pool)
    .await
    .unwrap();

    for risk_id in ["risk_bad_hash", "risk_noncanonical"] {
        assert!(matches!(
            store.get_risk_result(&RiskId::from(risk_id)).await,
            Err(StoreError::CorruptData { .. })
        ));
    }
}

#[tokio::test]
async fn typed_risk_result_read_detects_semantically_invalid_canonical_payload() {
    let (_database, store, pool) = test_store().await;
    seed_intent(&store).await;
    allow_risk_corruption(&pool).await;

    let stored = new_risk_result("risk_invalid_semantics");
    store.insert_risk_result(stored.clone()).await.unwrap();

    let mut invalid = stored.result;
    invalid.sizing_candidates = None;
    let payload = CanonicalJson::from_serializable(&invalid).unwrap();
    sqlx::query("UPDATE risk_results SET payload_json = ?, payload_hash = ? WHERE risk_id = ?")
        .bind(payload.as_str())
        .bind(payload.sha256_hex())
        .bind(invalid.risk_id.as_str())
        .execute(&pool)
        .await
        .unwrap();

    assert!(matches!(
        store.get_risk_result(&invalid.risk_id).await,
        Err(StoreError::CorruptData { .. })
    ));
}
