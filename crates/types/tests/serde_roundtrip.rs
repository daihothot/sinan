use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, Value};
use sinan_types::{
    single_leg_id, AccountId, AdjustedRiskLeg, AdjustedRiskLegAction, ErrorCode, ErrorCodeOrString,
    ExecutionAction, ExecutionCommand, ExecutionCommandState, ExecutionCommandStatus,
    FillingPolicy, IdempotencyKey, IntentId, OrderType, PlanId, RiskResult,
    SizingCandidateProvenance, StrategyId, SymbolCode, SymbolMetadataSnapshot, SymbolTradeMode,
    TimePolicy, TradeIntent, TradeIntentAction, WireInboxStatus,
};
use std::fmt::Debug;

fn assert_json_round_trip<T>(value: &T)
where
    T: Debug + PartialEq + Serialize + DeserializeOwned,
{
    let encoded = serde_json::to_string(value).expect("value should serialize");
    let decoded = serde_json::from_str(&encoded).expect("value should deserialize");
    assert_eq!(*value, decoded);
}

fn valid_trade_intent_json() -> Value {
    json!({
        "intent_id": "intent_001",
        "decision_id": "decision_001",
        "strategy_id": "trend_xau_h4_v1",
        "correlation_id": "corr_001",
        "idempotency_key": "idem_intent_001",
        "account_id": "acct_mt5_001",
        "symbol": "XAUUSD",
        "timeframe": "H4",
        "action": "BUY",
        "confidence": 0.84,
        "reason": "trend confirmed",
        "proposed_risk_pct": 1.0,
        "proposed_sl": 2320.5,
        "signal_expires_at": 1779800123000_i64,
        "requested_at": 1779800000123_i64
    })
}

fn valid_risk_result() -> RiskResult {
    RiskResult {
        risk_id: "risk_001".into(),
        request_id: "risk_request_001".into(),
        intent_id: "intent_001".into(),
        account_id: AccountId::from("acct_mt5_001"),
        risk_request_hash: "a".repeat(64),
        approved: true,
        reason: ErrorCodeOrString::from("OK"),
        message: None,
        sizing_version: Some("fixed-risk-at-stop.v1".to_owned()),
        risk_base_amount: Some(10_000.0),
        risk_budget_amount: Some(100.0),
        adjusted_risk_pct: Some(0.98),
        sizing_candidates: Some(vec![SizingCandidateProvenance {
            leg_id: "leg_1".into(),
            symbol: SymbolCode::from("XAUUSD"),
            action: AdjustedRiskLegAction::Buy,
            ratio: 1.0,
            worst_entry_price: 2_350.0,
            stop_loss_price: 2_336.0,
            estimated_cost_per_lot: 0.0,
        }]),
        adjusted_legs: Some(vec![AdjustedRiskLeg {
            leg_id: "leg_1".into(),
            symbol: SymbolCode::from("XAUUSD"),
            action: AdjustedRiskLegAction::Buy,
            lots: 0.07,
            risk_amount: 98.0,
            risk_pct: 0.98,
            sizing_entry_price: 2_350.0,
            approved_sl: 2_336.0,
            loss_per_lot: 1_400.0,
            reason: Some(ErrorCodeOrString::from("OK")),
        }]),
        decision_id: "decision_001".into(),
        snapshot_age_ms: 125,
        market_snapshot_age_ms: 75,
        symbol_metadata_age_ms: 250,
        capacity_age_ms: 100,
        evaluated_at: 1_779_800_000_123,
        valid_until: 1_779_800_005_123,
    }
}

fn assert_invalid_risk_result(result: RiskResult, expected_field: &str) {
    let error = result
        .validate()
        .expect_err("risk result should be invalid");
    assert_eq!(error.field(), expected_field);
}

#[test]
fn string_newtype_is_json_transparent() {
    let account_id = AccountId::from("acct_mt5_001");

    assert_eq!(account_id.as_ref(), "acct_mt5_001");
    assert_eq!(account_id.to_string(), "acct_mt5_001");
    assert_eq!(
        serde_json::to_value(&account_id).unwrap(),
        json!("acct_mt5_001")
    );
    assert_json_round_trip(&account_id);
    assert_eq!(
        single_leg_id(&IntentId::from("intent_001")).as_str(),
        "leg:intent_001:0"
    );
}

#[test]
fn every_error_code_uses_its_protocol_name() {
    for code in ErrorCode::ALL {
        assert_eq!(serde_json::to_value(code).unwrap(), json!(code.as_str()));
        assert_json_round_trip(code);
    }

    assert!(serde_json::from_value::<ErrorCode>(json!("NOT_A_REAL_CODE")).is_err());
}

#[test]
fn risk_error_codes_use_their_protocol_names() {
    for (code, expected) in [
        (ErrorCode::MarketSnapshotStale, "MARKET_SNAPSHOT_STALE"),
        (ErrorCode::RiskInputInvalid, "RISK_INPUT_INVALID"),
        (ErrorCode::RiskLimitExceeded, "RISK_LIMIT_EXCEEDED"),
        (ErrorCode::ExposureLimitExceeded, "EXPOSURE_LIMIT_EXCEEDED"),
        (ErrorCode::PositionLimitExceeded, "POSITION_LIMIT_EXCEEDED"),
        (
            ErrorCode::RiskReductionNotProvable,
            "RISK_REDUCTION_NOT_PROVABLE",
        ),
        (
            ErrorCode::PendingExposureConflict,
            "PENDING_EXPOSURE_CONFLICT",
        ),
    ] {
        assert_eq!(code.as_str(), expected);
        assert_eq!(serde_json::to_value(code).unwrap(), json!(expected));
    }
}

#[test]
fn status_and_policy_enums_use_documented_names() {
    assert_eq!(
        serde_json::to_value(WireInboxStatus::Deadletter).unwrap(),
        json!("DEADLETTER")
    );
    assert_eq!(ExecutionAction::Buy.to_string(), "BUY");
    assert_eq!(FillingPolicy::Ioc.to_string(), "IOC");
    assert_eq!(TimePolicy::Gtc.to_string(), "GTC");
    assert!(serde_json::from_value::<WireInboxStatus>(json!("NOT_A_STATUS")).is_err());
}

#[test]
fn trade_intent_round_trips_with_account_and_idempotency_key() {
    let json = valid_trade_intent_json();

    let intent: TradeIntent = serde_json::from_value(json.clone()).unwrap();
    assert_eq!(intent.account_id.as_str(), "acct_mt5_001");
    assert_eq!(intent.idempotency_key.as_str(), "idem_intent_001");
    assert_eq!(intent.action, TradeIntentAction::Buy);
    assert_eq!(serde_json::to_value(&intent).unwrap(), json);
}

#[test]
fn trade_intent_rejects_missing_account_and_idempotency_key() {
    for required_field in ["account_id", "idempotency_key"] {
        let mut json = valid_trade_intent_json();
        json.as_object_mut().unwrap().remove(required_field);

        assert!(
            serde_json::from_value::<TradeIntent>(json).is_err(),
            "missing {required_field} should be rejected"
        );
    }
}

#[test]
fn trade_intent_rejects_execution_command_fields() {
    for (forbidden_field, value) in [
        ("broker_order_id", json!("broker_order_001")),
        ("magic", json!(26_052_601)),
        ("filling_policy", json!("IOC")),
        ("lots", json!(0.1)),
        ("order_type", json!("MARKET")),
        ("hmac", json!("not-accepted-on-an-intent")),
    ] {
        let mut json = valid_trade_intent_json();
        json.as_object_mut()
            .unwrap()
            .insert(forbidden_field.to_owned(), value);

        assert!(
            serde_json::from_value::<TradeIntent>(json).is_err(),
            "forbidden field {forbidden_field} should be rejected"
        );
    }
}

#[test]
fn risk_result_round_trips_with_approved_lots_and_ok_reasons() {
    let result = valid_risk_result();

    result.validate().unwrap();
    assert_json_round_trip(&result);
    let encoded = serde_json::to_value(result).unwrap();
    assert_eq!(encoded["reason"], json!("OK"));
    assert_eq!(encoded["adjusted_legs"][0]["action"], json!("BUY"));
    assert_eq!(encoded["adjusted_legs"][0]["reason"], json!("OK"));
    assert!(encoded.get("message").is_none());
}

#[test]
fn risk_result_deserializes_known_error_codes_and_rejects_non_risk_leg_actions() {
    let rejected: RiskResult = serde_json::from_value(json!({
        "risk_id": "risk_002",
        "request_id": "risk_request_002",
        "intent_id": "intent_001",
        "account_id": "acct_mt5_001",
        "risk_request_hash": "b".repeat(64),
        "approved": false,
        "reason": "ACCOUNT_SNAPSHOT_STALE",
        "message": "account snapshot is stale",
        "decision_id": "decision_001",
        "snapshot_age_ms": 5_001,
        "market_snapshot_age_ms": 0,
        "symbol_metadata_age_ms": 250,
        "capacity_age_ms": 0,
        "evaluated_at": 1_779_800_000_123_i64,
        "valid_until": 1_779_800_000_123_i64
    }))
    .unwrap();
    assert_eq!(
        rejected.reason,
        ErrorCodeOrString::Known(ErrorCode::AccountSnapshotStale)
    );
    rejected.validate().unwrap();
    assert_json_round_trip(&rejected);

    assert!(serde_json::from_value::<AdjustedRiskLegAction>(json!("CLOSE")).is_err());
}

#[test]
fn risk_result_validation_accepts_approved_no_op_and_rejected_shapes() {
    let mut no_op = valid_risk_result();
    no_op.sizing_version = None;
    no_op.risk_base_amount = None;
    no_op.risk_budget_amount = None;
    no_op.adjusted_risk_pct = None;
    no_op.sizing_candidates = None;
    no_op.adjusted_legs = None;
    no_op.validate().unwrap();

    let mut rejected = no_op;
    rejected.approved = false;
    rejected.reason = ErrorCode::RiskLimitExceeded.into();
    rejected.valid_until = rejected.evaluated_at;
    rejected.validate().unwrap();
}

#[test]
fn risk_result_validation_rejects_unknown_rejection_reasons() {
    let mut result = valid_risk_result();
    result.approved = false;
    result.reason = ErrorCodeOrString::from("NOT_A_CENTRAL_ERROR_CODE");
    result.sizing_version = None;
    result.risk_base_amount = None;
    result.risk_budget_amount = None;
    result.adjusted_risk_pct = None;
    result.sizing_candidates = None;
    result.adjusted_legs = None;
    result.valid_until = result.evaluated_at;

    assert_invalid_risk_result(result, "reason");
}

#[test]
fn risk_result_validation_detects_relative_drift_for_small_risk_amounts() {
    let mut result = valid_risk_result();
    let leg = &mut result.adjusted_legs.as_mut().unwrap()[0];
    leg.lots = 1e-12;
    leg.loss_per_lot = 1.0;
    leg.risk_amount = 1e-10;
    leg.risk_pct = 1e-12;
    result.risk_budget_amount = Some(1e-10);
    result.adjusted_risk_pct = Some(1e-12);

    assert_invalid_risk_result(result, "adjusted_legs[].risk_amount");
}

#[test]
fn risk_result_validation_rejects_inconsistent_decision_shapes() {
    let mut approved_with_error = valid_risk_result();
    approved_with_error.reason = ErrorCode::RiskLimitExceeded.into();
    assert_invalid_risk_result(approved_with_error, "reason");

    let mut rejected_with_lots = valid_risk_result();
    rejected_with_lots.approved = false;
    rejected_with_lots.reason = ErrorCode::RiskLimitExceeded.into();
    assert_invalid_risk_result(rejected_with_lots, "sizing");

    let mut partial_sizing = valid_risk_result();
    partial_sizing.sizing_candidates = None;
    assert_invalid_risk_result(partial_sizing, "sizing");

    let mut expired_approval = valid_risk_result();
    expired_approval.valid_until = expired_approval.evaluated_at;
    assert_invalid_risk_result(expired_approval, "valid_until");
}

#[test]
fn risk_result_validation_rejects_illegal_numbers_times_and_duplicate_legs() {
    let mut invalid_hash = valid_risk_result();
    invalid_hash.risk_request_hash = "ABC".to_owned();
    assert_invalid_risk_result(invalid_hash, "risk_request_hash");

    let mut negative_age = valid_risk_result();
    negative_age.market_snapshot_age_ms = -1;
    assert_invalid_risk_result(negative_age, "market_snapshot_age_ms");

    let mut negative_capacity_age = valid_risk_result();
    negative_capacity_age.capacity_age_ms = -1;
    assert_invalid_risk_result(negative_capacity_age, "capacity_age_ms");

    let mut reversed_time = valid_risk_result();
    reversed_time.valid_until = reversed_time.evaluated_at - 1;
    assert_invalid_risk_result(reversed_time, "valid_until");

    let mut non_finite = valid_risk_result();
    non_finite.adjusted_legs.as_mut().unwrap()[0].lots = f64::NAN;
    assert_invalid_risk_result(non_finite, "adjusted_legs[].lots");

    let mut duplicate_candidate = valid_risk_result();
    let candidate = duplicate_candidate.sizing_candidates.as_ref().unwrap()[0].clone();
    duplicate_candidate
        .sizing_candidates
        .as_mut()
        .unwrap()
        .push(candidate);
    let adjusted = duplicate_candidate.adjusted_legs.as_ref().unwrap()[0].clone();
    duplicate_candidate
        .adjusted_legs
        .as_mut()
        .unwrap()
        .push(adjusted);
    assert_invalid_risk_result(duplicate_candidate, "sizing_candidates[].leg_id");

    let mut duplicate_adjusted = valid_risk_result();
    let mut candidate = duplicate_adjusted.sizing_candidates.as_ref().unwrap()[0].clone();
    candidate.leg_id = "leg_2".into();
    duplicate_adjusted
        .sizing_candidates
        .as_mut()
        .unwrap()
        .push(candidate);
    let adjusted = duplicate_adjusted.adjusted_legs.as_ref().unwrap()[0].clone();
    duplicate_adjusted
        .adjusted_legs
        .as_mut()
        .unwrap()
        .push(adjusted);
    assert_invalid_risk_result(duplicate_adjusted, "adjusted_legs[].leg_id");
}

#[test]
fn execution_command_state_round_trips_with_account_id() {
    let state = ExecutionCommandState {
        command_id: "cmd_20260526_000001".into(),
        account_id: AccountId::from("acct_mt5_001"),
        plan_id: Some("plan_20260526_000001".into()),
        leg_id: Some("leg_1".into()),
        status: ExecutionCommandStatus::Dispatched,
        delivery_attempts: 1,
        last_delivery_error: None,
        created_at: 1_779_800_000_100,
        dispatched_at: Some(1_779_800_000_123),
        command_received_at: None,
        reconciling_at: None,
        completed_at: None,
        updated_at: 1_779_800_000_123,
    };

    assert_json_round_trip(&state);
    assert_eq!(
        serde_json::to_value(state).unwrap()["account_id"],
        json!("acct_mt5_001")
    );
}

#[test]
fn symbol_metadata_round_trips_with_tick_value_loss() {
    let metadata = SymbolMetadataSnapshot {
        account_id: AccountId::from("acct_mt5_001"),
        symbol: SymbolCode::from("XAUUSD"),
        broker_symbol: "XAUUSD".to_owned(),
        digits: 2,
        point: 0.01,
        tick_size: 0.01,
        tick_value_loss: 1.25,
        contract_size: 100.0,
        volume_min: 0.01,
        volume_max: 100.0,
        volume_step: 0.01,
        stops_level_points: 10,
        freeze_level_points: 5,
        margin_initial: None,
        margin_maintenance: None,
        trade_mode: SymbolTradeMode::Full,
        observed_at: 1_779_800_000_123,
    };

    assert_json_round_trip(&metadata);
    assert_eq!(
        serde_json::to_value(metadata).unwrap()["tick_value_loss"],
        json!(1.25)
    );
}

#[test]
fn execution_command_round_trips_and_omits_missing_fields() {
    let command = ExecutionCommand {
        command_id: "cmd_20260526_000001".into(),
        plan_id: Some(PlanId::from("plan_20260526_000001")),
        leg_id: Some("leg_1".into()),
        strategy_id: StrategyId::from("trend_xau_h4_v1"),
        account_id: AccountId::from("acct_mt5_001"),
        terminal_id: Some("mt5_terminal_001".into()),
        client_id: Some("mt5_client_001".into()),
        symbol: SymbolCode::from("XAUUSD"),
        broker_symbol: Some("XAUUSD".to_owned()),
        action: ExecutionAction::Buy,
        order_type: Some(OrderType::Market),
        lots: Some(0.1),
        price: None,
        sl: Some(2320.5),
        tp: Some(2365.5),
        deviation_points: Some(20),
        magic: 26_052_601,
        comment: Some("trend_xau_h4".to_owned()),
        position_ticket: None,
        broker_order_id: None,
        filling_policy: Some(FillingPolicy::Ioc),
        time_policy: Some(TimePolicy::Gtc),
        expiration_time: None,
        expires_at: 1_779_800_123_000,
        idempotency_key: IdempotencyKey::from("idem_cmd_20260526_000001"),
        hmac: "044916a7aac911c86b107a0fb7ddb21529f2e8dcb755d3d0183d8fd3589f1d2e".to_owned(),
    };

    assert_json_round_trip(&command);
    let encoded = serde_json::to_value(command).unwrap();
    assert_eq!(encoded["action"], json!("BUY"));
    assert_eq!(encoded["order_type"], json!("MARKET"));
    assert!(encoded.get("price").is_none());
    assert!(encoded.get("expiration_time").is_none());
}
