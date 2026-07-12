use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, Value};
use sinan_types::{
    AccountId, ErrorCode, ExecutionAction, ExecutionCommand, ExecutionCommandState,
    ExecutionCommandStatus, FillingPolicy, IdempotencyKey, OrderType, PlanId, StrategyId,
    SymbolCode, SymbolMetadataSnapshot, SymbolTradeMode, TimePolicy, TradeIntent,
    TradeIntentAction, WireInboxStatus,
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
