use std::{sync::Arc, time::Duration};

use serde::Serialize;
use serde_json::json;
use sinan_core::{CoreInboundProcessor, CoreSessionResumeProcessor};
use sinan_execution::ServerClock;
use sinan_gateway::{DurableRecoveryBatchReport, DurableRecoveryConfig, DurableRecoveryDispatcher};
use sinan_protocol::{
    CommandInboxStatus, CommandReceived, ExecutionClientMessageType, MarketTick, ProtocolReason,
    ReconciliationReason, ReconciliationRequest, ReconciliationResult, ResumeCursor, TransportAck,
    TransportAckStatus, WireMessage, SUPPORTED_SCHEMA_VERSION,
};
use sinan_store::{
    AuthorizedAccountScope, CanonicalJson, ClaimWireOutbox, CompleteTransportWrite,
    CoreEventMetadata, DeadletterReason, DeliverySubject, DurableWorkStatus, NewExecutionCommand,
    NewExecutionPlan, NewExecutionWorkflow, NewInboundAdmission, NewReconciliationRun,
    NewReservedDelivery, NewRiskResult, NewSessionRecord, NewSessionResumeAdmission,
    NewTradeIntent, NewWireOutbox, OutboxClaimOutcome, ReconciliationRunStatus,
    ReserveOutboundSequence, SequenceReservation, SqliteStateStore, StoreOptions,
};
use sinan_types::{
    single_leg_id, AccountId, AccountSnapshot, AdjustedRiskLeg, AdjustedRiskLegAction,
    BrokerOrderId, CausationId, ClientId, ClockSyncStatus, CommandDeliveryAttemptStatus, CommandId,
    CorrelationId, DecisionId, ErrorCode, ErrorCodeOrString, ExecutionAction, ExecutionCommand,
    ExecutionCommandState, ExecutionCommandStatus, ExecutionFailurePolicy, ExecutionLeg,
    ExecutionLegDefinition, ExecutionLegState, ExecutionLegStatus, ExecutionPlan,
    ExecutionPlanDefinition, ExecutionPlanMode, ExecutionPlanState, ExecutionPlanStatus,
    FillingPolicy, IdempotencyKey, IntentId, MarketSnapshot, MessageId, OrderSnapshot,
    OrderSnapshotStatus, OrderType, PlanId, PositionId, PositionSide, PositionSnapshot, RequestId,
    RiskId, RiskResult, SessionId, SessionStatus, SizingCandidateProvenance, StrategyId,
    SymbolCode, TimePolicy, TimeframeCode, TradeIntent, TradeIntentAction, TradeIntentStatus,
    WireOutboxStatus,
};

const ACCOUNT_ID: &str = "account-1";
const CLIENT_ID: &str = "client-1";
const SESSION_ID: &str = "session-1";
const DISPATCH_AT: i64 = 500;
const COMMAND_CREATED_AT: i64 = 100;
const DELIVERY_CREATED_AT: i64 = 120;
const COMMAND_RECEIVED_AT: i64 = 130;
const INBOUND_RECEIVED_AT: i64 = 140;

struct FixedClock;

impl ServerClock for FixedClock {
    fn now_ms(&self) -> i64 {
        DISPATCH_AT
    }
}

async fn store() -> SqliteStateStore {
    let mut options = StoreOptions::new("sqlite::memory:");
    options.max_connections = 1;
    let store = SqliteStateStore::connect(options).await.unwrap();
    store
        .replace_active_session(NewSessionRecord {
            session_id: SessionId::from(SESSION_ID),
            client_id: ClientId::from(CLIENT_ID),
            account_id: AccountId::from(ACCOUNT_ID),
            terminal_id: None,
            platform: "MT5".to_owned(),
            status: SessionStatus::Active,
            capabilities: CanonicalJson::from_value(json!([
                "market.tick",
                "reconciliation.result"
            ]))
            .unwrap(),
            remote_addr: Some("127.0.0.1:5000".to_owned()),
            connected_at: 10,
            last_heartbeat_at: Some(10),
            last_time_sync_at: Some(10),
            clock_sync_status: Some(ClockSyncStatus::Synced),
            disconnected_at: None,
            max_inflight_commands: 8,
            updated_at: 10,
        })
        .await
        .unwrap();
    store
}

fn inbound<T: Serialize>(
    message_id: &str,
    message_type: ExecutionClientMessageType,
    sequence: u64,
    payload: T,
    received_at: i64,
) -> NewInboundAdmission {
    let message = WireMessage {
        message_id: MessageId::from(message_id),
        message_type,
        schema_version: SUPPORTED_SCHEMA_VERSION.to_string(),
        client_id: Some(ClientId::from(CLIENT_ID)),
        session_id: Some(SessionId::from(SESSION_ID)),
        correlation_id: None,
        causation_id: None,
        sent_at: Some(received_at - 1),
        sequence: Some(sequence),
        payload,
    };
    NewInboundAdmission {
        message_id: message.message_id.clone(),
        session_id: SessionId::from(SESSION_ID),
        client_id: ClientId::from(CLIENT_ID),
        account_id: AccountId::from(ACCOUNT_ID),
        terminal_id: None,
        message_type: message_type.to_string(),
        schema_version: SUPPORTED_SCHEMA_VERSION.to_string(),
        sequence,
        correlation_id: None,
        causation_id: None,
        envelope: CanonicalJson::from_serializable(&message).unwrap(),
        raw_payload_length: None,
        received_at,
        created_at: received_at,
    }
}

fn execution_workflow() -> NewExecutionWorkflow {
    let intent_id = IntentId::from("intent-command-receipt");
    let decision_id = DecisionId::from("decision-command-receipt");
    let risk_id = RiskId::from("risk-command-receipt");
    let plan_id = PlanId::from("plan-command-receipt");
    let strategy_id = StrategyId::from("strategy-command-receipt");
    let account_id = AccountId::from(ACCOUNT_ID);
    let symbol = SymbolCode::from("EURUSD");
    let leg_id = single_leg_id(&intent_id);
    let intent = TradeIntent {
        intent_id: intent_id.clone(),
        decision_id: decision_id.clone(),
        strategy_id: strategy_id.clone(),
        correlation_id: CorrelationId::from("correlation-command-receipt"),
        idempotency_key: IdempotencyKey::from("intent-key-command-receipt"),
        account_id: account_id.clone(),
        symbol: symbol.clone(),
        timeframe: TimeframeCode::from("M5"),
        action: TradeIntentAction::Buy,
        confidence: 0.8,
        reason: "command receipt owner transaction test".to_owned(),
        proposed_risk_pct: 1.0,
        proposed_sl: Some(1.09),
        proposed_tp: Some(1.12),
        proposed_legs: None,
        decision_timestamp: 50,
        signal_expires_at: 1_000,
        requested_at: 60,
    };
    let risk_result = RiskResult {
        risk_id: risk_id.clone(),
        request_id: RequestId::from("risk-request-command-receipt"),
        intent_id: intent_id.clone(),
        account_id: account_id.clone(),
        risk_request_hash: "b".repeat(64),
        approved: true,
        reason: ErrorCodeOrString::from("OK"),
        message: None,
        sizing_version: Some("fixed-risk-at-stop.v1".to_owned()),
        risk_base_amount: Some(10_000.0),
        risk_budget_amount: Some(100.0),
        adjusted_risk_pct: Some(1.0),
        sizing_candidates: Some(vec![SizingCandidateProvenance {
            leg_id: leg_id.clone(),
            symbol: symbol.clone(),
            action: AdjustedRiskLegAction::Buy,
            ratio: 1.0,
            worst_entry_price: 1.1,
            stop_loss_price: 1.09,
            estimated_cost_per_lot: 0.0,
        }]),
        adjusted_legs: Some(vec![AdjustedRiskLeg {
            leg_id: leg_id.clone(),
            symbol: symbol.clone(),
            action: AdjustedRiskLegAction::Buy,
            lots: 1.0,
            risk_amount: 100.0,
            risk_pct: 1.0,
            sizing_entry_price: 1.1,
            approved_sl: 1.09,
            loss_per_lot: 100.0,
            reason: Some(ErrorCodeOrString::from("OK")),
        }]),
        decision_id,
        snapshot_age_ms: 10,
        market_snapshot_age_ms: 10,
        symbol_metadata_age_ms: 10,
        capacity_age_ms: 10,
        evaluated_at: 80,
        valid_until: 1_000,
    };
    let command = ExecutionCommand {
        command_id: CommandId::from("command-receipt"),
        plan_id: Some(plan_id.clone()),
        leg_id: Some(leg_id.clone()),
        strategy_id: strategy_id.clone(),
        account_id: account_id.clone(),
        terminal_id: None,
        client_id: Some(ClientId::from(CLIENT_ID)),
        symbol: symbol.clone(),
        broker_symbol: Some("EURUSD.a".to_owned()),
        action: ExecutionAction::Buy,
        order_type: Some(OrderType::Market),
        lots: Some(1.0),
        price: None,
        sl: Some(1.09),
        tp: Some(1.12),
        deviation_points: Some(10),
        magic: 42,
        comment: Some("command receipt test".to_owned()),
        position_ticket: None,
        broker_order_id: None,
        filling_policy: Some(FillingPolicy::Ioc),
        time_policy: Some(TimePolicy::Gtc),
        expiration_time: None,
        expires_at: 900,
        idempotency_key: IdempotencyKey::from("command-key-receipt"),
        hmac: "a".repeat(64),
    };
    let plan = ExecutionPlan {
        definition: ExecutionPlanDefinition {
            plan_id: plan_id.clone(),
            account_id: account_id.clone(),
            strategy_id,
            mode: ExecutionPlanMode::Sequential,
            failure_policy: ExecutionFailurePolicy::CancelAll,
            rollback_policy: None,
        },
        legs: vec![ExecutionLeg {
            definition: ExecutionLegDefinition {
                leg_id: leg_id.clone(),
                symbol,
                action: ExecutionAction::Buy,
                lots: Some(1.0),
                sl: Some(1.09),
                tp: Some(1.12),
                ratio: 1.0,
                dependency: Vec::new(),
            },
            state: ExecutionLegState {
                status: ExecutionLegStatus::Pending,
            },
        }],
        state: ExecutionPlanState {
            status: ExecutionPlanStatus::Pending,
            filled_legs: Vec::new(),
            failed_legs: Vec::new(),
        },
    };
    let state = ExecutionCommandState {
        command_id: command.command_id.clone(),
        account_id,
        plan_id: command.plan_id.clone(),
        leg_id: command.leg_id.clone(),
        status: ExecutionCommandStatus::Created,
        delivery_attempts: 0,
        last_delivery_error: None,
        created_at: COMMAND_CREATED_AT,
        dispatched_at: None,
        command_received_at: None,
        reconciling_at: None,
        completed_at: None,
        updated_at: COMMAND_CREATED_AT,
    };

    NewExecutionWorkflow {
        intent: NewTradeIntent {
            intent,
            initial_status: TradeIntentStatus::Accepted,
            recorded_at: 70,
        },
        risk_result: NewRiskResult {
            result: risk_result,
        },
        plan: NewExecutionPlan {
            plan,
            risk_id: risk_id.clone(),
            intent_id,
            recorded_at: COMMAND_CREATED_AT,
        },
        commands: vec![NewExecutionCommand {
            command,
            risk_id,
            created_at: COMMAND_CREATED_AT,
        }],
        command_states: vec![state],
    }
}

async fn seed_execution_workflow(store: &SqliteStateStore) -> ExecutionCommand {
    let workflow = execution_workflow();
    let command = workflow.commands[0].command.clone();
    store.commit_execution_workflow(workflow).await.unwrap();
    command
}

async fn prepare_command_delivery(
    store: &SqliteStateStore,
    command: &ExecutionCommand,
    transport_write_evidence: bool,
) -> MessageId {
    let session = store
        .get_session(&SessionId::from(SESSION_ID))
        .await
        .unwrap()
        .unwrap();
    let mut transaction = store.begin_write().await.unwrap();
    let reservation = match transaction
        .reserve_outbound_sequence(ReserveOutboundSequence {
            session_id: session.session_id,
            expected_revision: session.revision,
            subject: DeliverySubject::ExecutionCommand(command.command_id.clone()),
            fresh_after: 0,
            reserved_at: DELIVERY_CREATED_AT,
        })
        .await
        .unwrap()
    {
        SequenceReservation::Reserved(reservation) => reservation,
        other => panic!("expected command delivery reservation, got {other:?}"),
    };
    let message_id = MessageId::from("execution-command-message");
    let wire = WireMessage {
        message_id: message_id.clone(),
        message_type: ExecutionClientMessageType::ExecutionCommand,
        schema_version: SUPPORTED_SCHEMA_VERSION.to_string(),
        client_id: Some(reservation.client_id.clone()),
        session_id: Some(reservation.session_id.clone()),
        correlation_id: None,
        causation_id: None,
        sent_at: Some(DELIVERY_CREATED_AT),
        sequence: Some(reservation.sequence),
        payload: command.clone(),
    };
    let delivery = transaction
        .enqueue_reserved_delivery(NewReservedDelivery {
            reservation,
            attempt_id: "execution-command-attempt".to_owned(),
            message_id: message_id.clone(),
            message_type: ExecutionClientMessageType::ExecutionCommand.to_string(),
            envelope: CanonicalJson::from_serializable(&wire).unwrap(),
            created_at: DELIVERY_CREATED_AT,
        })
        .await
        .unwrap();
    transaction.commit().await.unwrap();

    if transport_write_evidence {
        let claimed = match store
            .claim_outbox(ClaimWireOutbox {
                message_id: message_id.clone(),
                expected_outbox_revision: delivery.outbox.revision,
                expected_attempt_revision: delivery.attempt.revision,
                fresh_after: 0,
                require_synced_clock: true,
                claimed_at: DELIVERY_CREATED_AT + 1,
            })
            .await
            .unwrap()
        {
            OutboxClaimOutcome::Claimed(delivery) => delivery,
            other => panic!("expected claimed command delivery, got {other:?}"),
        };
        store
            .finish_transport_write_sent(CompleteTransportWrite {
                message_id: message_id.clone(),
                expected_outbox_revision: claimed.outbox.revision,
                expected_attempt_revision: claimed.attempt.revision,
                completed_at: DELIVERY_CREATED_AT + 2,
                error: None,
            })
            .await
            .unwrap();
    }

    message_id
}

fn command_receipt(
    command: &ExecutionCommand,
    inbox_status: CommandInboxStatus,
    reason: Option<ProtocolReason>,
) -> CommandReceived {
    CommandReceived {
        command_id: command.command_id.clone(),
        idempotency_key: command.idempotency_key.clone(),
        account_id: command.account_id.clone(),
        terminal_id: command.terminal_id.clone(),
        client_id: command.client_id.clone(),
        received_at: COMMAND_RECEIVED_AT,
        inbox_status,
        reason,
    }
}

fn command_received_inbound(
    message_id: &str,
    source_message_id: &MessageId,
    sequence: u64,
    receipt: CommandReceived,
) -> NewInboundAdmission {
    let causation_id = CausationId::from(source_message_id.as_str());
    let message = WireMessage {
        message_id: MessageId::from(message_id),
        message_type: ExecutionClientMessageType::CommandReceived,
        schema_version: SUPPORTED_SCHEMA_VERSION.to_string(),
        client_id: Some(ClientId::from(CLIENT_ID)),
        session_id: Some(SessionId::from(SESSION_ID)),
        correlation_id: None,
        causation_id: Some(causation_id.clone()),
        sent_at: Some(COMMAND_RECEIVED_AT),
        sequence: Some(sequence),
        payload: receipt,
    };
    NewInboundAdmission {
        message_id: message.message_id.clone(),
        session_id: SessionId::from(SESSION_ID),
        client_id: ClientId::from(CLIENT_ID),
        account_id: AccountId::from(ACCOUNT_ID),
        terminal_id: None,
        message_type: ExecutionClientMessageType::CommandReceived.to_string(),
        schema_version: SUPPORTED_SCHEMA_VERSION.to_string(),
        sequence,
        correlation_id: None,
        causation_id: Some(causation_id),
        envelope: CanonicalJson::from_serializable(&message).unwrap(),
        raw_payload_length: None,
        received_at: INBOUND_RECEIVED_AT,
        created_at: INBOUND_RECEIVED_AT,
    }
}

async fn dispatch_one(store: &SqliteStateStore) -> DurableRecoveryBatchReport {
    let dispatcher = DurableRecoveryDispatcher::new(
        store.clone(),
        Arc::new(CoreInboundProcessor),
        Arc::new(CoreSessionResumeProcessor),
        Arc::new(FixedClock),
        DurableRecoveryConfig {
            worker_id: "core-inbound-test".to_owned(),
            max_items_per_batch: 1,
            lease_duration: Duration::from_secs(1),
            handler_timeout: Duration::from_millis(100),
            finalization_budget: Duration::from_millis(100),
        },
    )
    .unwrap();
    dispatcher.dispatch_batch().await.unwrap()
}

fn reconciliation_event_metadata(request_id: &RequestId, requested_at: i64) -> CoreEventMetadata {
    CoreEventMetadata {
        event_id: format!("request:{request_id}"),
        event_type: ExecutionClientMessageType::ReconciliationRequest.to_string(),
        aggregate_type: "reconciliation".to_owned(),
        aggregate_id: request_id.to_string(),
        message_id: None,
        schema_version: SUPPORTED_SCHEMA_VERSION.to_string(),
        correlation_id: None,
        causation_id: None,
        account_id: Some(AccountId::from(ACCOUNT_ID)),
        client_id: None,
        terminal_id: None,
        strategy_id: None,
        intent_id: None,
        plan_id: None,
        leg_id: None,
        command_id: None,
        idempotency_key: None,
        event_at: requested_at,
        received_at: requested_at,
        created_at: requested_at,
        source: "core-inbound-test".to_owned(),
    }
}

fn account(observed_at: i64) -> AccountSnapshot {
    AccountSnapshot {
        account_id: AccountId::from(ACCOUNT_ID),
        balance: 9_900.0,
        equity: 10_000.0,
        margin: 100.0,
        free_margin: 9_900.0,
        currency: "USD".to_owned(),
        observed_at,
    }
}

fn position(observed_at: i64) -> PositionSnapshot {
    PositionSnapshot {
        account_id: AccountId::from(ACCOUNT_ID),
        symbol: SymbolCode::from("EURUSD"),
        position_id: PositionId::from("position-1"),
        side: PositionSide::Buy,
        lots: 0.2,
        open_price: 1.1,
        sl: Some(1.09),
        tp: Some(1.12),
        floating_pnl: 15.0,
        observed_at,
    }
}

fn order(observed_at: i64) -> OrderSnapshot {
    OrderSnapshot {
        account_id: AccountId::from(ACCOUNT_ID),
        terminal_id: None,
        client_id: None,
        symbol: SymbolCode::from("EURUSD"),
        broker_symbol: Some("EURUSD.a".to_owned()),
        broker_order_id: BrokerOrderId::from("order-1"),
        position_ticket: None,
        command_id: None,
        plan_id: None,
        leg_id: None,
        idempotency_key: None,
        side: PositionSide::Buy,
        order_type: OrderType::Limit,
        status: OrderSnapshotStatus::Placed,
        requested_lots: 0.1,
        filled_lots: 0.0,
        remaining_lots: 0.1,
        price: Some(1.095),
        sl: Some(1.085),
        tp: Some(1.115),
        created_at: Some(observed_at - 20),
        updated_at: Some(observed_at - 10),
        observed_at,
    }
}

#[tokio::test]
async fn market_tick_owner_transaction_updates_latest_projection() {
    let store = store().await;
    let message_id = MessageId::from("tick-1");
    store
        .admit_inbound(inbound(
            message_id.as_str(),
            ExecutionClientMessageType::MarketTick,
            1,
            MarketTick {
                account_id: AccountId::from(ACCOUNT_ID),
                symbol: SymbolCode::from("EURUSD"),
                broker_symbol: Some("EURUSD.a".to_owned()),
                bid: 1.1,
                ask: 1.1002,
                last: Some(1.1001),
                volume: Some(12.0),
                observed_at: 100,
            },
            110,
        ))
        .await
        .unwrap();

    let report = dispatch_one(&store).await;

    let admission = store
        .get_inbound_admission(&message_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.handled, 1, "admission: {admission:?}");
    let projection = store
        .load_latest_state(&AuthorizedAccountScope::new([AccountId::from(ACCOUNT_ID)]))
        .await
        .unwrap();
    assert_eq!(projection.markets.len(), 1);
    assert_eq!(
        projection.markets[0].account_id,
        AccountId::from(ACCOUNT_ID)
    );
    assert_eq!(
        projection.markets[0].snapshot.symbol,
        SymbolCode::from("EURUSD")
    );
    assert_eq!(projection.markets[0].snapshot.bid, 1.1);
    assert_eq!(projection.markets[0].snapshot.ask, 1.1002);
    assert_eq!(projection.markets[0].snapshot.observed_at, 100);
    assert_eq!(admission.status, DurableWorkStatus::Handled);
}

#[tokio::test]
async fn malformed_typed_payload_is_atomically_deadlettered_without_projection() {
    let store = store().await;
    let message_id = MessageId::from("tick-malformed");
    store
        .admit_inbound(inbound(
            message_id.as_str(),
            ExecutionClientMessageType::MarketTick,
            1,
            json!({"account_id": ACCOUNT_ID, "symbol": "EURUSD"}),
            110,
        ))
        .await
        .unwrap();

    let report = dispatch_one(&store).await;

    assert_eq!(report.handled, 0);
    assert_eq!(report.failed, 1);
    let admission = store
        .get_inbound_admission(&message_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(admission.status, DurableWorkStatus::Failed);
    let deadletter = store
        .get_deadletter_event("durable-inbound:tick-malformed")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(deadletter.message_id, Some(message_id));
    assert_eq!(deadletter.message_type.as_deref(), Some("market.tick"));
    assert_eq!(deadletter.schema_version.as_deref(), Some("ecp.v1.0"));
    assert_eq!(deadletter.reason, DeadletterReason::SchemaValidationFailed);
    assert_eq!(deadletter.raw_payload, None);
    assert_eq!(deadletter.raw_payload_length, admission.raw_payload_length);
    assert_eq!(deadletter.error_message, admission.last_error.unwrap());
    let projection = store
        .load_latest_state(&AuthorizedAccountScope::new([AccountId::from(ACCOUNT_ID)]))
        .await
        .unwrap();
    assert!(projection.markets.is_empty());
}

#[tokio::test]
async fn deterministic_store_observation_conflict_is_terminal() {
    let store = store().await;
    let account_id = AccountId::from(ACCOUNT_ID);
    store
        .update_market_snapshot(
            &account_id,
            &MarketSnapshot {
                symbol: SymbolCode::from("EURUSD"),
                broker_symbol: Some("EURUSD.a".to_owned()),
                bid: 1.1,
                ask: 1.1002,
                spread: 0.0002,
                observed_at: 100,
            },
            105,
        )
        .await
        .unwrap();
    let message_id = MessageId::from("tick-observation-conflict");
    store
        .admit_inbound(inbound(
            message_id.as_str(),
            ExecutionClientMessageType::MarketTick,
            1,
            MarketTick {
                account_id: account_id.clone(),
                symbol: SymbolCode::from("EURUSD"),
                broker_symbol: Some("EURUSD.a".to_owned()),
                bid: 1.2,
                ask: 1.2002,
                last: Some(1.2001),
                volume: Some(12.0),
                observed_at: 100,
            },
            110,
        ))
        .await
        .unwrap();

    let report = dispatch_one(&store).await;

    assert_eq!(report.failed, 1);
    let admission = store
        .get_inbound_admission(&message_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(admission.status, DurableWorkStatus::Failed);
    assert!(admission
        .last_error
        .as_deref()
        .is_some_and(|error| error.contains("observation conflict")));
    assert_eq!(
        store
            .get_deadletter_event("durable-inbound:tick-observation-conflict")
            .await
            .unwrap()
            .unwrap()
            .reason,
        DeadletterReason::SchemaValidationFailed
    );
    let projection = store
        .load_latest_state(&AuthorizedAccountScope::new([account_id]))
        .await
        .unwrap();
    assert_eq!(projection.markets[0].snapshot.bid, 1.1);
}

#[tokio::test]
async fn transport_ack_owner_transaction_updates_matching_outbox() {
    let store = store().await;
    let outbound_message_id = MessageId::from("heartbeat-1");
    let outbound = WireMessage {
        message_id: outbound_message_id.clone(),
        message_type: ExecutionClientMessageType::Heartbeat,
        schema_version: SUPPORTED_SCHEMA_VERSION.to_string(),
        client_id: Some(ClientId::from(CLIENT_ID)),
        session_id: Some(SessionId::from(SESSION_ID)),
        correlation_id: None,
        causation_id: None,
        sent_at: Some(120),
        sequence: Some(1),
        payload: json!({"heartbeat": true}),
    };
    store
        .enqueue_wire_outbox(NewWireOutbox {
            message_id: outbound_message_id.clone(),
            session_id: Some(SessionId::from(SESSION_ID)),
            message_type: ExecutionClientMessageType::Heartbeat.to_string(),
            sequence: Some(1),
            command_id: None,
            request_id: None,
            payload: CanonicalJson::from_serializable(&outbound).unwrap(),
            status: WireOutboxStatus::Sent,
            created_at: 100,
            updated_at: 120,
            sent_at: Some(120),
            acked_at: None,
            last_error: None,
        })
        .await
        .unwrap();
    let ack_message_id = MessageId::from("ack-1");
    store
        .admit_inbound(inbound(
            ack_message_id.as_str(),
            ExecutionClientMessageType::TransportAck,
            1,
            TransportAck {
                acked_message_id: outbound_message_id.clone(),
                acked_message_type: ExecutionClientMessageType::Heartbeat,
                status: TransportAckStatus::Accepted,
                reason: None,
                received_at: 130,
            },
            140,
        ))
        .await
        .unwrap();

    let report = dispatch_one(&store).await;

    let admission = store
        .get_inbound_admission(&ack_message_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.handled, 1, "admission: {admission:?}");
    let outbox = store
        .get_wire_outbox(&outbound_message_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(outbox.status, WireOutboxStatus::Acked);
    assert_eq!(outbox.sent_at, Some(120));
    assert_eq!(outbox.acked_at, Some(130));
    assert_eq!(outbox.last_error, None);
    assert_eq!(admission.status, DurableWorkStatus::Handled);
}

#[tokio::test]
async fn recorded_command_receipt_recovers_dispatch_and_acks_attempt_atomically() {
    let store = store().await;
    let command = seed_execution_workflow(&store).await;
    let source_message_id = prepare_command_delivery(&store, &command, true).await;
    let before = store
        .get_execution_command_state(&command.command_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(before.status, ExecutionCommandStatus::Created);
    assert_eq!(before.delivery_attempts, 0);

    let receipt = command_receipt(
        &command,
        CommandInboxStatus::Recorded,
        Some(ProtocolReason::Ok),
    );
    let receipt_message_id = MessageId::from("command-received-recorded");
    store
        .admit_inbound(command_received_inbound(
            receipt_message_id.as_str(),
            &source_message_id,
            1,
            receipt.clone(),
        ))
        .await
        .unwrap();

    let report = dispatch_one(&store).await;

    let admission = store
        .get_inbound_admission(&receipt_message_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.handled, 1, "admission: {admission:?}");
    assert_eq!(report.failed, 0, "admission: {admission:?}");
    assert_eq!(admission.status, DurableWorkStatus::Handled);
    let state = store
        .get_execution_command_state(&command.command_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state.status, ExecutionCommandStatus::CommandReceived);
    assert_eq!(state.delivery_attempts, 1);
    assert_eq!(state.dispatched_at, Some(DELIVERY_CREATED_AT));
    assert_eq!(state.command_received_at, Some(COMMAND_RECEIVED_AT));
    let delivery = store
        .get_outbound_delivery(&source_message_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(delivery.outbox.status, WireOutboxStatus::Sent);
    assert_eq!(delivery.attempt.status, CommandDeliveryAttemptStatus::Acked);
    assert_eq!(delivery.attempt.acked_at, Some(COMMAND_RECEIVED_AT));
    let event = store
        .get_core_event(&format!("inbound:{receipt_message_id}"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        event.metadata.event_type,
        ExecutionClientMessageType::CommandReceived.as_str()
    );
    assert_eq!(
        event.payload,
        CanonicalJson::from_serializable(&receipt).unwrap()
    );
    let plan = store
        .get_execution_plan(command.plan_id.as_ref().unwrap())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        plan.plan.legs[0].state.status,
        ExecutionLegStatus::CommandReceived
    );
}

#[tokio::test]
async fn terminal_command_receipts_append_fact_without_acking_or_advancing_lifecycle() {
    for (message_id, status, reason) in [
        (
            "command-received-expired",
            CommandInboxStatus::Expired,
            ProtocolReason::Error(ErrorCode::CommandExpired),
        ),
        (
            "command-received-rejected",
            CommandInboxStatus::Rejected,
            ProtocolReason::Error(ErrorCode::SchemaValidationFailed),
        ),
    ] {
        let store = store().await;
        let command = seed_execution_workflow(&store).await;
        let source_message_id = prepare_command_delivery(&store, &command, true).await;
        let receipt = command_receipt(&command, status, Some(reason));
        let receipt_message_id = MessageId::from(message_id);
        store
            .admit_inbound(command_received_inbound(
                receipt_message_id.as_str(),
                &source_message_id,
                1,
                receipt.clone(),
            ))
            .await
            .unwrap();

        let report = dispatch_one(&store).await;

        assert_eq!(report.handled, 1, "receipt status {status:?}");
        assert_eq!(report.failed, 0, "receipt status {status:?}");
        let admission = store
            .get_inbound_admission(&receipt_message_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(admission.status, DurableWorkStatus::Handled);
        let event = store
            .get_core_event(&format!("inbound:{receipt_message_id}"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            event.payload,
            CanonicalJson::from_serializable(&receipt).unwrap()
        );
        let state = store
            .get_execution_command_state(&command.command_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(state.status, ExecutionCommandStatus::Created);
        assert_eq!(state.delivery_attempts, 0);
        assert_eq!(state.dispatched_at, None);
        assert_eq!(state.command_received_at, None);
        let delivery = store
            .get_outbound_delivery(&source_message_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(delivery.outbox.status, WireOutboxStatus::Sent);
        assert_eq!(delivery.attempt.status, CommandDeliveryAttemptStatus::Sent);
        assert_eq!(delivery.attempt.acked_at, None);
        let plan = store
            .get_execution_plan(command.plan_id.as_ref().unwrap())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(plan.plan.legs[0].state.status, ExecutionLegStatus::Pending);
    }
}

#[tokio::test]
async fn recorded_command_receipt_without_transport_write_evidence_rolls_back() {
    let store = store().await;
    let command = seed_execution_workflow(&store).await;
    let source_message_id = prepare_command_delivery(&store, &command, false).await;
    let receipt = command_receipt(
        &command,
        CommandInboxStatus::Recorded,
        Some(ProtocolReason::Ok),
    );
    let receipt_message_id = MessageId::from("command-received-without-write-evidence");
    store
        .admit_inbound(command_received_inbound(
            receipt_message_id.as_str(),
            &source_message_id,
            1,
            receipt,
        ))
        .await
        .unwrap();

    let report = dispatch_one(&store).await;

    assert_eq!(report.handled, 0);
    assert_eq!(report.failed, 1);
    let admission = store
        .get_inbound_admission(&receipt_message_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(admission.status, DurableWorkStatus::Failed);
    assert!(admission
        .last_error
        .as_deref()
        .is_some_and(|error| error.contains("no transport-write evidence")));
    assert!(store
        .get_core_event(&format!("inbound:{receipt_message_id}"))
        .await
        .unwrap()
        .is_none());
    let state = store
        .get_execution_command_state(&command.command_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state.status, ExecutionCommandStatus::Created);
    assert_eq!(state.delivery_attempts, 0);
    assert_eq!(state.dispatched_at, None);
    assert_eq!(state.command_received_at, None);
    let delivery = store
        .get_outbound_delivery(&source_message_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(delivery.outbox.status, WireOutboxStatus::Pending);
    assert_eq!(
        delivery.attempt.status,
        CommandDeliveryAttemptStatus::Pending
    );
    assert_eq!(delivery.attempt.acked_at, None);
}

#[tokio::test]
async fn reconciliation_result_owner_transaction_commits_evaluation_full_sets_and_checkpoint() {
    let store = store().await;
    let request_id = RequestId::from("reconciliation-1");
    let requested_at = 100;
    store
        .create_reconciliation_run(NewReconciliationRun {
            request: ReconciliationRequest {
                request_id: request_id.clone(),
                account_id: AccountId::from(ACCOUNT_ID),
                terminal_id: None,
                client_id: None,
                reason: ReconciliationReason::ManualRequest,
                command_ids: None,
                since_server_time: None,
            },
            requested_at,
            event_metadata: reconciliation_event_metadata(&request_id, requested_at),
        })
        .await
        .unwrap();

    let observed_at = 200;
    let expected_account = account(observed_at);
    let expected_position = position(observed_at);
    let expected_order = order(observed_at);
    let message_id = MessageId::from("reconciliation-result-1");
    store
        .admit_inbound(inbound(
            message_id.as_str(),
            ExecutionClientMessageType::ReconciliationResult,
            1,
            ReconciliationResult {
                request_id: request_id.clone(),
                account_id: AccountId::from(ACCOUNT_ID),
                terminal_id: None,
                client_id: None,
                observed_at,
                account: Some(expected_account.clone()),
                positions: vec![expected_position.clone()],
                orders: vec![expected_order.clone()],
                symbol_metadata: Vec::new(),
                unresolved_command_ids: Vec::new(),
            },
            210,
        ))
        .await
        .unwrap();

    let report = dispatch_one(&store).await;

    let admission = store
        .get_inbound_admission(&message_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.handled, 1, "admission: {admission:?}");
    let run = store
        .get_reconciliation_run(&request_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.status, ReconciliationRunStatus::Completed);
    assert_eq!(
        run.result.as_ref().unwrap().account,
        Some(expected_account.clone())
    );
    assert_eq!(
        run.result.as_ref().unwrap().positions,
        vec![expected_position.clone()]
    );
    assert_eq!(
        run.result.as_ref().unwrap().orders,
        vec![expected_order.clone()]
    );
    let completeness = run.completeness.unwrap();
    assert!(!completeness.symbol_metadata_complete);
    assert!(completeness.command_scope_complete);

    let projection = store
        .load_latest_state(&AuthorizedAccountScope::new([AccountId::from(ACCOUNT_ID)]))
        .await
        .unwrap();
    assert_eq!(projection.accounts, vec![expected_account]);
    assert_eq!(projection.positions, vec![expected_position]);
    assert_eq!(projection.orders, vec![expected_order]);
    let checkpoint = store
        .get_account_reconciliation_checkpoint(&AccountId::from(ACCOUNT_ID))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(checkpoint.source_request_id, request_id);
    assert_eq!(checkpoint.result_observed_at, observed_at);
    assert_eq!(checkpoint.account_refreshed_at, Some(observed_at));
    assert_eq!(checkpoint.positions_observed_at, observed_at);
    assert_eq!(checkpoint.orders_observed_at, observed_at);
    assert_eq!(checkpoint.pending_commands_reconciled_at, Some(observed_at));
    assert_eq!(admission.status, DurableWorkStatus::Handled);
}

#[tokio::test]
async fn session_resume_creates_reconciliation_run_without_replaying_commands() {
    let store = store().await;
    let hello_message_id = MessageId::from("hello-resume-1");
    let pending_a = CommandId::from("command-a");
    let pending_z = CommandId::from("command-z");
    let old_gateway_message_id = MessageId::from("old-gateway-message");
    store
        .admit_session_resume(NewSessionResumeAdmission {
            hello_message_id: hello_message_id.clone(),
            session_id: SessionId::from(SESSION_ID),
            client_id: ClientId::from(CLIENT_ID),
            account_id: AccountId::from(ACCOUNT_ID),
            terminal_id: None,
            cursor: CanonicalJson::from_serializable(&ResumeCursor {
                previous_session_id: Some(SessionId::from("old-session")),
                last_gateway_message_id: Some(old_gateway_message_id.clone()),
                last_gateway_sequence: Some(9),
                last_client_message_id: Some(MessageId::from("old-client-message")),
                last_client_sequence: Some(8),
                pending_command_ids: Some(vec![pending_z.clone(), pending_a.clone()]),
            })
            .unwrap(),
            received_at: 100,
            created_at: 100,
        })
        .await
        .unwrap();

    let report = dispatch_one(&store).await;

    assert_eq!(report.handled, 1);
    let admission = store
        .get_session_resume_admission(&hello_message_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(admission.status, DurableWorkStatus::Handled);
    let request_id = RequestId::from(format!("reconciliation:resume:{hello_message_id}"));
    assert_eq!(
        admission.reconciliation_request_id,
        Some(request_id.clone())
    );
    let run = store
        .get_reconciliation_run(&request_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.status, ReconciliationRunStatus::Requested);
    assert_eq!(run.request.reason, ReconciliationReason::ConnectionRestored);
    assert_eq!(run.request.command_ids, None);
    assert!(store
        .get_execution_command(&pending_a)
        .await
        .unwrap()
        .is_none());
    assert!(store
        .get_execution_command(&pending_z)
        .await
        .unwrap()
        .is_none());
    assert!(store
        .get_wire_outbox(&old_gateway_message_id)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn malformed_session_resume_cursor_is_terminal_without_reconciliation_run() {
    let store = store().await;
    let hello_message_id = MessageId::from("hello-resume-malformed");
    let duplicate = CommandId::from("command-duplicate");
    store
        .admit_session_resume(NewSessionResumeAdmission {
            hello_message_id: hello_message_id.clone(),
            session_id: SessionId::from(SESSION_ID),
            client_id: ClientId::from(CLIENT_ID),
            account_id: AccountId::from(ACCOUNT_ID),
            terminal_id: None,
            cursor: CanonicalJson::from_serializable(&ResumeCursor {
                pending_command_ids: Some(vec![duplicate.clone(), duplicate]),
                ..ResumeCursor::default()
            })
            .unwrap(),
            received_at: 100,
            created_at: 100,
        })
        .await
        .unwrap();

    let report = dispatch_one(&store).await;

    assert_eq!(report.failed, 1);
    let admission = store
        .get_session_resume_admission(&hello_message_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(admission.status, DurableWorkStatus::Failed);
    assert!(admission
        .last_error
        .as_deref()
        .is_some_and(|error| error.contains("duplicate command identities")));
    let request_id = RequestId::from(format!("reconciliation:resume:{hello_message_id}"));
    assert!(store
        .get_reconciliation_run(&request_id)
        .await
        .unwrap()
        .is_none());
}
