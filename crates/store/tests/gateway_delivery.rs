use std::sync::Arc;

use serde_json::json;
use sinan_protocol::{
    ExecutionClientMessageType, ReconciliationReason, ReconciliationRequest, TransportAckStatus,
    WireMessage,
};
use sinan_store::{
    CanonicalJson, ClaimWireOutbox, CommandReceivedAttemptUpdate, CompleteTransportWrite,
    ControlSequenceReservation, DeliveryAttemptTimeout, DeliveryRejectionKind, DeliverySubject,
    ExactSessionClose, NewDeliveryAttempt, NewReservedDelivery, NewSessionRecord,
    OutboxClaimOutcome, ReserveControlOutboundSequence, ReserveOutboundSequence,
    SequenceReservation, SessionHeartbeatUpdate, SessionRouteQuery, SessionRouteResolution,
    SessionStatusUpdate, SqliteStateStore, StoreError, StoredOutboundDelivery,
    TRANSPORT_ACK_REJECTED_PREFIX,
};
use sinan_types::{
    AccountId, ClientId, ClockSyncStatus, CommandDeliveryAttemptStatus, CommandId, ExecutionAction,
    ExecutionCommand, FillingPolicy, IdempotencyKey, MessageId, OrderType, RequestId, SessionId,
    SessionStatus, StrategyId, SymbolCode, TerminalId, TimePolicy, WireOutboxStatus,
};
use sqlx::SqlitePool;

mod common;

use common::test_store;

const HASH: &str = "0000000000000000000000000000000000000000000000000000000000000000";

fn command(command_id: &str, expires_at: i64) -> ExecutionCommand {
    ExecutionCommand {
        command_id: CommandId::from(command_id),
        plan_id: None,
        leg_id: None,
        strategy_id: StrategyId::from("strategy-1"),
        account_id: AccountId::from("account-1"),
        terminal_id: Some(TerminalId::from("terminal-1")),
        client_id: Some(ClientId::from("client-1")),
        symbol: SymbolCode::from("EURUSD"),
        broker_symbol: Some("EURUSD".to_owned()),
        action: ExecutionAction::Buy,
        order_type: Some(OrderType::Market),
        lots: Some(0.1),
        price: None,
        sl: Some(1.05),
        tp: Some(1.15),
        deviation_points: Some(10),
        magic: 7,
        comment: Some("gateway-test".to_owned()),
        position_ticket: None,
        broker_order_id: None,
        filling_policy: Some(FillingPolicy::Ioc),
        time_policy: Some(TimePolicy::Gtc),
        expiration_time: None,
        expires_at,
        idempotency_key: IdempotencyKey::from(format!("idem-{command_id}")),
        hmac: "a".repeat(64),
    }
}

async fn seed_execution_parent(pool: &SqlitePool) {
    sqlx::query(
        "INSERT OR IGNORE INTO trade_intents (\
             intent_id, decision_id, strategy_id, account_id, symbol, action, status, \
             decision_timestamp, requested_at, signal_expires_at, idempotency_key, payload_json, \
             payload_hash, created_at, updated_at\
         ) VALUES ('intent-1', 'decision-1', 'strategy-1', 'account-1', 'EURUSD', 'BUY', \
                   'ACCEPTED', 0, 1, 100000, 'intent-idem-1', '{}', ?, 1, 1)",
    )
    .bind(HASH)
    .execute(pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT OR IGNORE INTO risk_results (\
             risk_id, intent_id, account_id, approved, reason, snapshot_age_ms, \
             symbol_metadata_age_ms, evaluated_at, valid_until, payload_json, payload_hash\
         ) VALUES ('risk-1', 'intent-1', 'account-1', 1, 'OK', 0, 0, 2, 100000, '{}', ?)",
    )
    .bind(HASH)
    .execute(pool)
    .await
    .unwrap();
}

async fn seed_command(pool: &SqlitePool, value: &ExecutionCommand) {
    seed_execution_parent(pool).await;
    let payload = CanonicalJson::from_serializable(value).unwrap();
    sqlx::query(
        "INSERT INTO execution_commands (\
             command_id, risk_id, account_id, client_id, terminal_id, symbol, action, expires_at, \
             idempotency_key, payload_json, payload_hash, hmac, created_at\
         ) VALUES (?, 'risk-1', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 3)",
    )
    .bind(value.command_id.as_str())
    .bind(value.account_id.as_str())
    .bind(value.client_id.as_ref().map(ClientId::as_str))
    .bind(value.terminal_id.as_ref().map(TerminalId::as_str))
    .bind(value.symbol.as_str())
    .bind(value.action.as_str())
    .bind(value.expires_at)
    .bind(value.idempotency_key.as_str())
    .bind(payload.as_str())
    .bind(payload.sha256_hex())
    .bind(&value.hmac)
    .execute(pool)
    .await
    .unwrap();
}

fn active_session(
    session_id: &str,
    client_id: &str,
    terminal_id: &str,
    at: i64,
    max_inflight_commands: u64,
) -> NewSessionRecord {
    NewSessionRecord {
        session_id: SessionId::from(session_id),
        client_id: ClientId::from(client_id),
        account_id: AccountId::from("account-1"),
        terminal_id: Some(TerminalId::from(terminal_id)),
        platform: "MT5".to_owned(),
        status: SessionStatus::Active,
        capabilities: CanonicalJson::from_value(json!(["MARKET_ORDER"])).unwrap(),
        remote_addr: Some("127.0.0.1:5000".to_owned()),
        connected_at: at,
        last_heartbeat_at: Some(at),
        last_time_sync_at: Some(at),
        clock_sync_status: Some(ClockSyncStatus::Synced),
        disconnected_at: None,
        max_inflight_commands,
        updated_at: at,
    }
}

fn route(require_synced_clock: bool) -> SessionRouteQuery {
    SessionRouteQuery {
        account_id: AccountId::from("account-1"),
        client_id: Some(ClientId::from("client-1")),
        terminal_id: Some(TerminalId::from("terminal-1")),
        fresh_after: 0,
        require_synced_clock,
    }
}

async fn prepare_command(
    transaction: &mut sinan_store::WriteTransaction,
    value: &ExecutionCommand,
    message_id: &str,
    attempt_id: &str,
    at: i64,
) -> StoredOutboundDelivery {
    let session = match transaction
        .resolve_session_route(route(true))
        .await
        .unwrap()
    {
        SessionRouteResolution::Ready(session) => session,
        other => panic!("expected ready route, got {other:?}"),
    };
    let reservation = match transaction
        .reserve_outbound_sequence(ReserveOutboundSequence {
            session_id: session.session_id.clone(),
            expected_revision: session.revision,
            subject: DeliverySubject::ExecutionCommand(value.command_id.clone()),
            fresh_after: 0,
            reserved_at: at,
        })
        .await
        .unwrap()
    {
        SequenceReservation::Reserved(reservation) => reservation,
        other => panic!("expected reservation, got {other:?}"),
    };
    let wire = WireMessage {
        message_id: MessageId::from(message_id),
        message_type: ExecutionClientMessageType::ExecutionCommand,
        schema_version: "ecp.v1.0".to_owned(),
        client_id: Some(reservation.client_id.clone()),
        session_id: Some(reservation.session_id.clone()),
        correlation_id: None,
        causation_id: None,
        sent_at: Some(at),
        sequence: Some(reservation.sequence),
        payload: value.clone(),
    };
    transaction
        .enqueue_reserved_delivery(NewReservedDelivery {
            reservation,
            attempt_id: attempt_id.to_owned(),
            message_id: MessageId::from(message_id),
            message_type: "execution.command".to_owned(),
            envelope: CanonicalJson::from_serializable(&wire).unwrap(),
            created_at: at,
        })
        .await
        .unwrap()
}

async fn prepare_and_commit(
    store: SqliteStateStore,
    value: ExecutionCommand,
    message_id: &'static str,
    attempt_id: &'static str,
    at: i64,
) -> StoredOutboundDelivery {
    let mut transaction = store.begin_write().await.unwrap();
    let delivery = prepare_command(&mut transaction, &value, message_id, attempt_id, at).await;
    transaction.commit().await.unwrap();
    delivery
}

async fn claim(
    store: &SqliteStateStore,
    delivery: &StoredOutboundDelivery,
    at: i64,
) -> StoredOutboundDelivery {
    match store
        .claim_outbox(ClaimWireOutbox {
            message_id: delivery.outbox.message_id.clone(),
            expected_outbox_revision: delivery.outbox.revision,
            expected_attempt_revision: delivery.attempt.revision,
            fresh_after: 0,
            require_synced_clock: true,
            claimed_at: at,
        })
        .await
        .unwrap()
    {
        OutboxClaimOutcome::Claimed(delivery) => delivery,
        other => panic!("expected claimed delivery, got {other:?}"),
    }
}

async fn probe_reservation(
    store: &SqliteStateStore,
    subject: DeliverySubject,
    reserved_at: i64,
) -> SequenceReservation {
    let mut transaction = store.begin_write().await.unwrap();
    let session = match transaction
        .resolve_session_route(route(true))
        .await
        .unwrap()
    {
        SessionRouteResolution::Ready(session) => session,
        other => panic!("expected ready route, got {other:?}"),
    };
    let outcome = transaction
        .reserve_outbound_sequence(ReserveOutboundSequence {
            session_id: session.session_id,
            expected_revision: session.revision,
            subject,
            fresh_after: 0,
            reserved_at,
        })
        .await
        .unwrap();
    transaction.rollback().await.unwrap();
    outcome
}

#[tokio::test]
async fn control_sequence_reservations_share_the_durable_session_cursor() {
    let (_database, store, pool) = test_store().await;
    let value = command("command-control-interleave", 10_000);
    seed_command(&pool, &value).await;
    store
        .replace_active_session(active_session(
            "session-control-1",
            "client-1",
            "terminal-1",
            100,
            8,
        ))
        .await
        .unwrap();

    let mut transaction = store.begin_write().await.unwrap();
    let first = transaction
        .reserve_control_outbound_sequence(ReserveControlOutboundSequence {
            session_id: SessionId::from("session-control-1"),
            reserved_at: 100,
        })
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    let ControlSequenceReservation::Reserved(first) = first else {
        panic!("expected the first control sequence reservation");
    };
    assert_eq!(first.sequence, 2);
    assert_eq!(first.session_revision, 1);

    let second = store
        .reserve_control_outbound_sequence(ReserveControlOutboundSequence {
            session_id: SessionId::from("session-control-1"),
            reserved_at: 101,
        })
        .await
        .unwrap();
    let ControlSequenceReservation::Reserved(second) = second else {
        panic!("expected the second control sequence reservation");
    };
    assert_eq!(second.sequence, 3);
    assert_eq!(second.session_revision, 2);

    let business_store = store.clone();
    let (control, business) = tokio::join!(
        store.reserve_control_outbound_sequence(ReserveControlOutboundSequence {
            session_id: SessionId::from("session-control-1"),
            reserved_at: 102,
        }),
        prepare_and_commit(
            business_store,
            value,
            "message-control-interleave",
            "attempt-control-interleave",
            102,
        ),
    );
    let ControlSequenceReservation::Reserved(control) = control.unwrap() else {
        panic!("expected the concurrent control sequence reservation");
    };
    let mut concurrent_sequences = vec![
        control.sequence,
        business
            .outbox
            .sequence
            .expect("reserved business delivery should have a sequence"),
    ];
    concurrent_sequences.sort_unstable();
    assert_eq!(concurrent_sequences, vec![4, 5]);

    let before_replacement = store
        .get_session(&SessionId::from("session-control-1"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(before_replacement.last_outbound_sequence, 5);
    assert_eq!(before_replacement.revision, 4);
    assert_eq!(before_replacement.updated_at, 102);

    let replacement = store
        .replace_active_session(active_session(
            "session-control-2",
            "client-1",
            "terminal-1",
            103,
            8,
        ))
        .await
        .unwrap();
    let stale = replacement.replaced_session.unwrap();
    assert_eq!(stale.status, SessionStatus::Stale);
    assert_eq!(stale.last_outbound_sequence, 5);
    assert!(matches!(
        store
            .reserve_control_outbound_sequence(ReserveControlOutboundSequence {
                session_id: stale.session_id.clone(),
                reserved_at: 103,
            })
            .await
            .unwrap(),
        ControlSequenceReservation::SessionUnavailable
    ));
    assert_eq!(
        store
            .get_session(&stale.session_id)
            .await
            .unwrap()
            .unwrap()
            .last_outbound_sequence,
        5
    );

    assert!(matches!(
        store
            .reserve_control_outbound_sequence(ReserveControlOutboundSequence {
                session_id: replacement.session.session_id.clone(),
                reserved_at: 102,
            })
            .await,
        Err(StoreError::StaleWrite {
            entity: "execution_client_session",
            ..
        })
    ));
    let active = store
        .get_session(&replacement.session.session_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(active.last_outbound_sequence, 1);
    assert_eq!(active.revision, 0);
    assert_eq!(active.updated_at, 103);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exact_close_wins_against_reservations_and_is_idempotent_after_terminal_state() {
    let (_database, store, pool) = test_store().await;
    let value = command("command-exact-close-race", 10_000);
    seed_command(&pool, &value).await;
    store
        .replace_active_session(active_session(
            "session-exact-close",
            "client-1",
            "terminal-1",
            100,
            64,
        ))
        .await
        .unwrap();

    let barrier = Arc::new(tokio::sync::Barrier::new(33));
    let mut tasks = Vec::new();
    for index in 0..32 {
        let store = store.clone();
        let barrier = Arc::clone(&barrier);
        let command_id = value.command_id.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            if index % 2 == 0 {
                let outcome = store
                    .reserve_control_outbound_sequence(ReserveControlOutboundSequence {
                        session_id: SessionId::from("session-exact-close"),
                        reserved_at: 200,
                    })
                    .await
                    .unwrap();
                assert!(matches!(
                    outcome,
                    ControlSequenceReservation::Reserved(_)
                        | ControlSequenceReservation::SessionUnavailable
                ));
                return;
            }

            let mut transaction = store.begin_write().await.unwrap();
            match transaction
                .resolve_session_route(route(true))
                .await
                .unwrap()
            {
                SessionRouteResolution::Ready(session) => {
                    let outcome = transaction
                        .reserve_outbound_sequence(ReserveOutboundSequence {
                            session_id: session.session_id,
                            expected_revision: session.revision,
                            subject: DeliverySubject::ExecutionCommand(command_id),
                            fresh_after: 0,
                            reserved_at: 200,
                        })
                        .await
                        .unwrap();
                    assert!(matches!(outcome, SequenceReservation::Reserved(_)));
                    transaction.commit().await.unwrap();
                }
                SessionRouteResolution::NoActiveSession => {
                    transaction.rollback().await.unwrap();
                }
                other => panic!("unexpected route during exact-close race: {other:?}"),
            }
        }));
    }

    barrier.wait().await;
    let closed = store
        .disconnect_exact_session(ExactSessionClose {
            session_id: SessionId::from("session-exact-close"),
            changed_at: 200,
            delivery_error: "CONNECTION_TASK_DROPPED".to_owned(),
        })
        .await
        .unwrap();
    assert_eq!(closed.session.status, SessionStatus::Disconnected);

    for task in tasks {
        task.await.unwrap();
    }
    let terminal = store
        .get_session(&SessionId::from("session-exact-close"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(terminal.status, SessionStatus::Disconnected);

    for close_as_stale in [false, true] {
        let request = ExactSessionClose {
            session_id: terminal.session_id.clone(),
            changed_at: 201,
            delivery_error: "DUPLICATE_CLOSE".to_owned(),
        };
        let repeated = if close_as_stale {
            store.mark_exact_session_stale(request).await.unwrap()
        } else {
            store.disconnect_exact_session(request).await.unwrap()
        };
        assert_eq!(repeated.session, terminal);
        assert!(repeated.unconfirmed_attempts.is_empty());
    }

    store
        .replace_active_session(active_session(
            "session-exact-stale",
            "client-2",
            "terminal-2",
            300,
            8,
        ))
        .await
        .unwrap();
    let stale = store
        .mark_exact_session_stale(ExactSessionClose {
            session_id: SessionId::from("session-exact-stale"),
            changed_at: 301,
            delivery_error: "HEARTBEAT_TIMEOUT".to_owned(),
        })
        .await
        .unwrap()
        .session;
    assert_eq!(stale.status, SessionStatus::Stale);
    let repeated = store
        .disconnect_exact_session(ExactSessionClose {
            session_id: stale.session_id.clone(),
            changed_at: 302,
            delivery_error: "LATE_DISCONNECT".to_owned(),
        })
        .await
        .unwrap();
    assert_eq!(repeated.session, stale);
}

#[tokio::test]
async fn session_registry_is_cas_fenced_fail_closed_and_route_ambiguous() {
    let (_database, store, _) = test_store().await;
    let first = store
        .replace_active_session(active_session(
            "session-1",
            "client-1",
            "terminal-1",
            100,
            8,
        ))
        .await
        .unwrap();
    assert_eq!(first.session.revision, 0);
    assert_eq!(first.session.last_outbound_sequence, 1);

    let unsynced = store
        .update_session_heartbeat(SessionHeartbeatUpdate {
            session_id: first.session.session_id.clone(),
            expected_revision: 0,
            heartbeat_at: 100,
            clock_sync_status: ClockSyncStatus::Unsynced,
            last_time_sync_at: None,
            updated_at: 100,
        })
        .await
        .unwrap();
    assert_eq!(unsynced.clock_sync_status, Some(ClockSyncStatus::Unsynced));
    assert_eq!(unsynced.last_time_sync_at, Some(100));
    let mut transaction = store.begin_write().await.unwrap();
    assert!(matches!(
        transaction
            .resolve_session_route(route(true))
            .await
            .unwrap(),
        SessionRouteResolution::ClockUnhealthy { candidate_count: 1 }
    ));
    transaction.rollback().await.unwrap();
    assert!(matches!(
        store
            .update_session_heartbeat(SessionHeartbeatUpdate {
                session_id: first.session.session_id.clone(),
                expected_revision: 0,
                heartbeat_at: 101,
                clock_sync_status: ClockSyncStatus::Synced,
                last_time_sync_at: Some(101),
                updated_at: 101,
            })
            .await,
        Err(StoreError::StaleWrite { .. })
    ));

    let replacement = store
        .replace_active_session(active_session(
            "session-2",
            "client-1",
            "terminal-1",
            100,
            8,
        ))
        .await
        .unwrap();
    assert_eq!(
        replacement.replaced_session.unwrap().status,
        SessionStatus::Stale
    );
    assert!(matches!(
        store
            .update_session_heartbeat(SessionHeartbeatUpdate {
                session_id: SessionId::from("session-1"),
                expected_revision: 1,
                heartbeat_at: 101,
                clock_sync_status: ClockSyncStatus::Synced,
                last_time_sync_at: Some(101),
                updated_at: 101,
            })
            .await,
        Err(StoreError::StaleWrite { .. })
    ));

    store
        .replace_active_session(active_session(
            "session-3",
            "client-2",
            "terminal-2",
            100,
            8,
        ))
        .await
        .unwrap();
    let mut transaction = store.begin_write().await.unwrap();
    assert!(matches!(
        transaction
            .resolve_session_route(SessionRouteQuery {
                account_id: AccountId::from("account-1"),
                client_id: None,
                terminal_id: None,
                fresh_after: 0,
                require_synced_clock: false,
            })
            .await
            .unwrap(),
        SessionRouteResolution::Ambiguous { candidate_count: 2 }
    ));
    transaction.rollback().await.unwrap();
}

#[tokio::test]
async fn concurrent_session_replacements_leave_one_active_and_fence_the_loser() {
    let (_database, store, pool) = test_store().await;
    store
        .replace_active_session(active_session(
            "session-1",
            "client-1",
            "terminal-1",
            100,
            8,
        ))
        .await
        .unwrap();

    let left_store = store.clone();
    let right_store = store.clone();
    let (left, right) = tokio::join!(
        left_store.replace_active_session(active_session(
            "session-2",
            "client-1",
            "terminal-1",
            101,
            8,
        )),
        right_store.replace_active_session(active_session(
            "session-3",
            "client-1",
            "terminal-1",
            101,
            8,
        )),
    );
    left.unwrap();
    right.unwrap();

    let second = store
        .get_session(&SessionId::from("session-2"))
        .await
        .unwrap()
        .unwrap();
    let third = store
        .get_session(&SessionId::from("session-3"))
        .await
        .unwrap()
        .unwrap();
    let active_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM execution_client_sessions \
         WHERE client_id = 'client-1' AND account_id = 'account-1' \
           AND terminal_id = 'terminal-1' AND status = 'ACTIVE'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(active_count, 1);

    let loser = if second.status == SessionStatus::Stale {
        second
    } else {
        assert_eq!(third.status, SessionStatus::Stale);
        third
    };
    assert!(matches!(
        store
            .update_session_heartbeat(SessionHeartbeatUpdate {
                session_id: loser.session_id,
                expected_revision: loser.revision,
                heartbeat_at: 102,
                clock_sync_status: ClockSyncStatus::Synced,
                last_time_sync_at: Some(102),
                updated_at: 102,
            })
            .await,
        Err(StoreError::StaleWrite { .. })
    ));
}

#[tokio::test]
async fn claim_revalidates_clock_and_session_freshness() {
    let (_database, store, pool) = test_store().await;
    let clock_unhealthy = command("command-clock-unhealthy", 10_000);
    let stale_session = command("command-stale-session", 10_000);
    seed_command(&pool, &clock_unhealthy).await;
    seed_command(&pool, &stale_session).await;
    store
        .replace_active_session(active_session(
            "session-1",
            "client-1",
            "terminal-1",
            100,
            1,
        ))
        .await
        .unwrap();

    let prepared = prepare_and_commit(
        store.clone(),
        clock_unhealthy,
        "message-clock-unhealthy",
        "attempt-clock-unhealthy",
        100,
    )
    .await;
    let session = store
        .get_session(&SessionId::from("session-1"))
        .await
        .unwrap()
        .unwrap();
    store
        .update_session_heartbeat(SessionHeartbeatUpdate {
            session_id: session.session_id,
            expected_revision: session.revision,
            heartbeat_at: 101,
            clock_sync_status: ClockSyncStatus::Unsynced,
            last_time_sync_at: None,
            updated_at: 101,
        })
        .await
        .unwrap();
    let outcome = store
        .claim_outbox(ClaimWireOutbox {
            message_id: prepared.outbox.message_id,
            expected_outbox_revision: prepared.outbox.revision,
            expected_attempt_revision: prepared.attempt.revision,
            fresh_after: 0,
            require_synced_clock: true,
            claimed_at: 102,
        })
        .await
        .unwrap();
    let OutboxClaimOutcome::ClockUnhealthy(cancelled) = outcome else {
        panic!("expected clock-unhealthy claim outcome");
    };
    assert_eq!(cancelled.outbox.status, WireOutboxStatus::Cancelled);
    assert_eq!(
        cancelled.attempt.status,
        CommandDeliveryAttemptStatus::NoActiveSession
    );

    let session = store
        .get_session(&SessionId::from("session-1"))
        .await
        .unwrap()
        .unwrap();
    store
        .update_session_heartbeat(SessionHeartbeatUpdate {
            session_id: session.session_id,
            expected_revision: session.revision,
            heartbeat_at: 103,
            clock_sync_status: ClockSyncStatus::Synced,
            last_time_sync_at: Some(103),
            updated_at: 103,
        })
        .await
        .unwrap();
    let prepared = prepare_and_commit(
        store.clone(),
        stale_session,
        "message-stale-session",
        "attempt-stale-session",
        103,
    )
    .await;
    let outcome = store
        .claim_outbox(ClaimWireOutbox {
            message_id: prepared.outbox.message_id,
            expected_outbox_revision: prepared.outbox.revision,
            expected_attempt_revision: prepared.attempt.revision,
            fresh_after: 104,
            require_synced_clock: true,
            claimed_at: 104,
        })
        .await
        .unwrap();
    let OutboxClaimOutcome::SessionUnavailable(cancelled) = outcome else {
        panic!("expected stale-session claim outcome");
    };
    assert_eq!(cancelled.outbox.status, WireOutboxStatus::Cancelled);
    assert_eq!(
        cancelled.attempt.status,
        CommandDeliveryAttemptStatus::NoActiveSession
    );
}

#[tokio::test]
async fn concurrent_reservations_are_monotonic_replay_safe_and_rollback_safe() {
    let (_database, store, pool) = test_store().await;
    let first = command("command-1", 10_000);
    let second = command("command-2", 10_000);
    let third = command("command-3", 10_000);
    seed_command(&pool, &first).await;
    seed_command(&pool, &second).await;
    seed_command(&pool, &third).await;
    store
        .replace_active_session(active_session(
            "session-1",
            "client-1",
            "terminal-1",
            100,
            8,
        ))
        .await
        .unwrap();

    let (left, right) = tokio::join!(
        prepare_and_commit(store.clone(), first, "message-1", "attempt-1", 100),
        prepare_and_commit(store.clone(), second, "message-2", "attempt-2", 100)
    );
    let mut sequences = [
        left.outbox.sequence.unwrap(),
        right.outbox.sequence.unwrap(),
    ];
    sequences.sort_unstable();
    assert_eq!(sequences, [2, 3]);

    let mut replay = store.begin_write().await.unwrap();
    assert!(replay
        .get_outbound_delivery(&MessageId::from("message-1"))
        .await
        .unwrap()
        .is_some());
    replay.rollback().await.unwrap();
    assert_eq!(
        store
            .get_session(&SessionId::from("session-1"))
            .await
            .unwrap()
            .unwrap()
            .last_outbound_sequence,
        3
    );

    let mut rolled_back = store.begin_write().await.unwrap();
    let transient = prepare_command(&mut rolled_back, &third, "message-3", "attempt-3", 100).await;
    assert_eq!(transient.outbox.sequence, Some(4));
    rolled_back.rollback().await.unwrap();
    assert!(store
        .get_outbound_delivery(&MessageId::from("message-3"))
        .await
        .unwrap()
        .is_none());
    let committed = prepare_and_commit(store, third, "message-3", "attempt-3", 100).await;
    assert_eq!(committed.outbox.sequence, Some(4));
}

#[tokio::test]
async fn inflight_limit_blocks_uncertain_attempts_and_releases_terminal_deliveries() {
    let (_database, store, pool) = test_store().await;
    let blocker = command("command-blocker", 10_000);
    let rejected = command("command-rejected", 10_000);
    let cancelled = command("command-cancelled", 108);
    let candidate = command("command-candidate", 10_000);
    for value in [&blocker, &rejected, &cancelled, &candidate] {
        seed_command(&pool, value).await;
    }
    store
        .replace_active_session(active_session(
            "session-1",
            "client-1",
            "terminal-1",
            100,
            1,
        ))
        .await
        .unwrap();

    let prepared = prepare_and_commit(
        store.clone(),
        blocker.clone(),
        "message-blocker",
        "attempt-blocker",
        100,
    )
    .await;
    for outcome in [
        probe_reservation(
            &store,
            DeliverySubject::ExecutionCommand(candidate.command_id.clone()),
            101,
        )
        .await,
        {
            let _claimed = claim(&store, &prepared, 101).await;
            probe_reservation(
                &store,
                DeliverySubject::ExecutionCommand(candidate.command_id.clone()),
                101,
            )
            .await
        },
    ] {
        assert!(matches!(
            outcome,
            SequenceReservation::InflightLimit {
                inflight: 1,
                limit: 1,
                ..
            }
        ));
    }
    let claimed = store
        .get_outbound_delivery(&prepared.outbox.message_id)
        .await
        .unwrap()
        .unwrap();
    let sent = store
        .finish_transport_write_sent(CompleteTransportWrite {
            message_id: claimed.outbox.message_id,
            expected_outbox_revision: claimed.outbox.revision,
            expected_attempt_revision: claimed.attempt.revision,
            completed_at: 102,
            error: None,
        })
        .await
        .unwrap();
    assert!(matches!(
        probe_reservation(
            &store,
            DeliverySubject::ExecutionCommand(candidate.command_id.clone()),
            102,
        )
        .await,
        SequenceReservation::InflightLimit {
            inflight: 1,
            limit: 1,
            ..
        }
    ));
    let timed_out = store
        .timeout_delivery_attempt(DeliveryAttemptTimeout {
            attempt_id: sent.attempt.attempt_id,
            expected_revision: sent.attempt.revision,
            timed_out_at: 103,
            error: "COMMAND_DELIVERY_TIMEOUT".to_owned(),
        })
        .await
        .unwrap();
    assert_eq!(timed_out.status, CommandDeliveryAttemptStatus::Unconfirmed);
    assert!(matches!(
        probe_reservation(
            &store,
            DeliverySubject::ExecutionCommand(candidate.command_id.clone()),
            103,
        )
        .await,
        SequenceReservation::InflightLimit {
            inflight: 1,
            limit: 1,
            ..
        }
    ));
    store
        .record_command_received_attempt(CommandReceivedAttemptUpdate {
            source_message_id: prepared.outbox.message_id,
            session_id: SessionId::from("session-1"),
            command_id: blocker.command_id,
            received_at: 104,
        })
        .await
        .unwrap();
    assert!(matches!(
        probe_reservation(
            &store,
            DeliverySubject::ExecutionCommand(candidate.command_id.clone()),
            104,
        )
        .await,
        SequenceReservation::Reserved(_)
    ));

    let prepared = prepare_and_commit(
        store.clone(),
        rejected,
        "message-rejected",
        "attempt-rejected",
        104,
    )
    .await;
    let claimed = claim(&store, &prepared, 105).await;
    let sent = store
        .finish_transport_write_sent(CompleteTransportWrite {
            message_id: claimed.outbox.message_id,
            expected_outbox_revision: claimed.outbox.revision,
            expected_attempt_revision: claimed.attempt.revision,
            completed_at: 106,
            error: None,
        })
        .await
        .unwrap();
    store
        .record_transport_ack(sinan_store::TransportAckUpdate {
            message_id: sent.outbox.message_id,
            session_id: SessionId::from("session-1"),
            message_type: "execution.command".to_owned(),
            status: TransportAckStatus::Rejected,
            reason: Some("SCHEMA_VALIDATION_FAILED".to_owned()),
            acked_at: 107,
        })
        .await
        .unwrap();
    assert!(matches!(
        probe_reservation(
            &store,
            DeliverySubject::ExecutionCommand(candidate.command_id.clone()),
            107,
        )
        .await,
        SequenceReservation::Reserved(_)
    ));

    let prepared = prepare_and_commit(
        store.clone(),
        cancelled,
        "message-cancelled",
        "attempt-cancelled",
        107,
    )
    .await;
    assert!(matches!(
        store
            .claim_outbox(ClaimWireOutbox {
                message_id: prepared.outbox.message_id,
                expected_outbox_revision: prepared.outbox.revision,
                expected_attempt_revision: prepared.attempt.revision,
                fresh_after: 0,
                require_synced_clock: true,
                claimed_at: 108,
            })
            .await
            .unwrap(),
        OutboxClaimOutcome::Expired(_)
    ));
    assert!(matches!(
        probe_reservation(
            &store,
            DeliverySubject::ExecutionCommand(candidate.command_id),
            108,
        )
        .await,
        SequenceReservation::Reserved(_)
    ));
}

#[tokio::test]
async fn ack_and_receipt_races_converge_without_regressing_stronger_evidence() {
    let (_database, store, pool) = test_store().await;
    let first = command("command-1", 10_000);
    let second = command("command-2", 10_000);
    let third = command("command-3", 10_000);
    let fourth = command("command-4", 10_000);
    let fifth = command("command-5", 10_000);
    let sixth = command("command-6", 10_000);
    for value in [&first, &second, &third, &fourth, &fifth, &sixth] {
        seed_command(&pool, value).await;
    }
    store
        .replace_active_session(active_session(
            "session-1",
            "client-1",
            "terminal-1",
            100,
            8,
        ))
        .await
        .unwrap();

    let prepared =
        prepare_and_commit(store.clone(), first.clone(), "message-1", "attempt-1", 100).await;
    let claimed = claim(&store, &prepared, 101).await;
    store
        .record_transport_ack(sinan_store::TransportAckUpdate {
            message_id: MessageId::from("message-1"),
            session_id: SessionId::from("session-1"),
            message_type: "execution.command".to_owned(),
            status: TransportAckStatus::Accepted,
            reason: None,
            acked_at: 102,
        })
        .await
        .unwrap();
    let completed = store
        .finish_transport_write_sent(CompleteTransportWrite {
            message_id: MessageId::from("message-1"),
            expected_outbox_revision: claimed.outbox.revision,
            expected_attempt_revision: claimed.attempt.revision,
            completed_at: 103,
            error: None,
        })
        .await
        .unwrap();
    assert_eq!(completed.outbox.status, WireOutboxStatus::Acked);
    assert_eq!(completed.attempt.status, CommandDeliveryAttemptStatus::Sent);

    let prepared =
        prepare_and_commit(store.clone(), second.clone(), "message-2", "attempt-2", 103).await;
    let claimed = claim(&store, &prepared, 104).await;
    store
        .record_command_received_attempt(CommandReceivedAttemptUpdate {
            source_message_id: MessageId::from("message-2"),
            session_id: SessionId::from("session-1"),
            command_id: second.command_id.clone(),
            received_at: 103,
        })
        .await
        .unwrap();
    let completed = store
        .finish_transport_write_failed(CompleteTransportWrite {
            message_id: MessageId::from("message-2"),
            expected_outbox_revision: claimed.outbox.revision,
            expected_attempt_revision: claimed.attempt.revision,
            completed_at: 105,
            error: Some("local write report raced receipt".to_owned()),
        })
        .await
        .unwrap();
    assert_eq!(completed.outbox.status, WireOutboxStatus::Sent);
    assert_eq!(
        completed.attempt.status,
        CommandDeliveryAttemptStatus::Acked
    );
    assert_eq!(completed.attempt.acked_at, Some(103));

    let prepared =
        prepare_and_commit(store.clone(), third.clone(), "message-3", "attempt-3", 105).await;
    let claimed = claim(&store, &prepared, 106).await;
    let sent = store
        .finish_transport_write_sent(CompleteTransportWrite {
            message_id: MessageId::from("message-3"),
            expected_outbox_revision: claimed.outbox.revision,
            expected_attempt_revision: claimed.attempt.revision,
            completed_at: 107,
            error: None,
        })
        .await
        .unwrap();
    store
        .timeout_delivery_attempt(DeliveryAttemptTimeout {
            attempt_id: sent.attempt.attempt_id.clone(),
            expected_revision: sent.attempt.revision,
            timed_out_at: 110,
            error: "COMMAND_DELIVERY_TIMEOUT".to_owned(),
        })
        .await
        .unwrap();
    let late = store
        .record_command_received_attempt(CommandReceivedAttemptUpdate {
            source_message_id: MessageId::from("message-3"),
            session_id: SessionId::from("session-1"),
            command_id: third.command_id,
            received_at: 108,
        })
        .await
        .unwrap();
    assert_eq!(late.status, CommandDeliveryAttemptStatus::Acked);
    assert_eq!(late.acked_at, Some(108));

    let prepared =
        prepare_and_commit(store.clone(), fourth.clone(), "message-4", "attempt-4", 110).await;
    let claimed = claim(&store, &prepared, 111).await;
    let rejected = store
        .record_transport_ack(sinan_store::TransportAckUpdate {
            message_id: MessageId::from("message-4"),
            session_id: SessionId::from("session-1"),
            message_type: "execution.command".to_owned(),
            status: TransportAckStatus::Rejected,
            reason: Some("SCHEMA_VALIDATION_FAILED".to_owned()),
            acked_at: 112,
        })
        .await
        .unwrap();
    assert_eq!(rejected.status, WireOutboxStatus::Failed);
    assert!(rejected
        .last_error
        .as_deref()
        .unwrap()
        .starts_with(TRANSPORT_ACK_REJECTED_PREFIX));
    let settled = store
        .finish_transport_write_sent(CompleteTransportWrite {
            message_id: MessageId::from("message-4"),
            expected_outbox_revision: claimed.outbox.revision,
            expected_attempt_revision: claimed.attempt.revision,
            completed_at: 113,
            error: None,
        })
        .await
        .unwrap();
    assert_eq!(settled.outbox.status, WireOutboxStatus::Failed);
    assert_eq!(settled.attempt.status, CommandDeliveryAttemptStatus::Sent);
    assert!(settled.attempt.error.is_none());

    let after_timeout = store
        .timeout_delivery_attempt(DeliveryAttemptTimeout {
            attempt_id: settled.attempt.attempt_id.clone(),
            expected_revision: settled.attempt.revision,
            timed_out_at: 114,
            error: "COMMAND_DELIVERY_TIMEOUT".to_owned(),
        })
        .await
        .unwrap();
    assert_eq!(after_timeout.status, CommandDeliveryAttemptStatus::Sent);
    let late_receipt = store
        .record_command_received_attempt(CommandReceivedAttemptUpdate {
            source_message_id: settled.outbox.message_id.clone(),
            session_id: SessionId::from("session-1"),
            command_id: fourth.command_id,
            received_at: 113,
        })
        .await
        .unwrap();
    assert_eq!(late_receipt.status, CommandDeliveryAttemptStatus::Acked);
    let after_receipt = store
        .get_outbound_delivery(&settled.outbox.message_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after_receipt.outbox.status, WireOutboxStatus::Failed);
    assert_eq!(after_receipt.outbox.last_error, settled.outbox.last_error);
    assert_eq!(
        after_receipt.attempt.status,
        CommandDeliveryAttemptStatus::Acked
    );

    let prepared = prepare_and_commit(store.clone(), fifth, "message-5", "attempt-5", 114).await;
    let claimed = claim(&store, &prepared, 115).await;
    let sent = store
        .finish_transport_write_sent(CompleteTransportWrite {
            message_id: claimed.outbox.message_id.clone(),
            expected_outbox_revision: claimed.outbox.revision,
            expected_attempt_revision: claimed.attempt.revision,
            completed_at: 116,
            error: None,
        })
        .await
        .unwrap();
    let rejected_after_finish = store
        .record_transport_ack(sinan_store::TransportAckUpdate {
            message_id: sent.outbox.message_id,
            session_id: SessionId::from("session-1"),
            message_type: "execution.command".to_owned(),
            status: TransportAckStatus::Rejected,
            reason: Some("SCHEMA_VALIDATION_FAILED".to_owned()),
            acked_at: 117,
        })
        .await
        .unwrap();
    assert_eq!(rejected_after_finish.status, WireOutboxStatus::Failed);
    let rejected_after_finish = store
        .get_outbound_delivery(&rejected_after_finish.message_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        rejected_after_finish.attempt.status,
        CommandDeliveryAttemptStatus::Sent
    );
    let after_timeout = store
        .timeout_delivery_attempt(DeliveryAttemptTimeout {
            attempt_id: rejected_after_finish.attempt.attempt_id,
            expected_revision: rejected_after_finish.attempt.revision,
            timed_out_at: 118,
            error: "COMMAND_DELIVERY_TIMEOUT".to_owned(),
        })
        .await
        .unwrap();
    assert_eq!(after_timeout.status, CommandDeliveryAttemptStatus::Sent);

    let prepared =
        prepare_and_commit(store.clone(), sixth.clone(), "message-6", "attempt-6", 118).await;
    let claimed = claim(&store, &prepared, 119).await;
    let sent = store
        .finish_transport_write_sent(CompleteTransportWrite {
            message_id: claimed.outbox.message_id,
            expected_outbox_revision: claimed.outbox.revision,
            expected_attempt_revision: claimed.attempt.revision,
            completed_at: 120,
            error: None,
        })
        .await
        .unwrap();
    store
        .record_command_received_attempt(CommandReceivedAttemptUpdate {
            source_message_id: sent.outbox.message_id.clone(),
            session_id: SessionId::from("session-1"),
            command_id: sixth.command_id,
            received_at: 121,
        })
        .await
        .unwrap();
    let rejected_after_receipt = store
        .record_transport_ack(sinan_store::TransportAckUpdate {
            message_id: sent.outbox.message_id,
            session_id: SessionId::from("session-1"),
            message_type: "execution.command".to_owned(),
            status: TransportAckStatus::Rejected,
            reason: Some("SCHEMA_VALIDATION_FAILED".to_owned()),
            acked_at: 122,
        })
        .await
        .unwrap();
    let rejected_after_receipt = store
        .get_outbound_delivery(&rejected_after_receipt.message_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        rejected_after_receipt.outbox.status,
        WireOutboxStatus::Failed
    );
    assert_eq!(
        rejected_after_receipt.attempt.status,
        CommandDeliveryAttemptStatus::Acked
    );
}

#[tokio::test]
async fn disconnect_and_startup_fences_cancel_unsent_and_preserve_uncertainty() {
    let (_database, store, pool) = test_store().await;
    let first = command("command-1", 10_000);
    let second = command("command-2", 10_000);
    let third = command("command-3", 10_000);
    let fourth = command("command-4", 10_000);
    let fifth = command("command-5", 10_000);
    for value in [&first, &second, &third, &fourth, &fifth] {
        seed_command(&pool, value).await;
    }
    store
        .replace_active_session(active_session(
            "session-1",
            "client-1",
            "terminal-1",
            100,
            8,
        ))
        .await
        .unwrap();
    let first_prepared =
        prepare_and_commit(store.clone(), first, "message-1", "attempt-1", 100).await;
    let first_claimed = claim(&store, &first_prepared, 101).await;
    let first_sent = store
        .finish_transport_write_sent(CompleteTransportWrite {
            message_id: MessageId::from("message-1"),
            expected_outbox_revision: first_claimed.outbox.revision,
            expected_attempt_revision: first_claimed.attempt.revision,
            completed_at: 102,
            error: None,
        })
        .await
        .unwrap();
    let second_prepared =
        prepare_and_commit(store.clone(), second, "message-2", "attempt-2", 102).await;
    let session = store
        .get_session(&SessionId::from("session-1"))
        .await
        .unwrap()
        .unwrap();
    let disconnected = store
        .disconnect_session(SessionStatusUpdate {
            session_id: session.session_id,
            expected_revision: session.revision,
            changed_at: 103,
            delivery_error: "CONNECTION_LOST".to_owned(),
        })
        .await
        .unwrap();
    assert_eq!(disconnected.session.status, SessionStatus::Disconnected);
    assert_eq!(disconnected.unconfirmed_attempts.len(), 1);
    assert_eq!(
        store
            .get_delivery_attempt(&first_sent.attempt.attempt_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        CommandDeliveryAttemptStatus::Unconfirmed
    );
    let cancelled = store
        .get_outbound_delivery(&second_prepared.outbox.message_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cancelled.outbox.status, WireOutboxStatus::Cancelled);
    assert_eq!(
        cancelled.attempt.status,
        CommandDeliveryAttemptStatus::Cancelled
    );

    store
        .replace_active_session(active_session(
            "session-2",
            "client-1",
            "terminal-1",
            104,
            8,
        ))
        .await
        .unwrap();
    let third_prepared =
        prepare_and_commit(store.clone(), third.clone(), "message-3", "attempt-3", 104).await;
    let third_claimed = claim(&store, &third_prepared, 105).await;
    let fourth_prepared =
        prepare_and_commit(store.clone(), fourth, "message-4", "attempt-4", 105).await;
    let fourth_claimed = claim(&store, &fourth_prepared, 105).await;
    store
        .record_transport_ack(sinan_store::TransportAckUpdate {
            message_id: fourth_claimed.outbox.message_id.clone(),
            session_id: SessionId::from("session-2"),
            message_type: "execution.command".to_owned(),
            status: TransportAckStatus::Accepted,
            reason: None,
            acked_at: 105,
        })
        .await
        .unwrap();
    let fifth_prepared =
        prepare_and_commit(store.clone(), fifth, "message-5", "attempt-5", 105).await;
    let fifth_claimed = claim(&store, &fifth_prepared, 105).await;
    store
        .record_transport_ack(sinan_store::TransportAckUpdate {
            message_id: fifth_claimed.outbox.message_id.clone(),
            session_id: SessionId::from("session-2"),
            message_type: "execution.command".to_owned(),
            status: TransportAckStatus::Rejected,
            reason: Some("SCHEMA_VALIDATION_FAILED".to_owned()),
            acked_at: 105,
        })
        .await
        .unwrap();
    let report = store
        .fence_interrupted_writes(106, "GATEWAY_RESTARTED")
        .await
        .unwrap();
    assert_eq!(report.sessions_staled, 1);
    assert_eq!(report.outboxes_fenced, 1);
    assert_eq!(report.attempts_unconfirmed, 2);
    assert_eq!(report.attempts_rejected, 1);
    assert_eq!(
        store
            .get_delivery_attempt(&fourth_claimed.attempt.attempt_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        CommandDeliveryAttemptStatus::Unconfirmed
    );
    let rejected = store
        .get_outbound_delivery(&fifth_claimed.outbox.message_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(rejected.outbox.status, WireOutboxStatus::Failed);
    assert_eq!(rejected.attempt.status, CommandDeliveryAttemptStatus::Sent);
    let after_fence = store
        .finish_transport_write_sent(CompleteTransportWrite {
            message_id: third_claimed.outbox.message_id.clone(),
            expected_outbox_revision: third_claimed.outbox.revision,
            expected_attempt_revision: third_claimed.attempt.revision,
            completed_at: 107,
            error: None,
        })
        .await
        .unwrap();
    assert_eq!(
        after_fence.attempt.status,
        CommandDeliveryAttemptStatus::Unconfirmed
    );
    let late = store
        .record_command_received_attempt(CommandReceivedAttemptUpdate {
            source_message_id: third_claimed.outbox.message_id,
            session_id: SessionId::from("session-2"),
            command_id: third.command_id,
            received_at: 105,
        })
        .await
        .unwrap();
    assert_eq!(late.status, CommandDeliveryAttemptStatus::Acked);
}

#[tokio::test]
async fn claim_revalidates_expiry_and_bundle_rejects_parent_payload_drift_atomically() {
    let (_database, store, pool) = test_store().await;
    let expiring = command("command-expiring", 105);
    let drifting = command("command-drifting", 10_000);
    seed_command(&pool, &expiring).await;
    seed_command(&pool, &drifting).await;
    store
        .replace_active_session(active_session(
            "session-1",
            "client-1",
            "terminal-1",
            100,
            1,
        ))
        .await
        .unwrap();

    let prepared = prepare_and_commit(
        store.clone(),
        expiring,
        "message-expiring",
        "attempt-expiring",
        100,
    )
    .await;
    let expired = store
        .claim_outbox(ClaimWireOutbox {
            message_id: prepared.outbox.message_id,
            expected_outbox_revision: prepared.outbox.revision,
            expected_attempt_revision: prepared.attempt.revision,
            fresh_after: 0,
            require_synced_clock: true,
            claimed_at: 105,
        })
        .await
        .unwrap();
    let OutboxClaimOutcome::Expired(expired) = expired else {
        panic!("expected expiry outcome");
    };
    assert_eq!(expired.outbox.status, WireOutboxStatus::Cancelled);
    assert_eq!(
        expired.attempt.status,
        CommandDeliveryAttemptStatus::Cancelled
    );

    let sequence_before_drift = store
        .get_session(&SessionId::from("session-1"))
        .await
        .unwrap()
        .unwrap()
        .last_outbound_sequence;
    let mut transaction = store.begin_write().await.unwrap();
    let session = match transaction
        .resolve_session_route(route(true))
        .await
        .unwrap()
    {
        SessionRouteResolution::Ready(session) => session,
        other => panic!("expected ready route, got {other:?}"),
    };
    let reservation = match transaction
        .reserve_outbound_sequence(ReserveOutboundSequence {
            session_id: session.session_id,
            expected_revision: session.revision,
            subject: DeliverySubject::ExecutionCommand(drifting.command_id.clone()),
            fresh_after: 0,
            reserved_at: 106,
        })
        .await
        .unwrap()
    {
        SequenceReservation::Reserved(reservation) => reservation,
        other => panic!("expected reservation, got {other:?}"),
    };
    let mut changed = drifting;
    changed.lots = Some(0.2);
    let wire = WireMessage {
        message_id: MessageId::from("message-drift"),
        message_type: ExecutionClientMessageType::ExecutionCommand,
        schema_version: "ecp.v1.0".to_owned(),
        client_id: Some(reservation.client_id.clone()),
        session_id: Some(reservation.session_id.clone()),
        correlation_id: None,
        causation_id: None,
        sent_at: Some(106),
        sequence: Some(reservation.sequence),
        payload: changed,
    };
    assert!(matches!(
        transaction
            .enqueue_reserved_delivery(NewReservedDelivery {
                reservation,
                attempt_id: "attempt-drift".to_owned(),
                message_id: MessageId::from("message-drift"),
                message_type: "execution.command".to_owned(),
                envelope: CanonicalJson::from_serializable(&wire).unwrap(),
                created_at: 106,
            })
            .await,
        Err(StoreError::IdentityConflict { .. })
    ));
    transaction.rollback().await.unwrap();
    assert!(store
        .get_outbound_delivery(&MessageId::from("message-drift"))
        .await
        .unwrap()
        .is_none());
    assert!(store
        .get_delivery_attempt("attempt-drift")
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        store
            .get_session(&SessionId::from("session-1"))
            .await
            .unwrap()
            .unwrap()
            .last_outbound_sequence,
        sequence_before_drift
    );

    let rejection = NewDeliveryAttempt {
        attempt_id: "attempt-route-rejection".to_owned(),
        subject: DeliverySubject::ExecutionCommand(CommandId::from("command-drifting")),
        session_id: Some(SessionId::from("session-1")),
        message_id: None,
        request_payload: Some(
            CanonicalJson::from_value(serde_json::json!({
                "message_id": "message-route-rejection"
            }))
            .unwrap(),
        ),
        status: DeliveryRejectionKind::InflightLimit.attempt_status(),
        attempted_at: 106,
        acked_at: None,
        error: Some("COMMAND_INFLIGHT_LIMIT_REACHED".to_owned()),
        updated_at: 106,
    };
    store.record_delivery_attempt(rejection).await.unwrap();
    let mut transaction = store.begin_write().await.unwrap();
    let replay = transaction
        .get_delivery_attempt("attempt-route-rejection")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(replay.session_id, Some(SessionId::from("session-1")));
    assert!(replay.message_id.is_none());
    transaction.rollback().await.unwrap();
}

#[tokio::test]
async fn reconciliation_delivery_allows_unsynced_session_and_matches_durable_request() {
    let (_database, store, pool) = test_store().await;
    let request = ReconciliationRequest {
        request_id: RequestId::from("request-1"),
        account_id: AccountId::from("account-1"),
        terminal_id: Some(TerminalId::from("terminal-1")),
        client_id: Some(ClientId::from("client-1")),
        reason: ReconciliationReason::ManualRequest,
        command_ids: None,
        since_server_time: None,
    };
    let request_payload = CanonicalJson::from_serializable(&request).unwrap();
    sqlx::query(
        "INSERT INTO core_events (\
             event_id, event_type, aggregate_type, aggregate_id, schema_version, event_at, \
             received_at, created_at, source, payload_json, payload_hash\
         ) VALUES ('request-event-1', 'reconciliation.request', 'reconciliation', 'request-1', \
                   'ecp.v1.0', 90, 90, 90, 'test', ?, ?)",
    )
    .bind(request_payload.as_str())
    .bind(request_payload.sha256_hex())
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO reconciliation_runs (\
             request_id, request_event_id, account_id, terminal_id, client_id, reason, scope, \
             requested_at, status, request_payload_json, request_payload_hash, created_at, updated_at\
         ) VALUES ('request-1', 'request-event-1', 'account-1', 'terminal-1', 'client-1', \
                   'MANUAL_REQUEST', 'ACCOUNT', 90, 'REQUESTED', ?, ?, 90, 90)",
    )
    .bind(request_payload.as_str())
    .bind(request_payload.sha256_hex())
    .execute(&pool)
    .await
    .unwrap();
    let mut session = active_session("session-1", "client-1", "terminal-1", 100, 1);
    session.clock_sync_status = Some(ClockSyncStatus::Unsynced);
    session.last_time_sync_at = None;
    store.replace_active_session(session).await.unwrap();

    let mut transaction = store.begin_write().await.unwrap();
    let session = match transaction
        .resolve_session_route(route(false))
        .await
        .unwrap()
    {
        SessionRouteResolution::Ready(session) => session,
        other => panic!("expected reconciliation route, got {other:?}"),
    };
    let reservation = match transaction
        .reserve_outbound_sequence(ReserveOutboundSequence {
            session_id: session.session_id,
            expected_revision: session.revision,
            subject: DeliverySubject::ReconciliationRequest(request.request_id.clone()),
            fresh_after: 0,
            reserved_at: 100,
        })
        .await
        .unwrap()
    {
        SequenceReservation::Reserved(reservation) => reservation,
        other => panic!("expected reservation, got {other:?}"),
    };
    let wire = WireMessage {
        message_id: MessageId::from("message-request-1"),
        message_type: ExecutionClientMessageType::ReconciliationRequest,
        schema_version: "ecp.v1.0".to_owned(),
        client_id: Some(reservation.client_id.clone()),
        session_id: Some(reservation.session_id.clone()),
        correlation_id: None,
        causation_id: None,
        sent_at: Some(100),
        sequence: Some(reservation.sequence),
        payload: request,
    };
    let prepared = transaction
        .enqueue_reserved_delivery(NewReservedDelivery {
            reservation,
            attempt_id: "attempt-request-1".to_owned(),
            message_id: MessageId::from("message-request-1"),
            message_type: "reconciliation.request".to_owned(),
            envelope: CanonicalJson::from_serializable(&wire).unwrap(),
            created_at: 100,
        })
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    assert!(matches!(
        store
            .claim_outbox(ClaimWireOutbox {
                message_id: prepared.outbox.message_id,
                expected_outbox_revision: prepared.outbox.revision,
                expected_attempt_revision: prepared.attempt.revision,
                fresh_after: 0,
                require_synced_clock: false,
                claimed_at: 101,
            })
            .await
            .unwrap(),
        OutboxClaimOutcome::Claimed(_)
    ));
}
