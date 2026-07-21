mod common;

use serde_json::json;
use sinan_store::{
    CanonicalJson, ClaimDurableWork, CompleteInboundAdmission, CompleteSessionResumeAdmission,
    DeadletterReason, DurableInboundAdmissionOutcome, DurableSessionResumeAdmissionOutcome,
    DurableWorkStatus, FailInboundAdmission, FailSessionResumeAdmission, NewDeadletterEvent,
    NewInboundAdmission, NewSessionResumeAdmission, NewSystemEvent, ReclaimDurableWork, StoreError,
    SystemEventSeverity, WriteOutcome,
};
use sinan_types::{
    AccountId, CausationId, ClientId, CorrelationId, ErrorCode, MessageId, RequestId, SessionId,
};

use common::test_store;

async fn seed_session(pool: &sqlx::SqlitePool) {
    sqlx::query(
        "INSERT INTO execution_client_sessions (\
            session_id, client_id, account_id, platform, status, capabilities_json, connected_at, \
            last_heartbeat_at, last_time_sync_at, clock_sync_status, updated_at\
         ) VALUES ('session-1', 'client-1', 'account-1', 'MT5', 'ACTIVE', '[]', 10, 10, 10, \
                   'SYNCED', 10)",
    )
    .execute(pool)
    .await
    .expect("session fixture should insert");
}

fn inbound(message_id: &str, sequence: u64, payload_value: i64) -> NewInboundAdmission {
    let envelope = CanonicalJson::from_value(json!({
        "message_id": message_id,
        "type": "market.tick",
        "schema_version": "ecp.v1.0",
        "client_id": "client-1",
        "session_id": "session-1",
        "correlation_id": "correlation-1",
        "causation_id": "causation-1",
        "sent_at": 90,
        "sequence": sequence,
        "payload": {"value": payload_value}
    }))
    .expect("envelope should canonicalize");
    NewInboundAdmission {
        message_id: MessageId::from(message_id),
        session_id: SessionId::from("session-1"),
        client_id: ClientId::from("client-1"),
        account_id: AccountId::from("account-1"),
        terminal_id: None,
        message_type: "market.tick".to_owned(),
        schema_version: "ecp.v1.0".to_owned(),
        sequence,
        correlation_id: Some(CorrelationId::from("correlation-1")),
        causation_id: Some(CausationId::from("causation-1")),
        envelope,
        received_at: 100,
        created_at: 100,
    }
}

fn resume(cursor_value: &str) -> NewSessionResumeAdmission {
    NewSessionResumeAdmission {
        hello_message_id: MessageId::from("hello-1"),
        session_id: SessionId::from("new-session-1"),
        client_id: ClientId::from("client-1"),
        account_id: AccountId::from("account-1"),
        terminal_id: None,
        cursor: CanonicalJson::from_value(json!({
            "previous_session_id": cursor_value,
            "pending_command_ids": ["command-1"]
        }))
        .unwrap(),
        received_at: 100,
        created_at: 100,
    }
}

#[tokio::test]
async fn inbound_admission_is_strictly_idempotent_and_journals_drift() {
    let (_database, store, pool) = test_store().await;
    seed_session(&pool).await;

    let original = inbound("message-1", 1, 10);
    let accepted = store.admit_inbound(original.clone()).await.unwrap();
    assert!(matches!(
        accepted,
        DurableInboundAdmissionOutcome::Accepted(ref record)
            if record.status == DurableWorkStatus::Pending
    ));

    let mut retransmission = original.clone();
    retransmission.received_at = 101;
    retransmission.created_at = 101;
    let duplicate = store.admit_inbound(retransmission).await.unwrap();
    assert!(matches!(
        duplicate,
        DurableInboundAdmissionOutcome::Duplicate(ref record)
            if record.received_at == 100 && record.envelope == original.envelope
    ));

    let payload_drift = inbound("message-1", 1, 11);
    let rejected = store.admit_inbound(payload_drift).await.unwrap();
    let rejection_id = match rejected {
        DurableInboundAdmissionOutcome::Rejected(rejection) => {
            assert_eq!(rejection.reason, ErrorCode::DuplicateIdempotencyConflict);
            rejection.rejection_id
        }
        other => panic!("expected rejection, got {other:?}"),
    };
    assert!(store
        .get_inbound_rejection(&rejection_id)
        .await
        .unwrap()
        .is_some());

    let sequence_drift = inbound("message-2", 1, 12);
    assert!(matches!(
        store.admit_inbound(sequence_drift).await.unwrap(),
        DurableInboundAdmissionOutcome::Rejected(_)
    ));

    let admission_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM inbound_admissions")
        .fetch_one(&pool)
        .await
        .unwrap();
    let rejection_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM inbound_rejections")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(admission_count, 1);
    assert_eq!(rejection_count, 2);
}

#[tokio::test]
async fn inbound_lease_is_reclaimable_after_crash_and_terminal_rows_are_immutable() {
    let (_database, store, pool) = test_store().await;
    seed_session(&pool).await;
    store
        .admit_inbound(inbound("message-1", 1, 10))
        .await
        .unwrap();

    let first = store
        .claim_next_inbound(ClaimDurableWork {
            worker_id: "worker-1".to_owned(),
            claimed_at: 110,
            lease_expires_at: 120,
        })
        .await
        .unwrap()
        .expect("pending record should claim");
    assert_eq!(first.status, DurableWorkStatus::Processing);
    assert_eq!(first.revision, 1);
    assert!(store
        .reclaim_expired_inbound(ReclaimDurableWork {
            worker_id: "worker-2".to_owned(),
            reclaimed_at: 119,
            lease_expires_at: 130,
        })
        .await
        .unwrap()
        .is_none());

    let reclaimed = store
        .reclaim_expired_inbound(ReclaimDurableWork {
            worker_id: "worker-2".to_owned(),
            reclaimed_at: 120,
            lease_expires_at: 140,
        })
        .await
        .unwrap()
        .expect("expired processing record should reclaim");
    assert_eq!(reclaimed.revision, 2);
    assert_eq!(reclaimed.lease_owner.as_deref(), Some("worker-2"));

    let stale = store
        .complete_inbound(CompleteInboundAdmission {
            message_id: first.message_id,
            expected_revision: first.revision,
            worker_id: "worker-1".to_owned(),
            completed_at: 121,
        })
        .await
        .expect_err("crashed worker must lose its lease");
    assert!(matches!(stale, StoreError::StaleWrite { .. }));

    let handled = store
        .complete_inbound(CompleteInboundAdmission {
            message_id: reclaimed.message_id,
            expected_revision: reclaimed.revision,
            worker_id: "worker-2".to_owned(),
            completed_at: 121,
        })
        .await
        .unwrap();
    assert_eq!(handled.status, DurableWorkStatus::Handled);
    assert_eq!(handled.revision, 3);

    let identity_update = sqlx::query(
        "UPDATE inbound_admissions SET message_type = 'market.bar' WHERE message_id = 'message-1'",
    )
    .execute(&pool)
    .await;
    assert!(identity_update.is_err());
    assert!(
        sqlx::query("DELETE FROM inbound_admissions WHERE message_id = 'message-1'")
            .execute(&pool)
            .await
            .is_err()
    );

    store
        .admit_inbound(inbound("message-2", 2, 20))
        .await
        .unwrap();
    let claimed = store
        .claim_next_inbound(ClaimDurableWork {
            worker_id: "worker-2".to_owned(),
            claimed_at: 122,
            lease_expires_at: 150,
        })
        .await
        .unwrap()
        .unwrap();
    let failed = store
        .fail_inbound(FailInboundAdmission {
            message_id: claimed.message_id,
            expected_revision: claimed.revision,
            worker_id: "worker-2".to_owned(),
            failed_at: 123,
            error: "typed handler rejected payload".to_owned(),
        })
        .await
        .unwrap();
    assert_eq!(failed.status, DurableWorkStatus::Failed);
    assert!(store
        .claim_next_inbound(ClaimDurableWork {
            worker_id: "worker-3".to_owned(),
            claimed_at: 124,
            lease_expires_at: 160,
        })
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn resume_cursor_is_strictly_idempotent_and_has_its_own_recovery_lease() {
    let (_database, store, pool) = test_store().await;

    let original = resume("old-session-1");
    assert!(matches!(
        store.admit_session_resume(original.clone()).await.unwrap(),
        DurableSessionResumeAdmissionOutcome::Accepted(_)
    ));
    let mut retransmission = original;
    retransmission.received_at = 101;
    retransmission.created_at = 101;
    assert!(matches!(
        store.admit_session_resume(retransmission).await.unwrap(),
        DurableSessionResumeAdmissionOutcome::Duplicate(_)
    ));
    assert!(matches!(
        store
            .admit_session_resume(resume("old-session-drift"))
            .await,
        Err(StoreError::IdentityConflict { .. })
    ));

    let first = store
        .claim_next_session_resume(ClaimDurableWork {
            worker_id: "resume-worker-1".to_owned(),
            claimed_at: 110,
            lease_expires_at: 120,
        })
        .await
        .unwrap()
        .unwrap();
    let reclaimed = store
        .reclaim_expired_session_resume(ReclaimDurableWork {
            worker_id: "resume-worker-2".to_owned(),
            reclaimed_at: 120,
            lease_expires_at: 140,
        })
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reclaimed.revision, first.revision + 1);
    let handled = store
        .complete_session_resume(CompleteSessionResumeAdmission {
            hello_message_id: reclaimed.hello_message_id,
            expected_revision: reclaimed.revision,
            worker_id: "resume-worker-2".to_owned(),
            completed_at: 121,
            reconciliation_request_id: Some(RequestId::from("reconciliation-1")),
        })
        .await
        .unwrap();
    assert_eq!(handled.status, DurableWorkStatus::Handled);
    assert_eq!(
        handled.reconciliation_request_id,
        Some(RequestId::from("reconciliation-1"))
    );
    assert!(sqlx::query(
        "UPDATE session_resume_admissions SET cursor_json = '{}' WHERE hello_message_id = 'hello-1'",
    )
    .execute(&pool)
    .await
    .is_err());
}

#[tokio::test]
async fn terminal_writes_are_fenced_at_the_lease_expiry_boundary() {
    let (_database, store, pool) = test_store().await;
    seed_session(&pool).await;
    store
        .admit_inbound(inbound("message-1", 1, 10))
        .await
        .unwrap();
    let inbound_claim = store
        .claim_next_inbound(ClaimDurableWork {
            worker_id: "inbound-worker-1".to_owned(),
            claimed_at: 110,
            lease_expires_at: 120,
        })
        .await
        .unwrap()
        .unwrap();

    let error = store
        .complete_inbound(CompleteInboundAdmission {
            message_id: inbound_claim.message_id.clone(),
            expected_revision: inbound_claim.revision,
            worker_id: "inbound-worker-1".to_owned(),
            completed_at: 120,
        })
        .await
        .expect_err("completion at the expiry boundary must lose the lease");
    assert!(matches!(error, StoreError::StaleWrite { .. }));

    let inbound_reclaimed = store
        .reclaim_expired_inbound(ReclaimDurableWork {
            worker_id: "inbound-worker-2".to_owned(),
            reclaimed_at: 120,
            lease_expires_at: 140,
        })
        .await
        .unwrap()
        .unwrap();
    let error = store
        .fail_inbound(FailInboundAdmission {
            message_id: inbound_reclaimed.message_id,
            expected_revision: inbound_reclaimed.revision,
            worker_id: "inbound-worker-2".to_owned(),
            failed_at: 140,
            error: "late handler failure".to_owned(),
        })
        .await
        .expect_err("failure at the expiry boundary must lose the lease");
    assert!(matches!(error, StoreError::StaleWrite { .. }));

    store
        .admit_session_resume(resume("old-session-1"))
        .await
        .unwrap();
    let resume_claim = store
        .claim_next_session_resume(ClaimDurableWork {
            worker_id: "resume-worker-1".to_owned(),
            claimed_at: 110,
            lease_expires_at: 120,
        })
        .await
        .unwrap()
        .unwrap();
    let error = store
        .fail_session_resume(FailSessionResumeAdmission {
            hello_message_id: resume_claim.hello_message_id.clone(),
            expected_revision: resume_claim.revision,
            worker_id: "resume-worker-1".to_owned(),
            failed_at: 120,
            error: "late resume failure".to_owned(),
        })
        .await
        .expect_err("resume failure at the expiry boundary must lose the lease");
    assert!(matches!(error, StoreError::StaleWrite { .. }));

    let resume_reclaimed = store
        .reclaim_expired_session_resume(ReclaimDurableWork {
            worker_id: "resume-worker-2".to_owned(),
            reclaimed_at: 120,
            lease_expires_at: 140,
        })
        .await
        .unwrap()
        .unwrap();
    let error = store
        .complete_session_resume(CompleteSessionResumeAdmission {
            hello_message_id: resume_reclaimed.hello_message_id,
            expected_revision: resume_reclaimed.revision,
            worker_id: "resume-worker-2".to_owned(),
            completed_at: 140,
            reconciliation_request_id: None,
        })
        .await
        .expect_err("resume completion at the expiry boundary must lose the lease");
    assert!(matches!(error, StoreError::StaleWrite { .. }));
}

#[tokio::test]
async fn typed_system_and_deadletter_events_are_idempotent_append_only_facts() {
    let (_database, store, pool) = test_store().await;
    let system = NewSystemEvent {
        system_event_id: "system-1".to_owned(),
        event_type: "TIME_SYNC_UNHEALTHY".to_owned(),
        severity: SystemEventSeverity::Warning,
        component: "trading-gateway".to_owned(),
        message: "clock evidence is stale".to_owned(),
        metadata: Some(CanonicalJson::from_value(json!({"session_id": "session-1"})).unwrap()),
        timestamp: 100,
        created_at: 100,
    };
    assert!(matches!(
        store.append_system_event(system.clone()).await.unwrap(),
        WriteOutcome::Inserted(_)
    ));
    assert!(matches!(
        store.append_system_event(system.clone()).await.unwrap(),
        WriteOutcome::Duplicate(_)
    ));
    let mut system_drift = system;
    system_drift.message = "different".to_owned();
    assert!(matches!(
        store.append_system_event(system_drift).await,
        Err(StoreError::IdentityConflict { .. })
    ));

    let deadletter = NewDeadletterEvent {
        deadletter_id: "deadletter-1".to_owned(),
        message_id: Some(MessageId::from("message-1")),
        message_type: Some("market.tick".to_owned()),
        schema_version: Some("ecp.v1.0".to_owned()),
        reason: DeadletterReason::DecodeFailed,
        source: "trading-gateway.native-tcp".to_owned(),
        raw_payload: Some("{bad".to_owned()),
        raw_payload_length: Some(64),
        error_message: "invalid JSON".to_owned(),
        received_at: 100,
        created_at: 100,
    };
    assert!(matches!(
        store
            .append_deadletter_event(deadletter.clone())
            .await
            .unwrap(),
        WriteOutcome::Inserted(_)
    ));
    assert!(matches!(
        store.append_deadletter_event(deadletter).await.unwrap(),
        WriteOutcome::Duplicate(_)
    ));
    assert!(store
        .get_deadletter_event("deadletter-1")
        .await
        .unwrap()
        .is_some());

    assert!(sqlx::query(
        "UPDATE deadletter_events SET reason = 'UNKNOWN_TYPE' WHERE deadletter_id = 'deadletter-1'",
    )
    .execute(&pool)
    .await
    .is_err());
    assert!(
        sqlx::query("DELETE FROM system_events WHERE system_event_id = 'system-1'")
            .execute(&pool)
            .await
            .is_err()
    );
}
