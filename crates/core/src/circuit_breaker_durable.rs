use sinan_risk::{
    fail_closed_circuit_breaker_restore_after_epoch, restore_circuit_breaker_snapshot,
    CircuitBreakerError, CircuitBreakerOutcome, CircuitBreakerRecoveryEpochExhausted,
    CircuitBreakerRestoreFailure, CircuitBreakerStatus,
};
use sinan_store::{
    CanonicalJson, CircuitBreakerHeadMetadata, NewCircuitBreakerSnapshot, SqliteStateStore,
    StoreError,
};
use thiserror::Error;

pub const DEFAULT_CIRCUIT_BREAKER_CAS_RELOADS: usize = 3;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurableCircuitBreakerRestoreDisposition {
    Restored,
    FailClosedPersisted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableCircuitBreakerRestore {
    pub outcome: CircuitBreakerOutcome,
    pub state_revision: u64,
    pub disposition: DurableCircuitBreakerRestoreDisposition,
    pub cas_reloads: usize,
}

#[derive(Debug, Error)]
pub enum DurableCircuitBreakerRestoreError {
    #[error("state store failed while restoring the circuit breaker")]
    StoreRead {
        outcome: CircuitBreakerOutcome,
        #[source]
        source: StoreError,
    },

    #[error("circuit-breaker recovery epoch is exhausted")]
    RecoveryEpochExhausted {
        #[source]
        source: CircuitBreakerRecoveryEpochExhausted,
    },

    #[error("failed to encode the fail-closed circuit-breaker snapshot")]
    SnapshotEncoding {
        outcome: CircuitBreakerOutcome,
        #[source]
        source: StoreError,
    },

    #[error("state store failed while persisting the fail-closed circuit breaker")]
    StoreWrite {
        outcome: CircuitBreakerOutcome,
        #[source]
        source: StoreError,
    },

    #[error("circuit-breaker snapshot CAS remained stale after {cas_reloads} reloads")]
    CasReloadsExhausted {
        outcome: CircuitBreakerOutcome,
        cas_reloads: usize,
        #[source]
        source: StoreError,
    },
}

impl DurableCircuitBreakerRestoreError {
    pub fn outcome(&self) -> &CircuitBreakerOutcome {
        match self {
            Self::StoreRead { outcome, .. }
            | Self::SnapshotEncoding { outcome, .. }
            | Self::StoreWrite { outcome, .. }
            | Self::CasReloadsExhausted { outcome, .. } => outcome,
            Self::RecoveryEpochExhausted { source } => source.outcome(),
        }
    }

    pub fn into_outcome(self) -> CircuitBreakerOutcome {
        match self {
            Self::StoreRead { outcome, .. }
            | Self::SnapshotEncoding { outcome, .. }
            | Self::StoreWrite { outcome, .. }
            | Self::CasReloadsExhausted { outcome, .. } => outcome,
            Self::RecoveryEpochExhausted { source } => source.into_outcome(),
        }
    }
}

/// Restores the complete durable breaker state or persists a new OPEN safety
/// incident before returning it to the caller.
pub async fn restore_durable_circuit_breaker(
    store: &SqliteStateStore,
    server_now_ms: i64,
) -> Result<DurableCircuitBreakerRestore, DurableCircuitBreakerRestoreError> {
    restore_durable_circuit_breaker_with_cas_reloads(
        store,
        server_now_ms,
        DEFAULT_CIRCUIT_BREAKER_CAS_RELOADS,
    )
    .await
}

/// Variant with an explicit bound for deterministic startup and concurrency
/// tests. `max_cas_reloads` excludes the initial write attempt.
pub async fn restore_durable_circuit_breaker_with_cas_reloads(
    store: &SqliteStateStore,
    server_now_ms: i64,
    max_cas_reloads: usize,
) -> Result<DurableCircuitBreakerRestore, DurableCircuitBreakerRestoreError> {
    let mut cas_reloads = 0;

    loop {
        let head = match store.get_circuit_breaker_head_metadata().await {
            Ok(head) => head,
            Err(source) => {
                let outcome = fail_closed_after_epoch(
                    CircuitBreakerRestoreFailure::StoreUnavailable,
                    server_now_ms,
                    0,
                )?;
                return Err(DurableCircuitBreakerRestoreError::StoreRead { outcome, source });
            }
        };

        let latest = match store.get_latest_circuit_breaker_snapshot().await {
            Ok(latest) => latest,
            Err(StoreError::CorruptData { .. }) => None,
            Err(source) => {
                let outcome = fail_closed_after_epoch(
                    CircuitBreakerRestoreFailure::StoreUnavailable,
                    server_now_ms,
                    head.as_ref().map_or(0, |head| head.recovery_epoch),
                )?;
                return Err(DurableCircuitBreakerRestoreError::StoreRead { outcome, source });
            }
        };

        let (effective_head, restore_failure) = match latest {
            Some(snapshot) => {
                let snapshot_head = CircuitBreakerHeadMetadata {
                    state_revision: snapshot.state_revision,
                    recovery_epoch: snapshot.recovery_epoch,
                };
                if head
                    .as_ref()
                    .is_some_and(|head| head.state_revision > snapshot.state_revision)
                {
                    (head, CircuitBreakerRestoreFailure::CorruptPayload)
                } else {
                    let outcome = restore_circuit_breaker_snapshot(
                        Some(snapshot.payload.as_str()),
                        server_now_ms,
                    );
                    match durable_restore_failure(&outcome) {
                        Some(failure) => (Some(snapshot_head), failure),
                        None => {
                            return Ok(DurableCircuitBreakerRestore {
                                outcome,
                                state_revision: snapshot.state_revision,
                                disposition: DurableCircuitBreakerRestoreDisposition::Restored,
                                cas_reloads,
                            });
                        }
                    }
                }
            }
            None if head.is_none() => (None, CircuitBreakerRestoreFailure::MissingSnapshot),
            None => (head, CircuitBreakerRestoreFailure::CorruptPayload),
        };

        let outcome = fail_closed_after_epoch(
            restore_failure,
            server_now_ms,
            effective_head
                .as_ref()
                .map_or(0, |head| head.recovery_epoch),
        )?;
        let new_snapshot = encode_snapshot(
            &outcome,
            effective_head.as_ref().map(|head| head.state_revision),
            server_now_ms,
        )?;

        match store.write_circuit_breaker_snapshot(new_snapshot).await {
            Ok(write) => {
                return Ok(DurableCircuitBreakerRestore {
                    outcome,
                    state_revision: write.record().state_revision,
                    disposition: DurableCircuitBreakerRestoreDisposition::FailClosedPersisted,
                    cas_reloads,
                });
            }
            Err(StoreError::StaleWrite { .. }) if cas_reloads < max_cas_reloads => {
                cas_reloads += 1;
            }
            Err(source @ StoreError::StaleWrite { .. }) => {
                return Err(DurableCircuitBreakerRestoreError::CasReloadsExhausted {
                    outcome,
                    cas_reloads,
                    source,
                });
            }
            Err(source) => {
                return Err(DurableCircuitBreakerRestoreError::StoreWrite { outcome, source });
            }
        }
    }
}

fn durable_restore_failure(
    outcome: &CircuitBreakerOutcome,
) -> Option<CircuitBreakerRestoreFailure> {
    match outcome.error.as_ref() {
        Some(CircuitBreakerError::DurableRestoreFailed { failure }) => Some(failure.clone()),
        _ => None,
    }
}

fn fail_closed_after_epoch(
    failure: CircuitBreakerRestoreFailure,
    server_now_ms: i64,
    last_recovery_epoch: u64,
) -> Result<CircuitBreakerOutcome, DurableCircuitBreakerRestoreError> {
    fail_closed_circuit_breaker_restore_after_epoch(failure, server_now_ms, last_recovery_epoch)
        .map_err(|source| DurableCircuitBreakerRestoreError::RecoveryEpochExhausted { source })
}

fn encode_snapshot(
    outcome: &CircuitBreakerOutcome,
    expected_head_revision: Option<u64>,
    server_now_ms: i64,
) -> Result<NewCircuitBreakerSnapshot, DurableCircuitBreakerRestoreError> {
    let snapshot = outcome.state.durable_snapshot_v1();
    let json = snapshot
        .to_json()
        .map_err(StoreError::from)
        .and_then(|json| CanonicalJson::parse(&json));
    let payload = json.map_err(
        |source| DurableCircuitBreakerRestoreError::SnapshotEncoding {
            outcome: outcome.clone(),
            source,
        },
    )?;

    Ok(NewCircuitBreakerSnapshot {
        expected_head_revision,
        schema_version: snapshot.schema_version().to_owned(),
        status: status_name(snapshot.status()).to_owned(),
        recovery_epoch: snapshot.recovery_epoch(),
        updated_at: server_now_ms.max(0),
        payload,
    })
}

const fn status_name(status: CircuitBreakerStatus) -> &'static str {
    match status {
        CircuitBreakerStatus::Closed => "CLOSED",
        CircuitBreakerStatus::Open => "OPEN",
        CircuitBreakerStatus::HalfOpen => "HALF_OPEN",
    }
}
