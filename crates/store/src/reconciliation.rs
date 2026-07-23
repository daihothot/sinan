//! Durable reconciliation runs and account-state checkpoints.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sinan_protocol::{ReconciliationReason, ReconciliationRequest, ReconciliationResult};
use sinan_types::{
    AccountId, AccountSnapshot, CommandId, OrderSnapshot, PositionSnapshot, RequestId,
    SymbolMetadataSnapshot,
};
use sqlx::{Row, SqliteConnection};

use crate::{
    connection::{SqliteStateStore, WriteTransaction},
    json::CanonicalJson,
    model::{CoreEventMetadata, NewCoreEvent, StoredCoreEvent, WriteOutcome},
    projection::{
        apply_account, apply_order, apply_position, apply_symbol,
        validate_account_durable_snapshot_full_set_consistency,
        validate_all_durable_snapshot_full_set_consistency,
    },
    repository::{append_core_event_on, fetch_core_event_by_id},
    StoreError,
};

const REQUEST_EVENT_TYPE: &str = "reconciliation.request";
const RESULT_EVENT_TYPE: &str = "reconciliation.result";
const RECONCILIATION_AGGREGATE: &str = "reconciliation";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReconciliationRunStatus {
    Requested,
    PendingEvidence,
    Completed,
    ManualReconciliationRequired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReconciliationDisposition {
    Completed,
    PendingEvidence,
    ManualRequired,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReconciliationEvaluation {
    pub request_id: RequestId,
    pub account_id: AccountId,
    pub observed_at: Option<i64>,
    pub disposition: ReconciliationDisposition,
    pub command_ids: Vec<CommandId>,
    pub findings: Vec<Value>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReconciliationCompleteness {
    /// True only when the producer explicitly attests that `symbol_metadata`
    /// is a complete readiness observation for the account.
    pub symbol_metadata_complete: bool,
    /// True only when an account-wide evaluation used the complete command
    /// scope from one trusted Store read snapshot.
    pub command_scope_complete: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ManualReconciliationEvidence {
    pub request_id: RequestId,
    pub escalated_at: i64,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct NewReconciliationRun {
    pub request: ReconciliationRequest,
    pub requested_at: i64,
    pub event_metadata: CoreEventMetadata,
}

#[derive(Clone, Debug, PartialEq)]
pub struct NewReconciliationResult {
    pub result: ReconciliationResult,
    pub evaluation: ReconciliationEvaluation,
    pub completeness: ReconciliationCompleteness,
    pub event_metadata: CoreEventMetadata,
}

#[derive(Clone, Debug, PartialEq)]
pub struct NewManualReconciliationEscalation {
    pub evidence: ManualReconciliationEvidence,
    pub evaluation: ReconciliationEvaluation,
    pub updated_at: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct StoredReconciliationRun {
    pub request: ReconciliationRequest,
    pub requested_at: i64,
    pub status: ReconciliationRunStatus,
    pub result: Option<ReconciliationResult>,
    pub result_evaluation: Option<ReconciliationEvaluation>,
    pub completeness: Option<ReconciliationCompleteness>,
    pub manual_evidence: Option<ManualReconciliationEvidence>,
    pub manual_evaluation: Option<ReconciliationEvaluation>,
    pub request_event: StoredCoreEvent,
    pub result_event: Option<StoredCoreEvent>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccountReconciliationCheckpoint {
    pub account_id: AccountId,
    pub source_request_id: RequestId,
    pub result_observed_at: i64,
    pub account_refreshed_at: Option<i64>,
    pub positions_observed_at: i64,
    pub positions_set_hash: String,
    pub orders_observed_at: i64,
    pub orders_set_hash: String,
    pub symbol_metadata_refreshed_at: Option<i64>,
    pub pending_commands_reconciled_at: Option<i64>,
    pub updated_at: i64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReconciliationProjectionRebuildReport {
    pub replayed_snapshot_facts: u64,
    pub replayed_reconciliation_results: u64,
}

impl SqliteStateStore {
    pub async fn create_reconciliation_run(
        &self,
        new_run: NewReconciliationRun,
    ) -> Result<WriteOutcome<StoredReconciliationRun>, StoreError> {
        let mut transaction = self.begin_write().await?;
        let result = create_reconciliation_run_on(transaction.connection(), new_run).await;
        finish_write(transaction, result).await
    }

    pub async fn get_reconciliation_run(
        &self,
        request_id: &RequestId,
    ) -> Result<Option<StoredReconciliationRun>, StoreError> {
        let mut connection = self.pool().acquire().await?;
        fetch_reconciliation_run_on(&mut connection, request_id).await
    }

    pub async fn commit_reconciliation_result(
        &self,
        new_result: NewReconciliationResult,
    ) -> Result<WriteOutcome<StoredReconciliationRun>, StoreError> {
        let mut transaction = self.begin_write().await?;
        let result = commit_reconciliation_result_on(transaction.connection(), new_result).await;
        finish_write(transaction, result).await
    }

    pub async fn escalate_reconciliation_manual(
        &self,
        escalation: NewManualReconciliationEscalation,
    ) -> Result<WriteOutcome<StoredReconciliationRun>, StoreError> {
        let mut transaction = self.begin_write().await?;
        let result = escalate_reconciliation_manual_on(transaction.connection(), escalation).await;
        finish_write(transaction, result).await
    }

    pub async fn get_account_reconciliation_checkpoint(
        &self,
        account_id: &AccountId,
    ) -> Result<Option<AccountReconciliationCheckpoint>, StoreError> {
        let mut connection = self.pool().acquire().await?;
        fetch_checkpoint_on(&mut connection, account_id).await
    }

    /// Rebuilds account, symbol, position, order, and reconciliation checkpoint
    /// projections from durable snapshot and `reconciliation.result` facts.
    /// Market tables and execution lifecycle tables are deliberately untouched.
    pub async fn rebuild_reconciliation_projections(
        &self,
    ) -> Result<ReconciliationProjectionRebuildReport, StoreError> {
        let mut transaction = self.begin_write().await?;
        let result = rebuild_reconciliation_projections_on(transaction.connection()).await;
        finish_write(transaction, result).await
    }
}

impl WriteTransaction {
    pub async fn create_reconciliation_run(
        &mut self,
        new_run: NewReconciliationRun,
    ) -> Result<WriteOutcome<StoredReconciliationRun>, StoreError> {
        create_reconciliation_run_on(self.connection(), new_run).await
    }

    pub async fn get_reconciliation_run(
        &mut self,
        request_id: &RequestId,
    ) -> Result<Option<StoredReconciliationRun>, StoreError> {
        fetch_reconciliation_run_on(self.connection(), request_id).await
    }

    pub async fn commit_reconciliation_result(
        &mut self,
        new_result: NewReconciliationResult,
    ) -> Result<WriteOutcome<StoredReconciliationRun>, StoreError> {
        commit_reconciliation_result_on(self.connection(), new_result).await
    }
}

async fn finish_write<T>(
    transaction: crate::WriteTransaction,
    result: Result<T, StoreError>,
) -> Result<T, StoreError> {
    match result {
        Ok(value) => {
            transaction.commit().await?;
            Ok(value)
        }
        Err(error) => {
            let _ = transaction.rollback().await;
            Err(error)
        }
    }
}

async fn create_reconciliation_run_on(
    connection: &mut SqliteConnection,
    new_run: NewReconciliationRun,
) -> Result<WriteOutcome<StoredReconciliationRun>, StoreError> {
    validate_request(&new_run.request, new_run.requested_at)?;
    validate_event_metadata(
        &new_run.event_metadata,
        REQUEST_EVENT_TYPE,
        &new_run.request.request_id,
        &new_run.request.account_id,
        new_run
            .request
            .terminal_id
            .as_ref()
            .map(|value| value.as_str()),
        new_run
            .request
            .client_id
            .as_ref()
            .map(|value| value.as_str()),
        new_run.requested_at,
    )?;
    let request_payload = CanonicalJson::from_serializable(&new_run.request)?;
    let command_ids = new_run
        .request
        .command_ids
        .as_ref()
        .map(CanonicalJson::from_serializable)
        .transpose()?;

    let event_outcome = append_core_event_on(
        connection,
        NewCoreEvent {
            metadata: new_run.event_metadata.clone(),
            payload: request_payload.clone(),
        },
    )
    .await?;

    let insert = sqlx::query(
        "INSERT INTO reconciliation_runs (\
            request_id, request_event_id, account_id, terminal_id, client_id, reason, \
            scope, command_ids_json, command_ids_hash, since_server_time, requested_at, \
            status, request_payload_json, request_payload_hash, created_at, updated_at\
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'REQUESTED', ?, ?, ?, ?) \
         ON CONFLICT DO NOTHING",
    )
    .bind(new_run.request.request_id.as_str())
    .bind(&new_run.event_metadata.event_id)
    .bind(new_run.request.account_id.as_str())
    .bind(
        new_run
            .request
            .terminal_id
            .as_ref()
            .map(|value| value.as_str()),
    )
    .bind(
        new_run
            .request
            .client_id
            .as_ref()
            .map(|value| value.as_str()),
    )
    .bind(reason_name(new_run.request.reason)?)
    .bind(if command_ids.is_some() {
        "TARGETED"
    } else {
        "ACCOUNT"
    })
    .bind(command_ids.as_ref().map(CanonicalJson::as_str))
    .bind(command_ids.as_ref().map(CanonicalJson::sha256_hex))
    .bind(new_run.request.since_server_time)
    .bind(new_run.requested_at)
    .bind(request_payload.as_str())
    .bind(request_payload.sha256_hex())
    .bind(new_run.event_metadata.created_at)
    .bind(new_run.event_metadata.created_at)
    .execute(&mut *connection)
    .await?;

    let stored = fetch_reconciliation_run_on(connection, &new_run.request.request_id)
        .await?
        .ok_or_else(|| StoreError::NotFound {
            entity: "reconciliation_run",
            key: new_run.request.request_id.to_string(),
        })?;

    if insert.rows_affected() == 1 && event_outcome.was_inserted() {
        Ok(WriteOutcome::Inserted(stored))
    } else if insert.rows_affected() == 0
        && !event_outcome.was_inserted()
        && same_request_run(&stored, &new_run)
    {
        Ok(WriteOutcome::Duplicate(stored))
    } else {
        Err(StoreError::conflict(
            "reconciliation_run",
            new_run.request.request_id.to_string(),
        ))
    }
}

async fn commit_reconciliation_result_on(
    connection: &mut SqliteConnection,
    new_result: NewReconciliationResult,
) -> Result<WriteOutcome<StoredReconciliationRun>, StoreError> {
    let request_id = new_result.result.request_id.clone();
    let existing = fetch_reconciliation_run_on(connection, &request_id)
        .await?
        .ok_or_else(|| StoreError::NotFound {
            entity: "reconciliation_run",
            key: request_id.to_string(),
        })?;

    validate_result_bundle(&existing, &new_result)?;
    let result_payload = CanonicalJson::from_serializable(&new_result.result)?;
    let evaluation_payload = CanonicalJson::from_serializable(&new_result.evaluation)?;
    let completeness_payload = CanonicalJson::from_serializable(&new_result.completeness)?;

    if existing.result.is_some() {
        if same_result_commit(&existing, &new_result) {
            rebuild_reconciliation_account_projections_on(connection, &existing.request.account_id)
                .await?;
            return Ok(WriteOutcome::Duplicate(existing));
        }
        return Err(StoreError::conflict(
            "reconciliation_result",
            request_id.to_string(),
        ));
    }
    if existing.status != ReconciliationRunStatus::Requested {
        return Err(StoreError::StaleWrite {
            entity: "reconciliation_run",
            key: request_id.to_string(),
        });
    }

    let event_outcome = append_core_event_on(
        connection,
        NewCoreEvent {
            metadata: new_result.event_metadata.clone(),
            payload: result_payload.clone(),
        },
    )
    .await?;
    if !event_outcome.was_inserted() {
        return Err(StoreError::conflict(
            "reconciliation_result",
            request_id.to_string(),
        ));
    }

    validate_account_durable_snapshot_full_set_consistency(
        connection,
        &new_result.result.account_id,
    )
    .await?;

    apply_result_projections(
        connection,
        &existing.request,
        &new_result.result,
        &new_result.evaluation,
        new_result.completeness,
        new_result.event_metadata.received_at,
    )
    .await?;

    let status = status_for_disposition(new_result.evaluation.disposition);
    let update = sqlx::query(
        "UPDATE reconciliation_runs SET \
            status = ?, result_event_id = ?, result_observed_at = ?, \
            result_payload_json = ?, result_payload_hash = ?, \
            result_evaluation_json = ?, result_evaluation_hash = ?, \
            completeness_json = ?, completeness_hash = ?, symbol_metadata_complete = ?, \
            command_scope_complete = ?, \
            updated_at = ? \
         WHERE request_id = ? AND status = 'REQUESTED' AND result_event_id IS NULL",
    )
    .bind(run_status_name(status))
    .bind(&new_result.event_metadata.event_id)
    .bind(new_result.result.observed_at)
    .bind(result_payload.as_str())
    .bind(result_payload.sha256_hex())
    .bind(evaluation_payload.as_str())
    .bind(evaluation_payload.sha256_hex())
    .bind(completeness_payload.as_str())
    .bind(completeness_payload.sha256_hex())
    .bind(i64::from(new_result.completeness.symbol_metadata_complete))
    .bind(i64::from(new_result.completeness.command_scope_complete))
    .bind(new_result.event_metadata.created_at)
    .bind(request_id.as_str())
    .execute(&mut *connection)
    .await?;
    if update.rows_affected() != 1 {
        return Err(StoreError::StaleWrite {
            entity: "reconciliation_run",
            key: request_id.to_string(),
        });
    }

    let stored = fetch_reconciliation_run_on(connection, &request_id)
        .await?
        .ok_or_else(|| StoreError::NotFound {
            entity: "reconciliation_run",
            key: request_id.to_string(),
        })?;
    Ok(WriteOutcome::Inserted(stored))
}

async fn escalate_reconciliation_manual_on(
    connection: &mut SqliteConnection,
    escalation: NewManualReconciliationEscalation,
) -> Result<WriteOutcome<StoredReconciliationRun>, StoreError> {
    let request_id = escalation.evidence.request_id.clone();
    let existing = fetch_reconciliation_run_on(connection, &request_id)
        .await?
        .ok_or_else(|| StoreError::NotFound {
            entity: "reconciliation_run",
            key: request_id.to_string(),
        })?;
    validate_manual_escalation(&existing, &escalation)?;

    if existing.manual_evidence.is_some() {
        if existing.manual_evidence.as_ref() == Some(&escalation.evidence)
            && existing.manual_evaluation.as_ref() == Some(&escalation.evaluation)
            && existing.updated_at == escalation.updated_at
        {
            return Ok(WriteOutcome::Duplicate(existing));
        }
        return Err(StoreError::conflict(
            "manual_reconciliation_evidence",
            request_id.to_string(),
        ));
    }
    if existing.status == ReconciliationRunStatus::Completed {
        return Err(StoreError::StaleWrite {
            entity: "reconciliation_run",
            key: request_id.to_string(),
        });
    }

    let evidence = CanonicalJson::from_serializable(&escalation.evidence)?;
    let evaluation = CanonicalJson::from_serializable(&escalation.evaluation)?;
    let update = sqlx::query(
        "UPDATE reconciliation_runs SET \
            status = 'MANUAL_RECONCILIATION_REQUIRED', \
            manual_evidence_json = ?, manual_evidence_hash = ?, \
            manual_evaluation_json = ?, manual_evaluation_hash = ?, updated_at = ? \
         WHERE request_id = ? AND status != 'COMPLETED' AND manual_evidence_json IS NULL",
    )
    .bind(evidence.as_str())
    .bind(evidence.sha256_hex())
    .bind(evaluation.as_str())
    .bind(evaluation.sha256_hex())
    .bind(escalation.updated_at)
    .bind(request_id.as_str())
    .execute(&mut *connection)
    .await?;
    if update.rows_affected() != 1 {
        return Err(StoreError::StaleWrite {
            entity: "reconciliation_run",
            key: request_id.to_string(),
        });
    }
    let stored = fetch_reconciliation_run_on(connection, &request_id)
        .await?
        .ok_or_else(|| StoreError::NotFound {
            entity: "reconciliation_run",
            key: request_id.to_string(),
        })?;
    Ok(WriteOutcome::Inserted(stored))
}

pub(crate) async fn fetch_reconciliation_run_on(
    connection: &mut SqliteConnection,
    request_id: &RequestId,
) -> Result<Option<StoredReconciliationRun>, StoreError> {
    let row = sqlx::query(
        "SELECT request_id, request_event_id, account_id, terminal_id, client_id, reason, scope, \
                command_ids_json, command_ids_hash, since_server_time, requested_at, status, \
                request_payload_json, request_payload_hash, result_event_id, result_observed_at, \
                result_payload_json, result_payload_hash, result_evaluation_json, \
                result_evaluation_hash, completeness_json, completeness_hash, \
                symbol_metadata_complete, command_scope_complete, manual_evidence_json, manual_evidence_hash, \
                manual_evaluation_json, manual_evaluation_hash, created_at, updated_at \
         FROM reconciliation_runs WHERE request_id = ?",
    )
    .bind(request_id.as_str())
    .fetch_optional(&mut *connection)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let key: String = row.try_get("request_id")?;
    let request_payload = CanonicalJson::from_stored(
        "reconciliation_run.request",
        &key,
        row.try_get("request_payload_json")?,
        row.try_get("request_payload_hash")?,
    )?;
    let request: ReconciliationRequest =
        decode_canonical("reconciliation_run.request", &key, &request_payload)?;
    let requested_at: i64 = row.try_get("requested_at")?;
    validate_request(&request, requested_at).map_err(|error| corrupt_from_invalid(error, &key))?;
    validate_request_aliases(&row, &request, &key)?;

    let request_event_id: String = row.try_get("request_event_id")?;
    let request_event = fetch_core_event_by_id(&mut *connection, &request_event_id)
        .await?
        .ok_or_else(|| {
            StoreError::corrupt("reconciliation_run", &key, "request event is missing")
        })?;
    validate_stored_event(
        &request_event,
        REQUEST_EVENT_TYPE,
        &request.request_id,
        &request.account_id,
        request.terminal_id.as_ref().map(|value| value.as_str()),
        request.client_id.as_ref().map(|value| value.as_str()),
        requested_at,
        &request_payload,
    )
    .map_err(|error| corrupt_from_invalid(error, &key))?;

    let status = parse_run_status(row.try_get("status")?, &key)?;
    let result_event_id: Option<String> = row.try_get("result_event_id")?;
    let result_observed_at: Option<i64> = row.try_get("result_observed_at")?;
    let result_payload = optional_canonical(
        &row,
        "reconciliation_run.result",
        &key,
        "result_payload_json",
        "result_payload_hash",
    )?;
    let result = result_payload
        .as_ref()
        .map(|payload| decode_canonical("reconciliation_run.result", &key, payload))
        .transpose()?;
    let result_evaluation = optional_decoded(
        &row,
        "reconciliation_run.result_evaluation",
        &key,
        "result_evaluation_json",
        "result_evaluation_hash",
    )?;
    let completeness: Option<ReconciliationCompleteness> = optional_decoded(
        &row,
        "reconciliation_run.completeness",
        &key,
        "completeness_json",
        "completeness_hash",
    )?;
    validate_result_columns(
        &row,
        &request,
        result_observed_at,
        result.as_ref(),
        result_evaluation.as_ref(),
        completeness.as_ref(),
        &key,
    )?;

    let result_event = match (&result_event_id, result_payload.as_ref(), result.as_ref()) {
        (Some(event_id), Some(payload), Some(result)) => {
            let event = fetch_core_event_by_id(&mut *connection, event_id)
                .await?
                .ok_or_else(|| {
                    StoreError::corrupt("reconciliation_run", &key, "result event is missing")
                })?;
            validate_stored_event(
                &event,
                RESULT_EVENT_TYPE,
                &result.request_id,
                &result.account_id,
                result.terminal_id.as_ref().map(|value| value.as_str()),
                result.client_id.as_ref().map(|value| value.as_str()),
                result.observed_at,
                payload,
            )
            .map_err(|error| corrupt_from_invalid(error, &key))?;
            Some(event)
        }
        (None, None, None) => None,
        _ => {
            return Err(StoreError::corrupt(
                "reconciliation_run",
                &key,
                "result columns are only partially populated",
            ));
        }
    };

    let manual_evidence: Option<ManualReconciliationEvidence> = optional_decoded(
        &row,
        "reconciliation_run.manual_evidence",
        &key,
        "manual_evidence_json",
        "manual_evidence_hash",
    )?;
    let manual_evaluation: Option<ReconciliationEvaluation> = optional_decoded(
        &row,
        "reconciliation_run.manual_evaluation",
        &key,
        "manual_evaluation_json",
        "manual_evaluation_hash",
    )?;
    validate_manual_columns(
        &request,
        requested_at,
        result_observed_at,
        result_evaluation.as_ref(),
        manual_evidence.as_ref(),
        manual_evaluation.as_ref(),
        status,
        &key,
    )?;

    let created_at: i64 = row.try_get("created_at")?;
    let updated_at: i64 = row.try_get("updated_at")?;
    let status_time_valid = match status {
        ReconciliationRunStatus::Requested => updated_at == created_at,
        ReconciliationRunStatus::PendingEvidence | ReconciliationRunStatus::Completed => {
            result_event
                .as_ref()
                .is_some_and(|event| updated_at == event.metadata.created_at)
        }
        ReconciliationRunStatus::ManualReconciliationRequired => {
            manual_evidence
                .as_ref()
                .is_some_and(|evidence| updated_at >= evidence.escalated_at)
                && result_event
                    .as_ref()
                    .is_none_or(|event| updated_at >= event.metadata.created_at)
        }
    };
    if created_at != request_event.metadata.created_at
        || created_at < requested_at
        || updated_at < created_at
        || !status_time_valid
    {
        return Err(StoreError::corrupt(
            "reconciliation_run",
            &key,
            "created_at/updated_at aliases are not monotonic",
        ));
    }
    validate_run_state_consistency(
        status,
        result.as_ref(),
        result_evaluation.as_ref(),
        manual_evidence.as_ref(),
        manual_evaluation.as_ref(),
        &key,
    )?;

    Ok(Some(StoredReconciliationRun {
        request,
        requested_at,
        status,
        result,
        result_evaluation,
        completeness,
        manual_evidence,
        manual_evaluation,
        request_event,
        result_event,
        created_at,
        updated_at,
    }))
}

pub(crate) async fn fetch_checkpoint_on(
    connection: &mut SqliteConnection,
    account_id: &AccountId,
) -> Result<Option<AccountReconciliationCheckpoint>, StoreError> {
    let row = sqlx::query(
        "SELECT account_id, source_request_id, result_observed_at, account_refreshed_at, \
                positions_observed_at, positions_set_hash, orders_observed_at, orders_set_hash, \
                symbol_metadata_refreshed_at, pending_commands_reconciled_at, updated_at \
         FROM account_reconciliation_checkpoints WHERE account_id = ?",
    )
    .bind(account_id.as_str())
    .fetch_optional(&mut *connection)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let stored_account: String = row.try_get("account_id")?;
    if stored_account != account_id.as_str() {
        return Err(StoreError::corrupt(
            "account_reconciliation_checkpoint",
            account_id.to_string(),
            "account_id alias does not match lookup key",
        ));
    }
    let checkpoint = AccountReconciliationCheckpoint {
        account_id: AccountId::from(stored_account),
        source_request_id: RequestId::from(row.try_get::<String, _>("source_request_id")?),
        result_observed_at: row.try_get("result_observed_at")?,
        account_refreshed_at: row.try_get("account_refreshed_at")?,
        positions_observed_at: row.try_get("positions_observed_at")?,
        positions_set_hash: row.try_get("positions_set_hash")?,
        orders_observed_at: row.try_get("orders_observed_at")?,
        orders_set_hash: row.try_get("orders_set_hash")?,
        symbol_metadata_refreshed_at: row.try_get("symbol_metadata_refreshed_at")?,
        pending_commands_reconciled_at: row.try_get("pending_commands_reconciled_at")?,
        updated_at: row.try_get("updated_at")?,
    };
    validate_checkpoint_integrity(connection, &checkpoint).await?;
    Ok(Some(checkpoint))
}

fn validate_request(request: &ReconciliationRequest, requested_at: i64) -> Result<(), StoreError> {
    validate_nonempty(
        "reconciliation_request",
        request.request_id.as_str(),
        "request_id",
    )?;
    validate_nonempty(
        "reconciliation_request",
        request.account_id.as_str(),
        "account_id",
    )?;
    if request
        .terminal_id
        .as_ref()
        .is_some_and(|value| value.as_str().trim().is_empty())
        || request
            .client_id
            .as_ref()
            .is_some_and(|value| value.as_str().trim().is_empty())
    {
        return invalid(
            "reconciliation_request",
            request.request_id.to_string(),
            "terminal_id and client_id must be non-empty when present",
        );
    }
    if request.command_ids.as_ref().is_some_and(|ids| {
        ids.is_empty()
            || !strictly_sorted_unique(ids)
            || ids.iter().any(|id| id.as_str().trim().is_empty())
    }) {
        return invalid(
            "reconciliation_request",
            request.request_id.to_string(),
            "command_ids must be None or a non-empty unique sorted list",
        );
    }
    if requested_at < 0 {
        return invalid(
            "reconciliation_request",
            request.request_id.to_string(),
            "requested_at must be non-negative",
        );
    }
    if request
        .since_server_time
        .is_some_and(|since| since < 0 || since > requested_at)
    {
        return invalid(
            "reconciliation_request",
            request.request_id.to_string(),
            "since_server_time must be non-negative and not exceed requested_at",
        );
    }
    Ok(())
}

fn validate_result_bundle(
    run: &StoredReconciliationRun,
    bundle: &NewReconciliationResult,
) -> Result<(), StoreError> {
    let result = &bundle.result;
    if result.request_id != run.request.request_id
        || result.account_id != run.request.account_id
        || result.terminal_id != run.request.terminal_id
        || result.client_id != run.request.client_id
    {
        return Err(StoreError::IdentityConflict {
            entity: "reconciliation_result",
            key: result.request_id.to_string(),
        });
    }
    if result.observed_at < run.requested_at {
        return invalid(
            "reconciliation_result",
            result.request_id.to_string(),
            "observed_at must not predate requested_at",
        );
    }
    if bundle.evaluation.disposition == ReconciliationDisposition::ManualRequired {
        return invalid(
            "reconciliation_result",
            result.request_id.to_string(),
            "MANUAL_REQUIRED must use explicit manual escalation evidence",
        );
    }
    if bundle.event_metadata.created_at < run.updated_at {
        return Err(StoreError::StaleWrite {
            entity: "reconciliation_run",
            key: result.request_id.to_string(),
        });
    }
    validate_event_metadata(
        &bundle.event_metadata,
        RESULT_EVENT_TYPE,
        &result.request_id,
        &result.account_id,
        result.terminal_id.as_ref().map(|value| value.as_str()),
        result.client_id.as_ref().map(|value| value.as_str()),
        result.observed_at,
    )?;
    validate_result_sets(result, run.request.command_ids.as_deref())?;
    validate_evaluation(&bundle.evaluation, &run.request, Some(result.observed_at))?;
    validate_completeness(&bundle.completeness, &run.request)?;
    validate_result_evaluation_link(result, &bundle.evaluation)
}

fn validate_result_sets(
    result: &ReconciliationResult,
    target: Option<&[CommandId]>,
) -> Result<(), StoreError> {
    let key = result.request_id.to_string();
    if let Some(account) = &result.account {
        if account.account_id != result.account_id || account.observed_at != result.observed_at {
            return invalid(
                "reconciliation_result",
                &key,
                "account snapshot must match result account_id and observed_at",
            );
        }
    }
    if !strictly_sorted_unique_by(&result.positions, |value| value.position_id.as_str())
        || result.positions.iter().any(|value| {
            value.position_id.as_str().trim().is_empty()
                || value.account_id != result.account_id
                || value.observed_at != result.observed_at
        })
    {
        return invalid(
            "reconciliation_result",
            &key,
            "positions must be a unique sorted full set for the result account and observed_at",
        );
    }
    if !strictly_sorted_unique_by(&result.orders, |value| value.broker_order_id.as_str())
        || result.orders.iter().any(|value| {
            value.broker_order_id.as_str().trim().is_empty()
                || value.account_id != result.account_id
                || value.observed_at != result.observed_at
        })
    {
        return invalid(
            "reconciliation_result",
            &key,
            "orders must be a unique sorted full set for the result account and observed_at",
        );
    }
    if !strictly_sorted_unique_by(&result.symbol_metadata, |value| {
        value.broker_symbol.as_str()
    }) || result.symbol_metadata.iter().any(|value| {
        value.broker_symbol.trim().is_empty()
            || value.account_id != result.account_id
            || value.observed_at != result.observed_at
    }) {
        return invalid(
            "reconciliation_result",
            &key,
            "symbol_metadata must be unique, sorted, and match account_id and observed_at",
        );
    }
    if !strictly_sorted_unique(&result.unresolved_command_ids)
        || result
            .unresolved_command_ids
            .iter()
            .any(|command_id| command_id.as_str().trim().is_empty())
    {
        return invalid(
            "reconciliation_result",
            &key,
            "unresolved_command_ids must be unique and sorted",
        );
    }
    if let Some(target) = target {
        let target: BTreeSet<_> = target.iter().collect();
        if result
            .unresolved_command_ids
            .iter()
            .any(|command_id| !target.contains(command_id))
        {
            return invalid(
                "reconciliation_result",
                &key,
                "targeted result contains an unresolved command outside request scope",
            );
        }
    }
    Ok(())
}

fn validate_evaluation(
    evaluation: &ReconciliationEvaluation,
    request: &ReconciliationRequest,
    expected_observed_at: Option<i64>,
) -> Result<(), StoreError> {
    if evaluation.request_id != request.request_id
        || evaluation.account_id != request.account_id
        || evaluation.observed_at != expected_observed_at
    {
        return Err(StoreError::IdentityConflict {
            entity: "reconciliation_evaluation",
            key: request.request_id.to_string(),
        });
    }
    if !strictly_sorted_unique(&evaluation.command_ids)
        || evaluation
            .command_ids
            .iter()
            .any(|command_id| command_id.as_str().trim().is_empty())
    {
        return invalid(
            "reconciliation_evaluation",
            request.request_id.to_string(),
            "command_ids must be unique and sorted",
        );
    }
    match evaluation.disposition {
        ReconciliationDisposition::Completed if !evaluation.command_ids.is_empty() => {
            return invalid(
                "reconciliation_evaluation",
                request.request_id.to_string(),
                "completed evaluation cannot retain attention commands",
            );
        }
        ReconciliationDisposition::PendingEvidence if evaluation.command_ids.is_empty() => {
            return invalid(
                "reconciliation_evaluation",
                request.request_id.to_string(),
                "pending evaluation must retain at least one attention command",
            );
        }
        _ => {}
    }
    if let Some(target) = &request.command_ids {
        let target: BTreeSet<_> = target.iter().collect();
        if evaluation
            .command_ids
            .iter()
            .any(|command_id| !target.contains(command_id))
        {
            return invalid(
                "reconciliation_evaluation",
                request.request_id.to_string(),
                "evaluation command is outside targeted request scope",
            );
        }
    }
    Ok(())
}

fn validate_completeness(
    completeness: &ReconciliationCompleteness,
    request: &ReconciliationRequest,
) -> Result<(), StoreError> {
    if completeness.command_scope_complete && request.command_ids.is_some() {
        return invalid(
            "reconciliation_completeness",
            request.request_id.to_string(),
            "command_scope_complete requires an account-wide request",
        );
    }
    Ok(())
}

fn validate_result_evaluation_link(
    result: &ReconciliationResult,
    evaluation: &ReconciliationEvaluation,
) -> Result<(), StoreError> {
    if !result.unresolved_command_ids.is_empty()
        && evaluation.disposition == ReconciliationDisposition::Completed
    {
        return invalid(
            "reconciliation_evaluation",
            result.request_id.to_string(),
            "a result with unresolved commands cannot be Completed",
        );
    }
    let attention: BTreeSet<_> = evaluation.command_ids.iter().collect();
    if result
        .unresolved_command_ids
        .iter()
        .any(|command_id| !attention.contains(command_id))
    {
        return invalid(
            "reconciliation_evaluation",
            result.request_id.to_string(),
            "unresolved commands must be included in evaluation.command_ids",
        );
    }
    Ok(())
}

fn validate_manual_escalation(
    run: &StoredReconciliationRun,
    escalation: &NewManualReconciliationEscalation,
) -> Result<(), StoreError> {
    let evidence = &escalation.evidence;
    if evidence.request_id != run.request.request_id {
        return Err(StoreError::IdentityConflict {
            entity: "manual_reconciliation_evidence",
            key: evidence.request_id.to_string(),
        });
    }
    if evidence.reason.trim().is_empty() {
        return invalid(
            "manual_reconciliation_evidence",
            evidence.request_id.to_string(),
            "reason must not be empty",
        );
    }
    if evidence.escalated_at < run.requested_at
        || run
            .result
            .as_ref()
            .is_some_and(|result| evidence.escalated_at < result.observed_at)
        || escalation.updated_at < evidence.escalated_at
        || escalation.updated_at < run.updated_at
    {
        return invalid(
            "manual_reconciliation_evidence",
            evidence.request_id.to_string(),
            "escalation timestamps must not predate request or result",
        );
    }
    if escalation.evaluation.disposition != ReconciliationDisposition::ManualRequired {
        return invalid(
            "manual_reconciliation_evidence",
            evidence.request_id.to_string(),
            "evaluation disposition must be MANUAL_REQUIRED",
        );
    }
    if let Some(result_evaluation) = &run.result_evaluation {
        if result_evaluation.disposition != ReconciliationDisposition::PendingEvidence
            || escalation.evaluation.request_id != result_evaluation.request_id
            || escalation.evaluation.account_id != result_evaluation.account_id
            || escalation.evaluation.observed_at != result_evaluation.observed_at
            || escalation.evaluation.command_ids != result_evaluation.command_ids
            || escalation.evaluation.findings != result_evaluation.findings
        {
            return invalid(
                "manual_reconciliation_evidence",
                evidence.request_id.to_string(),
                "manual evaluation may only change a pending result disposition",
            );
        }
    }
    validate_evaluation(
        &escalation.evaluation,
        &run.request,
        run.result.as_ref().map(|result| result.observed_at),
    )
}

fn validate_event_metadata(
    metadata: &CoreEventMetadata,
    event_type: &'static str,
    request_id: &RequestId,
    account_id: &AccountId,
    terminal_id: Option<&str>,
    client_id: Option<&str>,
    event_at: i64,
) -> Result<(), StoreError> {
    let route_matches = metadata.account_id.as_ref().map(|value| value.as_str())
        == Some(account_id.as_str())
        && metadata.terminal_id.as_ref().map(|value| value.as_str()) == terminal_id
        && metadata.client_id.as_ref().map(|value| value.as_str()) == client_id;
    if metadata.event_type != event_type
        || metadata.aggregate_type != RECONCILIATION_AGGREGATE
        || metadata.aggregate_id != request_id.as_str()
        || metadata.event_at != event_at
        || metadata.event_at < 0
        || metadata.received_at < metadata.event_at
        || metadata.created_at < metadata.received_at
        || !route_matches
        || metadata.strategy_id.is_some()
        || metadata.intent_id.is_some()
        || metadata.plan_id.is_some()
        || metadata.leg_id.is_some()
        || metadata.command_id.is_some()
        || metadata.idempotency_key.is_some()
        || metadata.event_id.trim().is_empty()
        || metadata.schema_version.trim().is_empty()
        || metadata.source.trim().is_empty()
    {
        return Err(StoreError::IdentityConflict {
            entity: "reconciliation_core_event",
            key: metadata.event_id.clone(),
        });
    }
    Ok(())
}

fn validate_stored_event(
    event: &StoredCoreEvent,
    event_type: &'static str,
    request_id: &RequestId,
    account_id: &AccountId,
    terminal_id: Option<&str>,
    client_id: Option<&str>,
    event_at: i64,
    payload: &CanonicalJson,
) -> Result<(), StoreError> {
    validate_event_metadata(
        &event.metadata,
        event_type,
        request_id,
        account_id,
        terminal_id,
        client_id,
        event_at,
    )?;
    if &event.payload != payload {
        return Err(StoreError::IdentityConflict {
            entity: "reconciliation_core_event.payload",
            key: event.metadata.event_id.clone(),
        });
    }
    Ok(())
}

fn validate_request_aliases(
    row: &sqlx::sqlite::SqliteRow,
    request: &ReconciliationRequest,
    key: &str,
) -> Result<(), StoreError> {
    let command_ids = optional_canonical(
        row,
        "reconciliation_run.command_ids",
        key,
        "command_ids_json",
        "command_ids_hash",
    )?;
    let canonical_ids = request
        .command_ids
        .as_ref()
        .map(CanonicalJson::from_serializable)
        .transpose()?;
    let aliases_match = row.try_get::<String, _>("account_id")? == request.account_id.as_str()
        && row.try_get::<Option<String>, _>("terminal_id")?.as_deref()
            == request.terminal_id.as_ref().map(|value| value.as_str())
        && row.try_get::<Option<String>, _>("client_id")?.as_deref()
            == request.client_id.as_ref().map(|value| value.as_str())
        && row.try_get::<String, _>("reason")? == reason_name(request.reason)?
        && row.try_get::<String, _>("scope")?
            == if request.command_ids.is_some() {
                "TARGETED"
            } else {
                "ACCOUNT"
            }
        && command_ids == canonical_ids
        && row.try_get::<Option<i64>, _>("since_server_time")? == request.since_server_time;
    if aliases_match {
        Ok(())
    } else {
        Err(StoreError::corrupt(
            "reconciliation_run",
            key,
            "request payload aliases do not match columns",
        ))
    }
}

fn validate_result_columns(
    row: &sqlx::sqlite::SqliteRow,
    request: &ReconciliationRequest,
    observed_alias: Option<i64>,
    result: Option<&ReconciliationResult>,
    evaluation: Option<&ReconciliationEvaluation>,
    completeness: Option<&ReconciliationCompleteness>,
    key: &str,
) -> Result<(), StoreError> {
    match (result, evaluation, completeness) {
        (None, None, None) if observed_alias.is_none() => Ok(()),
        (Some(result), Some(evaluation), Some(completeness)) => {
            if observed_alias != Some(result.observed_at)
                || result.request_id != request.request_id
                || result.account_id != request.account_id
                || result.terminal_id != request.terminal_id
                || result.client_id != request.client_id
                || row.try_get::<Option<i64>, _>("symbol_metadata_complete")?
                    != Some(i64::from(completeness.symbol_metadata_complete))
                || row.try_get::<Option<i64>, _>("command_scope_complete")?
                    != Some(i64::from(completeness.command_scope_complete))
            {
                return Err(StoreError::corrupt(
                    "reconciliation_run",
                    key,
                    "result payload aliases do not match columns or request",
                ));
            }
            validate_result_sets(result, request.command_ids.as_deref())
                .map_err(|error| corrupt_from_invalid(error, key))?;
            validate_evaluation(evaluation, request, Some(result.observed_at))
                .and_then(|()| validate_completeness(completeness, request))
                .and_then(|()| validate_result_evaluation_link(result, evaluation))
                .map_err(|error| corrupt_from_invalid(error, key))
        }
        _ => Err(StoreError::corrupt(
            "reconciliation_run",
            key,
            "result columns are only partially populated",
        )),
    }
}

fn validate_manual_columns(
    request: &ReconciliationRequest,
    requested_at: i64,
    result_observed_at: Option<i64>,
    result_evaluation: Option<&ReconciliationEvaluation>,
    evidence: Option<&ManualReconciliationEvidence>,
    evaluation: Option<&ReconciliationEvaluation>,
    status: ReconciliationRunStatus,
    key: &str,
) -> Result<(), StoreError> {
    match (evidence, evaluation) {
        (None, None) => {
            if status == ReconciliationRunStatus::ManualReconciliationRequired {
                return Err(StoreError::corrupt(
                    "reconciliation_run",
                    key,
                    "manual status requires explicit evidence",
                ));
            }
            Ok(())
        }
        (Some(evidence), Some(evaluation)) => {
            if status != ReconciliationRunStatus::ManualReconciliationRequired
                || evidence.request_id != request.request_id
                || evidence.reason.trim().is_empty()
                || evidence.escalated_at < requested_at
                || result_observed_at.is_some_and(|at| evidence.escalated_at < at)
                || evaluation.disposition != ReconciliationDisposition::ManualRequired
                || result_evaluation.is_some_and(|result_evaluation| {
                    result_evaluation.disposition != ReconciliationDisposition::PendingEvidence
                        || evaluation.request_id != result_evaluation.request_id
                        || evaluation.account_id != result_evaluation.account_id
                        || evaluation.observed_at != result_evaluation.observed_at
                        || evaluation.command_ids != result_evaluation.command_ids
                        || evaluation.findings != result_evaluation.findings
                })
            {
                return Err(StoreError::corrupt(
                    "reconciliation_run",
                    key,
                    "manual evidence aliases or state are invalid",
                ));
            }
            validate_evaluation(evaluation, request, result_observed_at)
                .map_err(|error| corrupt_from_invalid(error, key))
        }
        _ => Err(StoreError::corrupt(
            "reconciliation_run",
            key,
            "manual evidence columns are only partially populated",
        )),
    }
}

fn validate_run_state_consistency(
    status: ReconciliationRunStatus,
    result: Option<&ReconciliationResult>,
    result_evaluation: Option<&ReconciliationEvaluation>,
    manual_evidence: Option<&ManualReconciliationEvidence>,
    manual_evaluation: Option<&ReconciliationEvaluation>,
    key: &str,
) -> Result<(), StoreError> {
    let valid = match status {
        ReconciliationRunStatus::Requested => {
            result.is_none() && manual_evidence.is_none() && manual_evaluation.is_none()
        }
        ReconciliationRunStatus::PendingEvidence => {
            result.is_some()
                && result_evaluation.is_some_and(|evaluation| {
                    evaluation.disposition == ReconciliationDisposition::PendingEvidence
                })
                && manual_evidence.is_none()
                && manual_evaluation.is_none()
        }
        ReconciliationRunStatus::Completed => {
            result.is_some()
                && result_evaluation.is_some_and(|evaluation| {
                    evaluation.disposition == ReconciliationDisposition::Completed
                })
                && manual_evidence.is_none()
                && manual_evaluation.is_none()
        }
        ReconciliationRunStatus::ManualReconciliationRequired => {
            manual_evidence.is_some()
                && manual_evaluation.is_some_and(|evaluation| {
                    evaluation.disposition == ReconciliationDisposition::ManualRequired
                })
        }
    };
    if valid {
        Ok(())
    } else {
        Err(StoreError::corrupt(
            "reconciliation_run",
            key,
            "status does not match durable result/manual evidence",
        ))
    }
}

fn same_request_run(stored: &StoredReconciliationRun, incoming: &NewReconciliationRun) -> bool {
    stored.request == incoming.request
        && stored.requested_at == incoming.requested_at
        && same_event_semantics(&stored.request_event.metadata, &incoming.event_metadata)
}

fn same_result_commit(
    stored: &StoredReconciliationRun,
    incoming: &NewReconciliationResult,
) -> bool {
    stored.result.as_ref() == Some(&incoming.result)
        && stored.result_evaluation.as_ref() == Some(&incoming.evaluation)
        && stored.completeness == Some(incoming.completeness)
        && stored
            .result_event
            .as_ref()
            .is_some_and(|event| same_event_semantics(&event.metadata, &incoming.event_metadata))
}

fn same_event_semantics(left: &CoreEventMetadata, right: &CoreEventMetadata) -> bool {
    left.event_id == right.event_id
        && left.event_type == right.event_type
        && left.aggregate_type == right.aggregate_type
        && left.aggregate_id == right.aggregate_id
        && left.message_id == right.message_id
        && left.schema_version == right.schema_version
        && left.correlation_id == right.correlation_id
        && left.causation_id == right.causation_id
        && left.account_id == right.account_id
        && left.client_id == right.client_id
        && left.terminal_id == right.terminal_id
        && left.event_at == right.event_at
        && left.source == right.source
}

fn optional_canonical(
    row: &sqlx::sqlite::SqliteRow,
    entity: &'static str,
    key: &str,
    json_column: &str,
    hash_column: &str,
) -> Result<Option<CanonicalJson>, StoreError> {
    let json: Option<String> = row.try_get(json_column)?;
    let hash: Option<String> = row.try_get(hash_column)?;
    match (json, hash) {
        (None, None) => Ok(None),
        (Some(json), Some(hash)) => CanonicalJson::from_stored(entity, key, json, hash).map(Some),
        _ => Err(StoreError::corrupt(
            entity,
            key,
            "JSON/hash columns are only partially populated",
        )),
    }
}

fn optional_decoded<T: serde::de::DeserializeOwned>(
    row: &sqlx::sqlite::SqliteRow,
    entity: &'static str,
    key: &str,
    json_column: &str,
    hash_column: &str,
) -> Result<Option<T>, StoreError> {
    optional_canonical(row, entity, key, json_column, hash_column)?
        .as_ref()
        .map(|payload| decode_canonical(entity, key, payload))
        .transpose()
}

fn decode_canonical<T: serde::de::DeserializeOwned>(
    entity: &'static str,
    key: &str,
    payload: &CanonicalJson,
) -> Result<T, StoreError> {
    serde_json::from_str(payload.as_str())
        .map_err(|error| StoreError::corrupt(entity, key, error.to_string()))
}

fn parse_run_status(raw: String, key: &str) -> Result<ReconciliationRunStatus, StoreError> {
    match raw.as_str() {
        "REQUESTED" => Ok(ReconciliationRunStatus::Requested),
        "PENDING_EVIDENCE" => Ok(ReconciliationRunStatus::PendingEvidence),
        "COMPLETED" => Ok(ReconciliationRunStatus::Completed),
        "MANUAL_RECONCILIATION_REQUIRED" => {
            Ok(ReconciliationRunStatus::ManualReconciliationRequired)
        }
        _ => Err(StoreError::corrupt(
            "reconciliation_run",
            key,
            format!("unknown status {raw:?}"),
        )),
    }
}

fn run_status_name(status: ReconciliationRunStatus) -> &'static str {
    match status {
        ReconciliationRunStatus::Requested => "REQUESTED",
        ReconciliationRunStatus::PendingEvidence => "PENDING_EVIDENCE",
        ReconciliationRunStatus::Completed => "COMPLETED",
        ReconciliationRunStatus::ManualReconciliationRequired => "MANUAL_RECONCILIATION_REQUIRED",
    }
}

fn status_for_disposition(disposition: ReconciliationDisposition) -> ReconciliationRunStatus {
    match disposition {
        ReconciliationDisposition::Completed => ReconciliationRunStatus::Completed,
        ReconciliationDisposition::PendingEvidence => ReconciliationRunStatus::PendingEvidence,
        ReconciliationDisposition::ManualRequired => {
            ReconciliationRunStatus::ManualReconciliationRequired
        }
    }
}

fn reason_name(reason: ReconciliationReason) -> Result<&'static str, StoreError> {
    match reason {
        ReconciliationReason::DeliveryUnconfirmed => Ok("DELIVERY_UNCONFIRMED"),
        ReconciliationReason::ConnectionRestored => Ok("CONNECTION_RESTORED"),
        ReconciliationReason::ManualRequest => Ok("MANUAL_REQUEST"),
        ReconciliationReason::StateStoreRestored => Ok("STATE_STORE_RESTORED"),
    }
}

fn strictly_sorted_unique<T: Ord>(values: &[T]) -> bool {
    values.windows(2).all(|pair| pair[0] < pair[1])
}

fn strictly_sorted_unique_by<T>(values: &[T], key: impl Fn(&T) -> &str) -> bool {
    values.windows(2).all(|pair| key(&pair[0]) < key(&pair[1]))
}

fn validate_nonempty(
    entity: &'static str,
    value: &str,
    field: &'static str,
) -> Result<(), StoreError> {
    if value.trim().is_empty() {
        invalid(entity, value, format!("{field} must not be empty"))
    } else {
        Ok(())
    }
}

fn invalid<T>(
    entity: &'static str,
    key: impl Into<String>,
    reason: impl Into<String>,
) -> Result<T, StoreError> {
    Err(StoreError::InvalidRecord {
        entity,
        key: key.into(),
        reason: reason.into(),
    })
}

fn corrupt_from_invalid(error: StoreError, key: &str) -> StoreError {
    match error {
        StoreError::InvalidRecord { reason, .. } | StoreError::CorruptData { reason, .. } => {
            StoreError::corrupt("reconciliation_run", key, reason)
        }
        StoreError::IdentityConflict { entity, .. } => StoreError::corrupt(
            "reconciliation_run",
            key,
            format!("{entity} identity aliases do not match"),
        ),
        other => other,
    }
}

async fn apply_result_projections(
    connection: &mut SqliteConnection,
    request: &ReconciliationRequest,
    result: &ReconciliationResult,
    evaluation: &ReconciliationEvaluation,
    completeness: ReconciliationCompleteness,
    updated_at: i64,
) -> Result<(), StoreError> {
    let existing_checkpoint = fetch_checkpoint_on(connection, &result.account_id).await?;
    if let Some(account) = &result.account {
        let payload = CanonicalJson::from_serializable(account)?;
        apply_account(connection, account, &payload, updated_at).await?;
    }
    for symbol in &result.symbol_metadata {
        let payload = CanonicalJson::from_serializable(symbol)?;
        apply_symbol(connection, symbol, &payload, updated_at).await?;
    }

    let positions_set = CanonicalJson::from_serializable(&result.positions)?;
    let orders_set = CanonicalJson::from_serializable(&result.orders)?;
    apply_position_full_set(connection, result, &positions_set, updated_at).await?;
    apply_order_full_set(connection, result, &orders_set, updated_at).await?;
    advance_checkpoint(
        connection,
        request,
        result,
        evaluation,
        completeness,
        positions_set.sha256_hex(),
        orders_set.sha256_hex(),
        updated_at,
        existing_checkpoint,
    )
    .await?;
    Ok(())
}

async fn apply_position_full_set(
    connection: &mut SqliteConnection,
    result: &ReconciliationResult,
    set_payload: &CanonicalJson,
    updated_at: i64,
) -> Result<(), StoreError> {
    let existing: Option<(i64, String)> = sqlx::query_as(
        "SELECT positions_observed_at, positions_set_hash \
         FROM account_reconciliation_checkpoints WHERE account_id = ?",
    )
    .bind(result.account_id.as_str())
    .fetch_optional(&mut *connection)
    .await?;
    match compare_full_set(
        "reconciliation_positions",
        &result.account_id,
        result.observed_at,
        set_payload.sha256_hex(),
        existing,
    )? {
        FullSetDecision::IgnoredOlder | FullSetDecision::Unchanged => return Ok(()),
        FullSetDecision::Applied => {}
    }

    for snapshot in &result.positions {
        let payload = CanonicalJson::from_serializable(snapshot)?;
        apply_position(connection, snapshot, &payload, updated_at).await?;
    }
    let retained: BTreeSet<&str> = result
        .positions
        .iter()
        .map(|snapshot| snapshot.position_id.as_str())
        .collect();
    let existing_rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT position_id, observed_at FROM position_snapshots_latest WHERE account_id = ?",
    )
    .bind(result.account_id.as_str())
    .fetch_all(&mut *connection)
    .await?;
    for (position_id, observed_at) in existing_rows {
        if observed_at <= result.observed_at && !retained.contains(position_id.as_str()) {
            sqlx::query(
                "DELETE FROM position_snapshots_latest WHERE account_id = ? AND position_id = ? \
                 AND observed_at <= ?",
            )
            .bind(result.account_id.as_str())
            .bind(&position_id)
            .bind(result.observed_at)
            .execute(&mut *connection)
            .await?;
        }
    }

    sqlx::query("DELETE FROM reconciliation_position_set_members WHERE account_id = ?")
        .bind(result.account_id.as_str())
        .execute(&mut *connection)
        .await?;
    for snapshot in &result.positions {
        let payload = CanonicalJson::from_serializable(snapshot)?;
        sqlx::query(
            "INSERT INTO reconciliation_position_set_members (\
                account_id, set_observed_at, position_id, payload_json, payload_hash\
             ) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(result.account_id.as_str())
        .bind(result.observed_at)
        .bind(snapshot.position_id.as_str())
        .bind(payload.as_str())
        .bind(payload.sha256_hex())
        .execute(&mut *connection)
        .await?;
    }
    Ok(())
}

async fn apply_order_full_set(
    connection: &mut SqliteConnection,
    result: &ReconciliationResult,
    set_payload: &CanonicalJson,
    updated_at: i64,
) -> Result<(), StoreError> {
    let existing: Option<(i64, String)> = sqlx::query_as(
        "SELECT orders_observed_at, orders_set_hash \
         FROM account_reconciliation_checkpoints WHERE account_id = ?",
    )
    .bind(result.account_id.as_str())
    .fetch_optional(&mut *connection)
    .await?;
    match compare_full_set(
        "reconciliation_orders",
        &result.account_id,
        result.observed_at,
        set_payload.sha256_hex(),
        existing,
    )? {
        FullSetDecision::IgnoredOlder | FullSetDecision::Unchanged => return Ok(()),
        FullSetDecision::Applied => {}
    }

    for snapshot in &result.orders {
        let payload = CanonicalJson::from_serializable(snapshot)?;
        apply_order(connection, snapshot, &payload, updated_at).await?;
    }
    let retained: BTreeSet<&str> = result
        .orders
        .iter()
        .map(|snapshot| snapshot.broker_order_id.as_str())
        .collect();
    let existing_rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT broker_order_id, observed_at FROM order_snapshots_latest WHERE account_id = ?",
    )
    .bind(result.account_id.as_str())
    .fetch_all(&mut *connection)
    .await?;
    for (broker_order_id, observed_at) in existing_rows {
        if observed_at <= result.observed_at && !retained.contains(broker_order_id.as_str()) {
            sqlx::query(
                "DELETE FROM order_snapshots_latest WHERE account_id = ? AND broker_order_id = ? \
                 AND observed_at <= ?",
            )
            .bind(result.account_id.as_str())
            .bind(&broker_order_id)
            .bind(result.observed_at)
            .execute(&mut *connection)
            .await?;
        }
    }

    sqlx::query("DELETE FROM reconciliation_order_set_members WHERE account_id = ?")
        .bind(result.account_id.as_str())
        .execute(&mut *connection)
        .await?;
    for snapshot in &result.orders {
        let payload = CanonicalJson::from_serializable(snapshot)?;
        sqlx::query(
            "INSERT INTO reconciliation_order_set_members (\
                account_id, set_observed_at, broker_order_id, payload_json, payload_hash\
             ) VALUES (?, ?, ?, ?, ?)",
        )
        .bind(result.account_id.as_str())
        .bind(result.observed_at)
        .bind(snapshot.broker_order_id.as_str())
        .bind(payload.as_str())
        .bind(payload.sha256_hex())
        .execute(&mut *connection)
        .await?;
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FullSetDecision {
    Applied,
    IgnoredOlder,
    Unchanged,
}

fn compare_full_set(
    entity: &'static str,
    account_id: &AccountId,
    observed_at: i64,
    set_hash: &str,
    existing: Option<(i64, String)>,
) -> Result<FullSetDecision, StoreError> {
    let Some((watermark, existing_hash)) = existing else {
        return Ok(FullSetDecision::Applied);
    };
    match observed_at.cmp(&watermark) {
        std::cmp::Ordering::Greater => Ok(FullSetDecision::Applied),
        std::cmp::Ordering::Less => Ok(FullSetDecision::IgnoredOlder),
        std::cmp::Ordering::Equal if existing_hash == set_hash => Ok(FullSetDecision::Unchanged),
        std::cmp::Ordering::Equal => Err(StoreError::ObservationConflict {
            entity,
            key: account_id.to_string(),
            observed_at,
        }),
    }
}

#[allow(clippy::too_many_arguments)]
async fn advance_checkpoint(
    connection: &mut SqliteConnection,
    request: &ReconciliationRequest,
    result: &ReconciliationResult,
    evaluation: &ReconciliationEvaluation,
    completeness: ReconciliationCompleteness,
    positions_set_hash: &str,
    orders_set_hash: &str,
    updated_at: i64,
    existing: Option<AccountReconciliationCheckpoint>,
) -> Result<(), StoreError> {
    let account_refresh = result.account.as_ref().map(|_| result.observed_at);
    let symbol_refresh = completeness
        .symbol_metadata_complete
        .then_some(result.observed_at);
    let pending_refresh = (request.command_ids.is_none()
        && request.terminal_id.is_none()
        && request.client_id.is_none()
        && evaluation.disposition == ReconciliationDisposition::Completed
        && completeness.command_scope_complete)
        .then_some(result.observed_at);

    let checkpoint = match existing {
        None => AccountReconciliationCheckpoint {
            account_id: result.account_id.clone(),
            source_request_id: result.request_id.clone(),
            result_observed_at: result.observed_at,
            account_refreshed_at: account_refresh,
            positions_observed_at: result.observed_at,
            positions_set_hash: positions_set_hash.to_owned(),
            orders_observed_at: result.observed_at,
            orders_set_hash: orders_set_hash.to_owned(),
            symbol_metadata_refreshed_at: symbol_refresh,
            pending_commands_reconciled_at: pending_refresh,
            updated_at,
        },
        Some(existing) => {
            let incoming_is_source = result.observed_at > existing.result_observed_at
                || (result.observed_at == existing.result_observed_at
                    && result.request_id < existing.source_request_id);
            let source_request_id = if incoming_is_source {
                result.request_id.clone()
            } else {
                existing.source_request_id.clone()
            };
            let (positions_observed_at, positions_set_hash) =
                if result.observed_at > existing.positions_observed_at {
                    (result.observed_at, positions_set_hash.to_owned())
                } else {
                    (
                        existing.positions_observed_at,
                        existing.positions_set_hash.clone(),
                    )
                };
            let (orders_observed_at, orders_set_hash) =
                if result.observed_at > existing.orders_observed_at {
                    (result.observed_at, orders_set_hash.to_owned())
                } else {
                    (
                        existing.orders_observed_at,
                        existing.orders_set_hash.clone(),
                    )
                };
            AccountReconciliationCheckpoint {
                account_id: existing.account_id,
                source_request_id,
                result_observed_at: existing.result_observed_at.max(result.observed_at),
                account_refreshed_at: max_optional(existing.account_refreshed_at, account_refresh),
                positions_observed_at,
                positions_set_hash,
                orders_observed_at,
                orders_set_hash,
                symbol_metadata_refreshed_at: max_optional(
                    existing.symbol_metadata_refreshed_at,
                    symbol_refresh,
                ),
                pending_commands_reconciled_at: max_optional(
                    existing.pending_commands_reconciled_at,
                    pending_refresh,
                ),
                updated_at: existing.updated_at.max(updated_at),
            }
        }
    };

    sqlx::query(
        "INSERT INTO account_reconciliation_checkpoints (\
            account_id, source_request_id, result_observed_at, account_refreshed_at, \
            positions_observed_at, positions_set_hash, orders_observed_at, orders_set_hash, \
            symbol_metadata_refreshed_at, pending_commands_reconciled_at, updated_at\
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT(account_id) DO UPDATE SET \
            source_request_id = excluded.source_request_id, \
            result_observed_at = excluded.result_observed_at, \
            account_refreshed_at = excluded.account_refreshed_at, \
            positions_observed_at = excluded.positions_observed_at, \
            positions_set_hash = excluded.positions_set_hash, \
            orders_observed_at = excluded.orders_observed_at, \
            orders_set_hash = excluded.orders_set_hash, \
            symbol_metadata_refreshed_at = excluded.symbol_metadata_refreshed_at, \
            pending_commands_reconciled_at = excluded.pending_commands_reconciled_at, \
            updated_at = excluded.updated_at",
    )
    .bind(checkpoint.account_id.as_str())
    .bind(checkpoint.source_request_id.as_str())
    .bind(checkpoint.result_observed_at)
    .bind(checkpoint.account_refreshed_at)
    .bind(checkpoint.positions_observed_at)
    .bind(&checkpoint.positions_set_hash)
    .bind(checkpoint.orders_observed_at)
    .bind(&checkpoint.orders_set_hash)
    .bind(checkpoint.symbol_metadata_refreshed_at)
    .bind(checkpoint.pending_commands_reconciled_at)
    .bind(checkpoint.updated_at)
    .execute(connection)
    .await?;
    Ok(())
}

fn max_optional(left: Option<i64>, right: Option<i64>) -> Option<i64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

async fn validate_checkpoint_integrity(
    connection: &mut SqliteConnection,
    checkpoint: &AccountReconciliationCheckpoint,
) -> Result<(), StoreError> {
    let key = checkpoint.account_id.to_string();
    let bounded = [
        checkpoint.account_refreshed_at,
        Some(checkpoint.positions_observed_at),
        Some(checkpoint.orders_observed_at),
        checkpoint.symbol_metadata_refreshed_at,
        checkpoint.pending_commands_reconciled_at,
    ];
    if checkpoint.result_observed_at < 0
        || checkpoint.updated_at < checkpoint.result_observed_at
        || checkpoint.positions_observed_at != checkpoint.result_observed_at
        || checkpoint.orders_observed_at != checkpoint.result_observed_at
        || bounded
            .into_iter()
            .flatten()
            .any(|value| value < 0 || value > checkpoint.result_observed_at)
        || !is_lower_hex_64(&checkpoint.positions_set_hash)
        || !is_lower_hex_64(&checkpoint.orders_set_hash)
    {
        return Err(StoreError::corrupt(
            "account_reconciliation_checkpoint",
            &key,
            "timestamps or full-set hashes are invalid",
        ));
    }

    let source = fetch_reconciliation_run_on(connection, &checkpoint.source_request_id)
        .await?
        .ok_or_else(|| {
            StoreError::corrupt(
                "account_reconciliation_checkpoint",
                &key,
                "source reconciliation run is missing",
            )
        })?;
    if source.request.account_id != checkpoint.account_id
        || source.result.as_ref().map(|result| result.observed_at)
            != Some(checkpoint.result_observed_at)
    {
        return Err(StoreError::corrupt(
            "account_reconciliation_checkpoint",
            &key,
            "source run does not match checkpoint account and result watermark",
        ));
    }

    validate_checkpoint_full_set_evidence(checkpoint, &source)?;
    validate_checkpoint_readiness_evidence(connection, checkpoint).await?;
    validate_position_membership(connection, checkpoint).await?;
    validate_order_membership(connection, checkpoint).await
}

fn validate_checkpoint_full_set_evidence(
    checkpoint: &AccountReconciliationCheckpoint,
    source: &StoredReconciliationRun,
) -> Result<(), StoreError> {
    let Some(result) = source.result.as_ref() else {
        return Err(StoreError::corrupt(
            "account_reconciliation_checkpoint",
            checkpoint.account_id.to_string(),
            "source reconciliation run has no durable result",
        ));
    };
    let positions = CanonicalJson::from_serializable(&result.positions).map_err(|error| {
        StoreError::corrupt(
            "account_reconciliation_checkpoint",
            checkpoint.account_id.to_string(),
            format!("source position full set cannot be canonicalized: {error}"),
        )
    })?;
    let orders = CanonicalJson::from_serializable(&result.orders).map_err(|error| {
        StoreError::corrupt(
            "account_reconciliation_checkpoint",
            checkpoint.account_id.to_string(),
            format!("source order full set cannot be canonicalized: {error}"),
        )
    })?;
    if positions.sha256_hex() != checkpoint.positions_set_hash
        || orders.sha256_hex() != checkpoint.orders_set_hash
    {
        return Err(StoreError::corrupt(
            "account_reconciliation_checkpoint",
            checkpoint.account_id.to_string(),
            "full-set hashes do not match the source reconciliation result",
        ));
    }
    Ok(())
}

async fn validate_checkpoint_readiness_evidence(
    connection: &mut SqliteConnection,
    checkpoint: &AccountReconciliationCheckpoint,
) -> Result<(), StoreError> {
    if let Some(observed_at) = checkpoint.account_refreshed_at {
        let runs =
            fetch_reconciliation_runs_at(connection, &checkpoint.account_id, observed_at).await?;
        if !runs.iter().any(|run| {
            run.result
                .as_ref()
                .is_some_and(|result| result.account.is_some())
        }) {
            return Err(StoreError::corrupt(
                "account_reconciliation_checkpoint",
                checkpoint.account_id.to_string(),
                "account_refreshed_at has no durable account snapshot evidence",
            ));
        }
    }
    if let Some(observed_at) = checkpoint.symbol_metadata_refreshed_at {
        let runs =
            fetch_reconciliation_runs_at(connection, &checkpoint.account_id, observed_at).await?;
        if !runs.iter().any(|run| {
            run.completeness
                .is_some_and(|completeness| completeness.symbol_metadata_complete)
        }) {
            return Err(StoreError::corrupt(
                "account_reconciliation_checkpoint",
                checkpoint.account_id.to_string(),
                "symbol_metadata_refreshed_at has no explicit completeness evidence",
            ));
        }
    }
    if let Some(observed_at) = checkpoint.pending_commands_reconciled_at {
        let runs =
            fetch_reconciliation_runs_at(connection, &checkpoint.account_id, observed_at).await?;
        if !runs.iter().any(|run| {
            run.status == ReconciliationRunStatus::Completed
                && run.request.command_ids.is_none()
                && run.request.terminal_id.is_none()
                && run.request.client_id.is_none()
                && run.result_evaluation.as_ref().is_some_and(|evaluation| {
                    evaluation.disposition == ReconciliationDisposition::Completed
                })
                && run
                    .completeness
                    .is_some_and(|completeness| completeness.command_scope_complete)
        }) {
            return Err(StoreError::corrupt(
                "account_reconciliation_checkpoint",
                checkpoint.account_id.to_string(),
                "pending_commands_reconciled_at has no account-wide completeness evidence",
            ));
        }
    }
    Ok(())
}

async fn fetch_reconciliation_runs_at(
    connection: &mut SqliteConnection,
    account_id: &AccountId,
    observed_at: i64,
) -> Result<Vec<StoredReconciliationRun>, StoreError> {
    let request_ids: Vec<String> = sqlx::query_scalar(
        "SELECT request_id FROM reconciliation_runs \
         WHERE account_id = ? AND result_observed_at = ? ORDER BY request_id",
    )
    .bind(account_id.as_str())
    .bind(observed_at)
    .fetch_all(&mut *connection)
    .await?;
    let mut runs = Vec::with_capacity(request_ids.len());
    for request_id in request_ids {
        let request_id = RequestId::from(request_id);
        let run = fetch_reconciliation_run_on(connection, &request_id)
            .await?
            .ok_or_else(|| {
                StoreError::corrupt(
                    "account_reconciliation_checkpoint",
                    account_id.to_string(),
                    "readiness source run disappeared",
                )
            })?;
        runs.push(run);
    }
    Ok(runs)
}

async fn validate_position_membership(
    connection: &mut SqliteConnection,
    checkpoint: &AccountReconciliationCheckpoint,
) -> Result<(), StoreError> {
    let rows = sqlx::query(
        "SELECT set_observed_at, position_id, payload_json, payload_hash \
         FROM reconciliation_position_set_members WHERE account_id = ? ORDER BY position_id",
    )
    .bind(checkpoint.account_id.as_str())
    .fetch_all(&mut *connection)
    .await?;
    let mut values = Vec::with_capacity(rows.len());
    let mut member_ids = BTreeSet::new();
    for row in rows {
        let position_id: String = row.try_get("position_id")?;
        let member_key = format!("{}:{position_id}", checkpoint.account_id);
        let set_observed_at: i64 = row.try_get("set_observed_at")?;
        let payload = CanonicalJson::from_stored(
            "reconciliation_position_set_member",
            &member_key,
            row.try_get("payload_json")?,
            row.try_get("payload_hash")?,
        )?;
        let value: PositionSnapshot =
            decode_canonical("reconciliation_position_set_member", &member_key, &payload)?;
        if set_observed_at != checkpoint.positions_observed_at
            || value.observed_at != set_observed_at
            || value.account_id != checkpoint.account_id
            || value.position_id.as_str() != position_id
        {
            return Err(StoreError::corrupt(
                "reconciliation_position_set_member",
                member_key,
                "membership aliases do not match payload or checkpoint",
            ));
        }
        let latest: Option<(i64, String)> = sqlx::query_as(
            "SELECT observed_at, payload_hash FROM position_snapshots_latest \
             WHERE account_id = ? AND position_id = ?",
        )
        .bind(checkpoint.account_id.as_str())
        .bind(&position_id)
        .fetch_optional(&mut *connection)
        .await?;
        if latest.is_none_or(|(at, hash)| {
            at < set_observed_at || (at == set_observed_at && hash != payload.sha256_hex())
        }) {
            return Err(StoreError::corrupt(
                "reconciliation_position_set_member",
                member_key,
                "latest projection disagrees with full-set membership",
            ));
        }
        member_ids.insert(position_id);
        values.push(value);
    }
    let set = CanonicalJson::from_serializable(&values)?;
    if set.sha256_hex() != checkpoint.positions_set_hash {
        return Err(StoreError::corrupt(
            "account_reconciliation_checkpoint",
            checkpoint.account_id.to_string(),
            "positions_set_hash does not match membership",
        ));
    }
    let live_at_or_before: Vec<String> = sqlx::query_scalar(
        "SELECT position_id FROM position_snapshots_latest \
         WHERE account_id = ? AND observed_at <= ? ORDER BY position_id",
    )
    .bind(checkpoint.account_id.as_str())
    .bind(checkpoint.positions_observed_at)
    .fetch_all(&mut *connection)
    .await?;
    if live_at_or_before
        .iter()
        .any(|position_id| !member_ids.contains(position_id))
    {
        return Err(StoreError::corrupt(
            "account_reconciliation_checkpoint",
            checkpoint.account_id.to_string(),
            "position projection contains a row tombstoned by the full set",
        ));
    }
    Ok(())
}

async fn validate_order_membership(
    connection: &mut SqliteConnection,
    checkpoint: &AccountReconciliationCheckpoint,
) -> Result<(), StoreError> {
    let rows = sqlx::query(
        "SELECT set_observed_at, broker_order_id, payload_json, payload_hash \
         FROM reconciliation_order_set_members WHERE account_id = ? ORDER BY broker_order_id",
    )
    .bind(checkpoint.account_id.as_str())
    .fetch_all(&mut *connection)
    .await?;
    let mut values = Vec::with_capacity(rows.len());
    let mut member_ids = BTreeSet::new();
    for row in rows {
        let order_id: String = row.try_get("broker_order_id")?;
        let member_key = format!("{}:{order_id}", checkpoint.account_id);
        let set_observed_at: i64 = row.try_get("set_observed_at")?;
        let payload = CanonicalJson::from_stored(
            "reconciliation_order_set_member",
            &member_key,
            row.try_get("payload_json")?,
            row.try_get("payload_hash")?,
        )?;
        let value: OrderSnapshot =
            decode_canonical("reconciliation_order_set_member", &member_key, &payload)?;
        if set_observed_at != checkpoint.orders_observed_at
            || value.observed_at != set_observed_at
            || value.account_id != checkpoint.account_id
            || value.broker_order_id.as_str() != order_id
        {
            return Err(StoreError::corrupt(
                "reconciliation_order_set_member",
                member_key,
                "membership aliases do not match payload or checkpoint",
            ));
        }
        let latest: Option<(i64, String)> = sqlx::query_as(
            "SELECT observed_at, payload_hash FROM order_snapshots_latest \
             WHERE account_id = ? AND broker_order_id = ?",
        )
        .bind(checkpoint.account_id.as_str())
        .bind(&order_id)
        .fetch_optional(&mut *connection)
        .await?;
        if latest.is_none_or(|(at, hash)| {
            at < set_observed_at || (at == set_observed_at && hash != payload.sha256_hex())
        }) {
            return Err(StoreError::corrupt(
                "reconciliation_order_set_member",
                member_key,
                "latest projection disagrees with full-set membership",
            ));
        }
        member_ids.insert(order_id);
        values.push(value);
    }
    let set = CanonicalJson::from_serializable(&values)?;
    if set.sha256_hex() != checkpoint.orders_set_hash {
        return Err(StoreError::corrupt(
            "account_reconciliation_checkpoint",
            checkpoint.account_id.to_string(),
            "orders_set_hash does not match membership",
        ));
    }
    let live_at_or_before: Vec<String> = sqlx::query_scalar(
        "SELECT broker_order_id FROM order_snapshots_latest \
         WHERE account_id = ? AND observed_at <= ? ORDER BY broker_order_id",
    )
    .bind(checkpoint.account_id.as_str())
    .bind(checkpoint.orders_observed_at)
    .fetch_all(&mut *connection)
    .await?;
    if live_at_or_before
        .iter()
        .any(|order_id| !member_ids.contains(order_id))
    {
        return Err(StoreError::corrupt(
            "account_reconciliation_checkpoint",
            checkpoint.account_id.to_string(),
            "order projection contains a row tombstoned by the full set",
        ));
    }
    Ok(())
}

fn is_lower_hex_64(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(crate) async fn rebuild_reconciliation_projections_on(
    connection: &mut SqliteConnection,
) -> Result<ReconciliationProjectionRebuildReport, StoreError> {
    validate_all_durable_snapshot_full_set_consistency(connection).await?;
    let event_ids: Vec<String> = sqlx::query_scalar(
        "SELECT event_id FROM core_events \
         WHERE event_type IN (\
            'account.snapshot', 'symbol.metadata', 'position.snapshot', 'order.snapshot', \
            'reconciliation.result'\
         ) ORDER BY received_at, created_at, event_id",
    )
    .fetch_all(&mut *connection)
    .await?;

    for table in [
        "reconciliation_position_set_members",
        "reconciliation_order_set_members",
        "account_reconciliation_checkpoints",
        "account_snapshots_latest",
        "symbol_metadata_latest",
        "position_snapshots_latest",
        "order_snapshots_latest",
    ] {
        sqlx::query(&format!("DELETE FROM {table}"))
            .execute(&mut *connection)
            .await?;
    }

    replay_reconciliation_event_ids(connection, event_ids).await
}

async fn rebuild_reconciliation_account_projections_on(
    connection: &mut SqliteConnection,
    account_id: &AccountId,
) -> Result<ReconciliationProjectionRebuildReport, StoreError> {
    validate_account_durable_snapshot_full_set_consistency(connection, account_id).await?;
    let event_ids: Vec<String> = sqlx::query_scalar(
        "SELECT event_id FROM core_events \
         WHERE account_id = ? AND event_type IN (\
            'account.snapshot', 'symbol.metadata', 'position.snapshot', 'order.snapshot', \
            'reconciliation.result'\
         ) ORDER BY received_at, created_at, event_id",
    )
    .bind(account_id.as_str())
    .fetch_all(&mut *connection)
    .await?;
    for table in [
        "reconciliation_position_set_members",
        "reconciliation_order_set_members",
        "account_reconciliation_checkpoints",
        "account_snapshots_latest",
        "symbol_metadata_latest",
        "position_snapshots_latest",
        "order_snapshots_latest",
    ] {
        sqlx::query(&format!("DELETE FROM {table} WHERE account_id = ?"))
            .bind(account_id.as_str())
            .execute(&mut *connection)
            .await?;
    }
    replay_reconciliation_event_ids(connection, event_ids).await
}

async fn replay_reconciliation_event_ids(
    connection: &mut SqliteConnection,
    event_ids: Vec<String>,
) -> Result<ReconciliationProjectionRebuildReport, StoreError> {
    let mut report = ReconciliationProjectionRebuildReport::default();
    for event_id in event_ids {
        let event = fetch_core_event_by_id(&mut *connection, &event_id)
            .await?
            .ok_or_else(|| StoreError::corrupt("core_event", &event_id, "event disappeared"))?;
        let account_id = event.metadata.account_id.as_ref().ok_or_else(|| {
            StoreError::corrupt(
                "core_event",
                &event_id,
                "account_id is required for reconciliation projection rebuild",
            )
        })?;
        match event.metadata.event_type.as_str() {
            "account.snapshot" => {
                let value: AccountSnapshot =
                    decode_canonical("core_event", &event_id, &event.payload)?;
                ensure_rebuild_account(&event_id, account_id, &value.account_id)?;
                apply_account(
                    connection,
                    &value,
                    &event.payload,
                    event.metadata.received_at,
                )
                .await?;
                report.replayed_snapshot_facts += 1;
            }
            "symbol.metadata" => {
                let value: SymbolMetadataSnapshot =
                    decode_canonical("core_event", &event_id, &event.payload)?;
                ensure_rebuild_account(&event_id, account_id, &value.account_id)?;
                apply_symbol(
                    connection,
                    &value,
                    &event.payload,
                    event.metadata.received_at,
                )
                .await?;
                report.replayed_snapshot_facts += 1;
            }
            "position.snapshot" => {
                let value: PositionSnapshot =
                    decode_canonical("core_event", &event_id, &event.payload)?;
                ensure_rebuild_account(&event_id, account_id, &value.account_id)?;
                apply_position(
                    connection,
                    &value,
                    &event.payload,
                    event.metadata.received_at,
                )
                .await?;
                report.replayed_snapshot_facts += 1;
            }
            "order.snapshot" => {
                let value: OrderSnapshot =
                    decode_canonical("core_event", &event_id, &event.payload)?;
                ensure_rebuild_account(&event_id, account_id, &value.account_id)?;
                apply_order(
                    connection,
                    &value,
                    &event.payload,
                    event.metadata.received_at,
                )
                .await?;
                report.replayed_snapshot_facts += 1;
            }
            RESULT_EVENT_TYPE => {
                let result: ReconciliationResult =
                    decode_canonical("core_event", &event_id, &event.payload)?;
                let run = fetch_reconciliation_run_on(connection, &result.request_id)
                    .await?
                    .ok_or_else(|| {
                        StoreError::corrupt(
                            "core_event",
                            &event_id,
                            "reconciliation result has no durable run",
                        )
                    })?;
                if run.result.as_ref() != Some(&result)
                    || run
                        .result_event
                        .as_ref()
                        .is_none_or(|stored| stored.metadata.event_id != event_id)
                {
                    return Err(StoreError::corrupt(
                        "core_event",
                        &event_id,
                        "reconciliation result does not match run aliases",
                    ));
                }
                let evaluation = run.result_evaluation.as_ref().ok_or_else(|| {
                    StoreError::corrupt(
                        "reconciliation_run",
                        result.request_id.to_string(),
                        "result evaluation is missing",
                    )
                })?;
                let completeness = run.completeness.ok_or_else(|| {
                    StoreError::corrupt(
                        "reconciliation_run",
                        result.request_id.to_string(),
                        "result completeness is missing",
                    )
                })?;
                apply_result_projections(
                    connection,
                    &run.request,
                    &result,
                    evaluation,
                    completeness,
                    event.metadata.received_at,
                )
                .await?;
                report.replayed_reconciliation_results += 1;
            }
            _ => {}
        }
    }
    Ok(report)
}

fn ensure_rebuild_account(
    event_id: &str,
    metadata_account_id: &AccountId,
    payload_account_id: &AccountId,
) -> Result<(), StoreError> {
    if metadata_account_id == payload_account_id {
        Ok(())
    } else {
        Err(StoreError::corrupt(
            "core_event",
            event_id,
            "payload account_id does not match event metadata",
        ))
    }
}
