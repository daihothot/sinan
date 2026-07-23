use std::str::FromStr;

use sha2::{Digest, Sha256};
use sinan_types::{
    AccountId, CausationId, ClientId, CorrelationId, ErrorCode, MessageId, RequestId, SessionId,
    TerminalId,
};
use sqlx::{Row, SqliteConnection};

use crate::{CanonicalJson, SqliteStateStore, StoreError, WriteTransaction};

const INBOUND_COLUMNS: &str = "message_id, session_id, client_id, account_id, terminal_id, \
    message_type, schema_version, sequence, correlation_id, causation_id, envelope_json, \
    envelope_hash, raw_payload_length, received_at, status, lease_owner, lease_expires_at, \
    revision, finished_at, last_error, created_at, updated_at";
const REJECTION_COLUMNS: &str = "rejection_id, message_id, session_id, client_id, account_id, \
    terminal_id, message_type, schema_version, sequence, correlation_id, causation_id, \
    envelope_json, envelope_hash, reason, received_at, created_at";
const RESUME_COLUMNS: &str = "hello_message_id, session_id, client_id, account_id, terminal_id, \
    cursor_json, cursor_hash, received_at, status, lease_owner, lease_expires_at, revision, \
    reconciliation_request_id, finished_at, last_error, created_at, updated_at";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurableWorkStatus {
    Pending,
    Processing,
    Handled,
    Failed,
}

impl DurableWorkStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Processing => "PROCESSING",
            Self::Handled => "HANDLED",
            Self::Failed => "FAILED",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewInboundAdmission {
    pub message_id: MessageId,
    pub session_id: SessionId,
    pub client_id: ClientId,
    pub account_id: AccountId,
    pub terminal_id: Option<TerminalId>,
    pub message_type: String,
    pub schema_version: String,
    pub sequence: u64,
    pub correlation_id: Option<CorrelationId>,
    pub causation_id: Option<CausationId>,
    pub envelope: CanonicalJson,
    pub raw_payload_length: Option<u64>,
    pub received_at: i64,
    pub created_at: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredInboundAdmission {
    pub message_id: MessageId,
    pub session_id: SessionId,
    pub client_id: ClientId,
    pub account_id: AccountId,
    pub terminal_id: Option<TerminalId>,
    pub message_type: String,
    pub schema_version: String,
    pub sequence: u64,
    pub correlation_id: Option<CorrelationId>,
    pub causation_id: Option<CausationId>,
    pub envelope: CanonicalJson,
    pub raw_payload_length: Option<u64>,
    pub received_at: i64,
    pub status: DurableWorkStatus,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<i64>,
    pub revision: u64,
    pub finished_at: Option<i64>,
    pub last_error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredInboundRejection {
    pub rejection_id: String,
    pub message_id: MessageId,
    pub session_id: SessionId,
    pub client_id: ClientId,
    pub account_id: AccountId,
    pub terminal_id: Option<TerminalId>,
    pub message_type: String,
    pub schema_version: String,
    pub sequence: u64,
    pub correlation_id: Option<CorrelationId>,
    pub causation_id: Option<CausationId>,
    pub envelope: CanonicalJson,
    pub reason: ErrorCode,
    pub received_at: i64,
    pub created_at: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DurableInboundAdmissionOutcome {
    Accepted(StoredInboundAdmission),
    Duplicate(StoredInboundAdmission),
    Rejected(StoredInboundRejection),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewSessionResumeAdmission {
    pub hello_message_id: MessageId,
    pub session_id: SessionId,
    pub client_id: ClientId,
    pub account_id: AccountId,
    pub terminal_id: Option<TerminalId>,
    pub cursor: CanonicalJson,
    pub received_at: i64,
    pub created_at: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredSessionResumeAdmission {
    pub hello_message_id: MessageId,
    pub session_id: SessionId,
    pub client_id: ClientId,
    pub account_id: AccountId,
    pub terminal_id: Option<TerminalId>,
    pub cursor: CanonicalJson,
    pub received_at: i64,
    pub status: DurableWorkStatus,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<i64>,
    pub revision: u64,
    pub reconciliation_request_id: Option<RequestId>,
    pub finished_at: Option<i64>,
    pub last_error: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DurableSessionResumeAdmissionOutcome {
    Accepted(StoredSessionResumeAdmission),
    Duplicate(StoredSessionResumeAdmission),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaimDurableWork {
    pub worker_id: String,
    pub claimed_at: i64,
    pub lease_expires_at: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReclaimDurableWork {
    pub worker_id: String,
    pub reclaimed_at: i64,
    pub lease_expires_at: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompleteInboundAdmission {
    pub message_id: MessageId,
    pub expected_revision: u64,
    pub worker_id: String,
    pub completed_at: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FailInboundAdmission {
    pub message_id: MessageId,
    pub expected_revision: u64,
    pub worker_id: String,
    pub failed_at: i64,
    pub error: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompleteSessionResumeAdmission {
    pub hello_message_id: MessageId,
    pub expected_revision: u64,
    pub worker_id: String,
    pub completed_at: i64,
    pub reconciliation_request_id: Option<RequestId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FailSessionResumeAdmission {
    pub hello_message_id: MessageId,
    pub expected_revision: u64,
    pub worker_id: String,
    pub failed_at: i64,
    pub error: String,
}

impl SqliteStateStore {
    pub async fn admit_inbound(
        &self,
        admission: NewInboundAdmission,
    ) -> Result<DurableInboundAdmissionOutcome, StoreError> {
        validate_new_inbound(&admission)?;
        let mut transaction = self.begin_write().await?;
        let conflicts = fetch_inbound_conflicts(transaction.connection(), &admission).await?;
        let outcome = if conflicts.is_empty() {
            insert_inbound(transaction.connection(), &admission).await?;
            DurableInboundAdmissionOutcome::Accepted(
                required_inbound(transaction.connection(), &admission.message_id).await?,
            )
        } else if conflicts.len() == 1 && same_inbound_identity(&conflicts[0], &admission) {
            DurableInboundAdmissionOutcome::Duplicate(
                conflicts.into_iter().next().expect("length checked"),
            )
        } else {
            DurableInboundAdmissionOutcome::Rejected(
                record_inbound_rejection(
                    transaction.connection(),
                    &admission,
                    ErrorCode::DuplicateIdempotencyConflict,
                )
                .await?,
            )
        };
        transaction.commit().await?;
        Ok(outcome)
    }

    pub async fn get_inbound_admission(
        &self,
        message_id: &MessageId,
    ) -> Result<Option<StoredInboundAdmission>, StoreError> {
        let mut connection = self.pool().acquire().await?;
        fetch_inbound_by_id(&mut connection, message_id).await
    }

    pub async fn get_inbound_rejection(
        &self,
        rejection_id: &str,
    ) -> Result<Option<StoredInboundRejection>, StoreError> {
        let mut connection = self.pool().acquire().await?;
        fetch_inbound_rejection_by_id(&mut connection, rejection_id).await
    }

    pub async fn claim_next_inbound(
        &self,
        claim: ClaimDurableWork,
    ) -> Result<Option<StoredInboundAdmission>, StoreError> {
        validate_claim(&claim.worker_id, claim.claimed_at, claim.lease_expires_at)?;
        let mut transaction = self.begin_write().await?;
        let candidate = fetch_next_inbound_pending(transaction.connection()).await?;
        let claimed = match candidate {
            Some(candidate) => Some(
                claim_inbound_on(
                    transaction.connection(),
                    &candidate,
                    &claim.worker_id,
                    claim.claimed_at,
                    claim.lease_expires_at,
                    false,
                )
                .await?,
            ),
            None => None,
        };
        transaction.commit().await?;
        Ok(claimed)
    }

    pub async fn reclaim_expired_inbound(
        &self,
        reclaim: ReclaimDurableWork,
    ) -> Result<Option<StoredInboundAdmission>, StoreError> {
        validate_claim(
            &reclaim.worker_id,
            reclaim.reclaimed_at,
            reclaim.lease_expires_at,
        )?;
        let mut transaction = self.begin_write().await?;
        let candidate =
            fetch_next_expired_inbound(transaction.connection(), reclaim.reclaimed_at).await?;
        let claimed = match candidate {
            Some(candidate) => Some(
                claim_inbound_on(
                    transaction.connection(),
                    &candidate,
                    &reclaim.worker_id,
                    reclaim.reclaimed_at,
                    reclaim.lease_expires_at,
                    true,
                )
                .await?,
            ),
            None => None,
        };
        transaction.commit().await?;
        Ok(claimed)
    }

    pub async fn complete_inbound(
        &self,
        completion: CompleteInboundAdmission,
    ) -> Result<StoredInboundAdmission, StoreError> {
        let mut transaction = self.begin_write().await?;
        let stored = transaction.complete_inbound(completion).await?;
        transaction.commit().await?;
        Ok(stored)
    }

    pub async fn fail_inbound(
        &self,
        failure: FailInboundAdmission,
    ) -> Result<StoredInboundAdmission, StoreError> {
        let mut transaction = self.begin_write().await?;
        let stored = transaction.fail_inbound(failure).await?;
        transaction.commit().await?;
        Ok(stored)
    }

    pub async fn admit_session_resume(
        &self,
        admission: NewSessionResumeAdmission,
    ) -> Result<DurableSessionResumeAdmissionOutcome, StoreError> {
        validate_new_resume(&admission)?;
        let mut transaction = self.begin_write().await?;
        let conflicts = fetch_resume_conflicts(transaction.connection(), &admission).await?;
        let outcome = if conflicts.is_empty() {
            insert_resume(transaction.connection(), &admission).await?;
            DurableSessionResumeAdmissionOutcome::Accepted(
                required_resume(transaction.connection(), &admission.hello_message_id).await?,
            )
        } else if conflicts.len() == 1 && same_resume_identity(&conflicts[0], &admission) {
            DurableSessionResumeAdmissionOutcome::Duplicate(
                conflicts.into_iter().next().expect("length checked"),
            )
        } else {
            return Err(StoreError::conflict(
                "session_resume_admission",
                admission.hello_message_id.to_string(),
            ));
        };
        transaction.commit().await?;
        Ok(outcome)
    }

    pub async fn get_session_resume_admission(
        &self,
        hello_message_id: &MessageId,
    ) -> Result<Option<StoredSessionResumeAdmission>, StoreError> {
        let mut connection = self.pool().acquire().await?;
        fetch_resume_by_id(&mut connection, hello_message_id).await
    }

    pub async fn claim_next_session_resume(
        &self,
        claim: ClaimDurableWork,
    ) -> Result<Option<StoredSessionResumeAdmission>, StoreError> {
        validate_claim(&claim.worker_id, claim.claimed_at, claim.lease_expires_at)?;
        let mut transaction = self.begin_write().await?;
        let candidate = fetch_next_resume_pending(transaction.connection()).await?;
        let claimed = match candidate {
            Some(candidate) => Some(
                claim_resume_on(
                    transaction.connection(),
                    &candidate,
                    &claim.worker_id,
                    claim.claimed_at,
                    claim.lease_expires_at,
                    false,
                )
                .await?,
            ),
            None => None,
        };
        transaction.commit().await?;
        Ok(claimed)
    }

    pub async fn reclaim_expired_session_resume(
        &self,
        reclaim: ReclaimDurableWork,
    ) -> Result<Option<StoredSessionResumeAdmission>, StoreError> {
        validate_claim(
            &reclaim.worker_id,
            reclaim.reclaimed_at,
            reclaim.lease_expires_at,
        )?;
        let mut transaction = self.begin_write().await?;
        let candidate =
            fetch_next_expired_resume(transaction.connection(), reclaim.reclaimed_at).await?;
        let claimed = match candidate {
            Some(candidate) => Some(
                claim_resume_on(
                    transaction.connection(),
                    &candidate,
                    &reclaim.worker_id,
                    reclaim.reclaimed_at,
                    reclaim.lease_expires_at,
                    true,
                )
                .await?,
            ),
            None => None,
        };
        transaction.commit().await?;
        Ok(claimed)
    }

    pub async fn complete_session_resume(
        &self,
        completion: CompleteSessionResumeAdmission,
    ) -> Result<StoredSessionResumeAdmission, StoreError> {
        let mut transaction = self.begin_write().await?;
        let stored = transaction.complete_session_resume(completion).await?;
        transaction.commit().await?;
        Ok(stored)
    }

    pub async fn fail_session_resume(
        &self,
        failure: FailSessionResumeAdmission,
    ) -> Result<StoredSessionResumeAdmission, StoreError> {
        let mut transaction = self.begin_write().await?;
        let stored = transaction.fail_session_resume(failure).await?;
        transaction.commit().await?;
        Ok(stored)
    }
}

impl WriteTransaction {
    pub async fn complete_inbound(
        &mut self,
        completion: CompleteInboundAdmission,
    ) -> Result<StoredInboundAdmission, StoreError> {
        finish_inbound_on(
            self.connection(),
            &completion.message_id,
            completion.expected_revision,
            &completion.worker_id,
            completion.completed_at,
            None,
        )
        .await?;
        required_inbound(self.connection(), &completion.message_id).await
    }

    pub async fn fail_inbound(
        &mut self,
        failure: FailInboundAdmission,
    ) -> Result<StoredInboundAdmission, StoreError> {
        require_non_empty("inbound failure error", &failure.error)?;
        finish_inbound_on(
            self.connection(),
            &failure.message_id,
            failure.expected_revision,
            &failure.worker_id,
            failure.failed_at,
            Some(&failure.error),
        )
        .await?;
        required_inbound(self.connection(), &failure.message_id).await
    }

    pub async fn complete_session_resume(
        &mut self,
        completion: CompleteSessionResumeAdmission,
    ) -> Result<StoredSessionResumeAdmission, StoreError> {
        finish_resume_on(
            self.connection(),
            &completion.hello_message_id,
            completion.expected_revision,
            &completion.worker_id,
            completion.completed_at,
            completion.reconciliation_request_id.as_ref(),
            None,
        )
        .await?;
        required_resume(self.connection(), &completion.hello_message_id).await
    }

    pub async fn fail_session_resume(
        &mut self,
        failure: FailSessionResumeAdmission,
    ) -> Result<StoredSessionResumeAdmission, StoreError> {
        require_non_empty("session resume failure error", &failure.error)?;
        finish_resume_on(
            self.connection(),
            &failure.hello_message_id,
            failure.expected_revision,
            &failure.worker_id,
            failure.failed_at,
            None,
            Some(&failure.error),
        )
        .await?;
        required_resume(self.connection(), &failure.hello_message_id).await
    }
}

fn validate_new_inbound(admission: &NewInboundAdmission) -> Result<(), StoreError> {
    require_non_empty("message_id", admission.message_id.as_str())?;
    require_non_empty("session_id", admission.session_id.as_str())?;
    require_non_empty("client_id", admission.client_id.as_str())?;
    require_non_empty("account_id", admission.account_id.as_str())?;
    require_non_empty("message_type", &admission.message_type)?;
    require_non_empty("schema_version", &admission.schema_version)?;
    if admission.sequence == 0 {
        return Err(StoreError::InvalidSequence {
            field: "inbound_admissions.sequence",
        });
    }
    if let Some(raw_payload_length) = admission.raw_payload_length {
        u64_to_i64("inbound_admissions.raw_payload_length", raw_payload_length)?;
    }
    validate_time("inbound admission", admission.received_at)?;
    validate_time("inbound admission", admission.created_at)?;
    validate_envelope_identity(admission)
}

fn validate_envelope_identity(admission: &NewInboundAdmission) -> Result<(), StoreError> {
    let value: serde_json::Value = serde_json::from_str(admission.envelope.as_str())?;
    let object = value.as_object().ok_or_else(|| StoreError::InvalidRecord {
        entity: "inbound_admission",
        key: admission.message_id.to_string(),
        reason: "canonical envelope must be a JSON object".to_owned(),
    })?;
    let matches = object.get("message_id").and_then(|value| value.as_str())
        == Some(admission.message_id.as_str())
        && object.get("type").and_then(|value| value.as_str())
            == Some(admission.message_type.as_str())
        && object
            .get("schema_version")
            .and_then(|value| value.as_str())
            == Some(admission.schema_version.as_str())
        && object.get("session_id").and_then(|value| value.as_str())
            == Some(admission.session_id.as_str())
        && object.get("sequence").and_then(|value| value.as_u64()) == Some(admission.sequence)
        && optional_json_string(object.get("client_id"))
            .is_none_or(|client_id| client_id == admission.client_id.as_str())
        && optional_json_string(object.get("correlation_id"))
            == admission.correlation_id.as_ref().map(CorrelationId::as_str)
        && optional_json_string(object.get("causation_id"))
            == admission.causation_id.as_ref().map(CausationId::as_str);
    if matches {
        Ok(())
    } else {
        Err(StoreError::InvalidRecord {
            entity: "inbound_admission",
            key: admission.message_id.to_string(),
            reason: "canonical envelope identity differs from indexed columns".to_owned(),
        })
    }
}

fn optional_json_string(value: Option<&serde_json::Value>) -> Option<&str> {
    value.and_then(serde_json::Value::as_str)
}

fn validate_new_resume(admission: &NewSessionResumeAdmission) -> Result<(), StoreError> {
    require_non_empty("hello_message_id", admission.hello_message_id.as_str())?;
    require_non_empty("session_id", admission.session_id.as_str())?;
    require_non_empty("client_id", admission.client_id.as_str())?;
    require_non_empty("account_id", admission.account_id.as_str())?;
    validate_time("session resume admission", admission.received_at)?;
    validate_time("session resume admission", admission.created_at)
}

fn validate_claim(worker_id: &str, at: i64, lease_expires_at: i64) -> Result<(), StoreError> {
    require_non_empty("worker_id", worker_id)?;
    validate_time("durable work claim", at)?;
    if lease_expires_at <= at {
        return Err(StoreError::InvalidRecord {
            entity: "durable_work_claim",
            key: worker_id.to_owned(),
            reason: "lease_expires_at must be later than the claim time".to_owned(),
        });
    }
    Ok(())
}

fn validate_time(entity: &'static str, value: i64) -> Result<(), StoreError> {
    if value < 0 {
        Err(StoreError::InvalidRecord {
            entity,
            key: value.to_string(),
            reason: "timestamp must be non-negative".to_owned(),
        })
    } else {
        Ok(())
    }
}

fn require_non_empty(field: &'static str, value: &str) -> Result<(), StoreError> {
    if value.trim().is_empty() {
        Err(StoreError::InvalidRecord {
            entity: "inbound_durability",
            key: field.to_owned(),
            reason: format!("{field} must not be empty"),
        })
    } else {
        Ok(())
    }
}

async fn insert_inbound(
    connection: &mut SqliteConnection,
    admission: &NewInboundAdmission,
) -> Result<(), StoreError> {
    sqlx::query(
        "INSERT INTO inbound_admissions (\
            message_id, session_id, client_id, account_id, terminal_id, message_type, \
            schema_version, sequence, correlation_id, causation_id, envelope_json, envelope_hash, \
            raw_payload_length, received_at, status, created_at, updated_at\
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?, ?)",
    )
    .bind(admission.message_id.as_str())
    .bind(admission.session_id.as_str())
    .bind(admission.client_id.as_str())
    .bind(admission.account_id.as_str())
    .bind(admission.terminal_id.as_ref().map(TerminalId::as_str))
    .bind(&admission.message_type)
    .bind(&admission.schema_version)
    .bind(u64_to_i64(
        "inbound_admissions.sequence",
        admission.sequence,
    )?)
    .bind(admission.correlation_id.as_ref().map(CorrelationId::as_str))
    .bind(admission.causation_id.as_ref().map(CausationId::as_str))
    .bind(admission.envelope.as_str())
    .bind(admission.envelope.sha256_hex())
    .bind(
        admission
            .raw_payload_length
            .map(|value| u64_to_i64("inbound_admissions.raw_payload_length", value))
            .transpose()?,
    )
    .bind(admission.received_at)
    .bind(admission.created_at)
    .bind(admission.created_at)
    .execute(&mut *connection)
    .await?;
    Ok(())
}

async fn fetch_inbound_conflicts(
    connection: &mut SqliteConnection,
    admission: &NewInboundAdmission,
) -> Result<Vec<StoredInboundAdmission>, StoreError> {
    let rows = sqlx::query(&format!(
        "SELECT {INBOUND_COLUMNS} FROM inbound_admissions \
         WHERE message_id = ? OR (session_id = ? AND sequence = ?) \
         ORDER BY message_id"
    ))
    .bind(admission.message_id.as_str())
    .bind(admission.session_id.as_str())
    .bind(u64_to_i64(
        "inbound_admissions.sequence",
        admission.sequence,
    )?)
    .fetch_all(&mut *connection)
    .await?;
    rows.into_iter().map(inbound_from_row).collect()
}

async fn fetch_inbound_by_id(
    connection: &mut SqliteConnection,
    message_id: &MessageId,
) -> Result<Option<StoredInboundAdmission>, StoreError> {
    let row = sqlx::query(&format!(
        "SELECT {INBOUND_COLUMNS} FROM inbound_admissions WHERE message_id = ?"
    ))
    .bind(message_id.as_str())
    .fetch_optional(&mut *connection)
    .await?;
    row.map(inbound_from_row).transpose()
}

async fn required_inbound(
    connection: &mut SqliteConnection,
    message_id: &MessageId,
) -> Result<StoredInboundAdmission, StoreError> {
    fetch_inbound_by_id(connection, message_id)
        .await?
        .ok_or_else(|| StoreError::NotFound {
            entity: "inbound_admission",
            key: message_id.to_string(),
        })
}

fn same_inbound_identity(stored: &StoredInboundAdmission, incoming: &NewInboundAdmission) -> bool {
    stored.message_id == incoming.message_id
        && stored.session_id == incoming.session_id
        && stored.client_id == incoming.client_id
        && stored.account_id == incoming.account_id
        && stored.terminal_id == incoming.terminal_id
        && stored.message_type == incoming.message_type
        && stored.schema_version == incoming.schema_version
        && stored.sequence == incoming.sequence
        && stored.correlation_id == incoming.correlation_id
        && stored.causation_id == incoming.causation_id
        && stored.envelope == incoming.envelope
}

async fn record_inbound_rejection(
    connection: &mut SqliteConnection,
    admission: &NewInboundAdmission,
    reason: ErrorCode,
) -> Result<StoredInboundRejection, StoreError> {
    let rejection_id = rejection_id(admission, reason);
    sqlx::query(
        "INSERT INTO inbound_rejections (\
            rejection_id, message_id, session_id, client_id, account_id, terminal_id, \
            message_type, schema_version, sequence, correlation_id, causation_id, envelope_json, \
            envelope_hash, reason, received_at, created_at\
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
         ON CONFLICT DO NOTHING",
    )
    .bind(&rejection_id)
    .bind(admission.message_id.as_str())
    .bind(admission.session_id.as_str())
    .bind(admission.client_id.as_str())
    .bind(admission.account_id.as_str())
    .bind(admission.terminal_id.as_ref().map(TerminalId::as_str))
    .bind(&admission.message_type)
    .bind(&admission.schema_version)
    .bind(u64_to_i64(
        "inbound_rejections.sequence",
        admission.sequence,
    )?)
    .bind(admission.correlation_id.as_ref().map(CorrelationId::as_str))
    .bind(admission.causation_id.as_ref().map(CausationId::as_str))
    .bind(admission.envelope.as_str())
    .bind(admission.envelope.sha256_hex())
    .bind(reason.as_str())
    .bind(admission.received_at)
    .bind(admission.created_at)
    .execute(&mut *connection)
    .await?;

    let stored = fetch_inbound_rejection_by_id(connection, &rejection_id)
        .await?
        .ok_or_else(|| StoreError::NotFound {
            entity: "inbound_rejection",
            key: rejection_id.clone(),
        })?;
    if !same_rejection_identity(&stored, admission, reason) {
        return Err(StoreError::conflict("inbound_rejection", rejection_id));
    }
    Ok(stored)
}

fn rejection_id(admission: &NewInboundAdmission, reason: ErrorCode) -> String {
    let identity = format!(
        "{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}",
        admission.message_id,
        admission.session_id,
        admission.sequence,
        admission.envelope.sha256_hex(),
        reason.as_str()
    );
    format!(
        "inbound-rejection-{:x}",
        Sha256::digest(identity.as_bytes())
    )
}

fn same_rejection_identity(
    stored: &StoredInboundRejection,
    incoming: &NewInboundAdmission,
    reason: ErrorCode,
) -> bool {
    stored.message_id == incoming.message_id
        && stored.session_id == incoming.session_id
        && stored.client_id == incoming.client_id
        && stored.account_id == incoming.account_id
        && stored.terminal_id == incoming.terminal_id
        && stored.message_type == incoming.message_type
        && stored.schema_version == incoming.schema_version
        && stored.sequence == incoming.sequence
        && stored.correlation_id == incoming.correlation_id
        && stored.causation_id == incoming.causation_id
        && stored.envelope == incoming.envelope
        && stored.reason == reason
}

async fn fetch_inbound_rejection_by_id(
    connection: &mut SqliteConnection,
    rejection_id: &str,
) -> Result<Option<StoredInboundRejection>, StoreError> {
    let row = sqlx::query(&format!(
        "SELECT {REJECTION_COLUMNS} FROM inbound_rejections WHERE rejection_id = ?"
    ))
    .bind(rejection_id)
    .fetch_optional(&mut *connection)
    .await?;
    row.map(inbound_rejection_from_row).transpose()
}

async fn fetch_next_inbound_pending(
    connection: &mut SqliteConnection,
) -> Result<Option<StoredInboundAdmission>, StoreError> {
    let row = sqlx::query(&format!(
        "SELECT {INBOUND_COLUMNS} FROM inbound_admissions \
         WHERE status = 'PENDING' ORDER BY received_at, message_id LIMIT 1"
    ))
    .fetch_optional(&mut *connection)
    .await?;
    row.map(inbound_from_row).transpose()
}

async fn fetch_next_expired_inbound(
    connection: &mut SqliteConnection,
    reclaimed_at: i64,
) -> Result<Option<StoredInboundAdmission>, StoreError> {
    let row = sqlx::query(&format!(
        "SELECT {INBOUND_COLUMNS} FROM inbound_admissions \
         WHERE status = 'PROCESSING' AND lease_expires_at <= ? \
         ORDER BY lease_expires_at, received_at, message_id LIMIT 1"
    ))
    .bind(reclaimed_at)
    .fetch_optional(&mut *connection)
    .await?;
    row.map(inbound_from_row).transpose()
}

async fn claim_inbound_on(
    connection: &mut SqliteConnection,
    candidate: &StoredInboundAdmission,
    worker_id: &str,
    claimed_at: i64,
    lease_expires_at: i64,
    reclaim: bool,
) -> Result<StoredInboundAdmission, StoreError> {
    if claimed_at < candidate.updated_at {
        return Err(StoreError::InvalidRecord {
            entity: "inbound_admission",
            key: candidate.message_id.to_string(),
            reason: "claim time predates the current revision".to_owned(),
        });
    }
    let expected_status = if reclaim { "PROCESSING" } else { "PENDING" };
    let result = sqlx::query(
        "UPDATE inbound_admissions SET \
            status = 'PROCESSING', lease_owner = ?, lease_expires_at = ?, \
            revision = revision + 1, updated_at = ? \
         WHERE message_id = ? AND status = ? AND revision = ?",
    )
    .bind(worker_id)
    .bind(lease_expires_at)
    .bind(claimed_at)
    .bind(candidate.message_id.as_str())
    .bind(expected_status)
    .bind(u64_to_i64(
        "inbound_admissions.revision",
        candidate.revision,
    )?)
    .execute(&mut *connection)
    .await?;
    if result.rows_affected() != 1 {
        return Err(StoreError::StaleWrite {
            entity: "inbound_admission",
            key: candidate.message_id.to_string(),
        });
    }
    required_inbound(connection, &candidate.message_id).await
}

async fn finish_inbound_on(
    connection: &mut SqliteConnection,
    message_id: &MessageId,
    expected_revision: u64,
    worker_id: &str,
    finished_at: i64,
    error: Option<&str>,
) -> Result<(), StoreError> {
    require_non_empty("worker_id", worker_id)?;
    validate_time("inbound completion", finished_at)?;
    let current = required_inbound(connection, message_id).await?;
    if current.status != DurableWorkStatus::Processing
        || current.revision != expected_revision
        || current.lease_owner.as_deref() != Some(worker_id)
        || finished_at < current.updated_at
        || current
            .lease_expires_at
            .is_none_or(|lease_expires_at| finished_at >= lease_expires_at)
    {
        return Err(StoreError::StaleWrite {
            entity: "inbound_admission",
            key: message_id.to_string(),
        });
    }
    let status = if error.is_some() { "FAILED" } else { "HANDLED" };
    let result = sqlx::query(
        "UPDATE inbound_admissions SET \
            status = ?, lease_owner = NULL, lease_expires_at = NULL, \
            revision = revision + 1, finished_at = ?, last_error = ?, updated_at = ? \
         WHERE message_id = ? AND status = 'PROCESSING' AND revision = ? AND lease_owner = ? \
           AND lease_expires_at > ?",
    )
    .bind(status)
    .bind(finished_at)
    .bind(error)
    .bind(finished_at)
    .bind(message_id.as_str())
    .bind(u64_to_i64(
        "inbound_admissions.revision",
        expected_revision,
    )?)
    .bind(worker_id)
    .bind(finished_at)
    .execute(&mut *connection)
    .await?;
    if result.rows_affected() != 1 {
        return Err(StoreError::StaleWrite {
            entity: "inbound_admission",
            key: message_id.to_string(),
        });
    }
    Ok(())
}

async fn insert_resume(
    connection: &mut SqliteConnection,
    admission: &NewSessionResumeAdmission,
) -> Result<(), StoreError> {
    sqlx::query(
        "INSERT INTO session_resume_admissions (\
            hello_message_id, session_id, client_id, account_id, terminal_id, cursor_json, \
            cursor_hash, received_at, status, created_at, updated_at\
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'PENDING', ?, ?)",
    )
    .bind(admission.hello_message_id.as_str())
    .bind(admission.session_id.as_str())
    .bind(admission.client_id.as_str())
    .bind(admission.account_id.as_str())
    .bind(admission.terminal_id.as_ref().map(TerminalId::as_str))
    .bind(admission.cursor.as_str())
    .bind(admission.cursor.sha256_hex())
    .bind(admission.received_at)
    .bind(admission.created_at)
    .bind(admission.created_at)
    .execute(&mut *connection)
    .await?;
    Ok(())
}

async fn fetch_resume_conflicts(
    connection: &mut SqliteConnection,
    admission: &NewSessionResumeAdmission,
) -> Result<Vec<StoredSessionResumeAdmission>, StoreError> {
    let rows = sqlx::query(&format!(
        "SELECT {RESUME_COLUMNS} FROM session_resume_admissions \
         WHERE hello_message_id = ? OR session_id = ? ORDER BY hello_message_id"
    ))
    .bind(admission.hello_message_id.as_str())
    .bind(admission.session_id.as_str())
    .fetch_all(&mut *connection)
    .await?;
    rows.into_iter().map(resume_from_row).collect()
}

async fn fetch_resume_by_id(
    connection: &mut SqliteConnection,
    hello_message_id: &MessageId,
) -> Result<Option<StoredSessionResumeAdmission>, StoreError> {
    let row = sqlx::query(&format!(
        "SELECT {RESUME_COLUMNS} FROM session_resume_admissions WHERE hello_message_id = ?"
    ))
    .bind(hello_message_id.as_str())
    .fetch_optional(&mut *connection)
    .await?;
    row.map(resume_from_row).transpose()
}

async fn required_resume(
    connection: &mut SqliteConnection,
    hello_message_id: &MessageId,
) -> Result<StoredSessionResumeAdmission, StoreError> {
    fetch_resume_by_id(connection, hello_message_id)
        .await?
        .ok_or_else(|| StoreError::NotFound {
            entity: "session_resume_admission",
            key: hello_message_id.to_string(),
        })
}

fn same_resume_identity(
    stored: &StoredSessionResumeAdmission,
    incoming: &NewSessionResumeAdmission,
) -> bool {
    stored.hello_message_id == incoming.hello_message_id
        && stored.session_id == incoming.session_id
        && stored.client_id == incoming.client_id
        && stored.account_id == incoming.account_id
        && stored.terminal_id == incoming.terminal_id
        && stored.cursor == incoming.cursor
}

async fn fetch_next_resume_pending(
    connection: &mut SqliteConnection,
) -> Result<Option<StoredSessionResumeAdmission>, StoreError> {
    let row = sqlx::query(&format!(
        "SELECT {RESUME_COLUMNS} FROM session_resume_admissions \
         WHERE status = 'PENDING' ORDER BY received_at, hello_message_id LIMIT 1"
    ))
    .fetch_optional(&mut *connection)
    .await?;
    row.map(resume_from_row).transpose()
}

async fn fetch_next_expired_resume(
    connection: &mut SqliteConnection,
    reclaimed_at: i64,
) -> Result<Option<StoredSessionResumeAdmission>, StoreError> {
    let row = sqlx::query(&format!(
        "SELECT {RESUME_COLUMNS} FROM session_resume_admissions \
         WHERE status = 'PROCESSING' AND lease_expires_at <= ? \
         ORDER BY lease_expires_at, received_at, hello_message_id LIMIT 1"
    ))
    .bind(reclaimed_at)
    .fetch_optional(&mut *connection)
    .await?;
    row.map(resume_from_row).transpose()
}

async fn claim_resume_on(
    connection: &mut SqliteConnection,
    candidate: &StoredSessionResumeAdmission,
    worker_id: &str,
    claimed_at: i64,
    lease_expires_at: i64,
    reclaim: bool,
) -> Result<StoredSessionResumeAdmission, StoreError> {
    if claimed_at < candidate.updated_at {
        return Err(StoreError::InvalidRecord {
            entity: "session_resume_admission",
            key: candidate.hello_message_id.to_string(),
            reason: "claim time predates the current revision".to_owned(),
        });
    }
    let expected_status = if reclaim { "PROCESSING" } else { "PENDING" };
    let result = sqlx::query(
        "UPDATE session_resume_admissions SET \
            status = 'PROCESSING', lease_owner = ?, lease_expires_at = ?, \
            revision = revision + 1, updated_at = ? \
         WHERE hello_message_id = ? AND status = ? AND revision = ?",
    )
    .bind(worker_id)
    .bind(lease_expires_at)
    .bind(claimed_at)
    .bind(candidate.hello_message_id.as_str())
    .bind(expected_status)
    .bind(u64_to_i64(
        "session_resume_admissions.revision",
        candidate.revision,
    )?)
    .execute(&mut *connection)
    .await?;
    if result.rows_affected() != 1 {
        return Err(StoreError::StaleWrite {
            entity: "session_resume_admission",
            key: candidate.hello_message_id.to_string(),
        });
    }
    required_resume(connection, &candidate.hello_message_id).await
}

#[allow(clippy::too_many_arguments)]
async fn finish_resume_on(
    connection: &mut SqliteConnection,
    hello_message_id: &MessageId,
    expected_revision: u64,
    worker_id: &str,
    finished_at: i64,
    reconciliation_request_id: Option<&RequestId>,
    error: Option<&str>,
) -> Result<(), StoreError> {
    require_non_empty("worker_id", worker_id)?;
    validate_time("session resume completion", finished_at)?;
    let current = required_resume(connection, hello_message_id).await?;
    if current.status != DurableWorkStatus::Processing
        || current.revision != expected_revision
        || current.lease_owner.as_deref() != Some(worker_id)
        || finished_at < current.updated_at
        || current
            .lease_expires_at
            .is_none_or(|lease_expires_at| finished_at >= lease_expires_at)
    {
        return Err(StoreError::StaleWrite {
            entity: "session_resume_admission",
            key: hello_message_id.to_string(),
        });
    }
    let status = if error.is_some() { "FAILED" } else { "HANDLED" };
    let result = sqlx::query(
        "UPDATE session_resume_admissions SET \
            status = ?, lease_owner = NULL, lease_expires_at = NULL, \
            revision = revision + 1, reconciliation_request_id = ?, finished_at = ?, \
            last_error = ?, updated_at = ? \
         WHERE hello_message_id = ? AND status = 'PROCESSING' \
           AND revision = ? AND lease_owner = ? AND lease_expires_at > ?",
    )
    .bind(status)
    .bind(reconciliation_request_id.map(RequestId::as_str))
    .bind(finished_at)
    .bind(error)
    .bind(finished_at)
    .bind(hello_message_id.as_str())
    .bind(u64_to_i64(
        "session_resume_admissions.revision",
        expected_revision,
    )?)
    .bind(worker_id)
    .bind(finished_at)
    .execute(&mut *connection)
    .await?;
    if result.rows_affected() != 1 {
        return Err(StoreError::StaleWrite {
            entity: "session_resume_admission",
            key: hello_message_id.to_string(),
        });
    }
    Ok(())
}

fn inbound_from_row(row: sqlx::sqlite::SqliteRow) -> Result<StoredInboundAdmission, StoreError> {
    let message_id: String = row.try_get("message_id")?;
    Ok(StoredInboundAdmission {
        message_id: message_id.as_str().into(),
        session_id: row.try_get::<String, _>("session_id")?.into(),
        client_id: row.try_get::<String, _>("client_id")?.into(),
        account_id: row.try_get::<String, _>("account_id")?.into(),
        terminal_id: row
            .try_get::<Option<String>, _>("terminal_id")?
            .map(TerminalId::from),
        message_type: row.try_get("message_type")?,
        schema_version: row.try_get("schema_version")?,
        sequence: i64_to_u64("inbound_admissions", &message_id, row.try_get("sequence")?)?,
        correlation_id: row
            .try_get::<Option<String>, _>("correlation_id")?
            .map(CorrelationId::from),
        causation_id: row
            .try_get::<Option<String>, _>("causation_id")?
            .map(CausationId::from),
        envelope: CanonicalJson::from_stored(
            "inbound_admission",
            &message_id,
            row.try_get("envelope_json")?,
            row.try_get("envelope_hash")?,
        )?,
        raw_payload_length: row
            .try_get::<Option<i64>, _>("raw_payload_length")?
            .map(|value| i64_to_u64("inbound_admissions", &message_id, value))
            .transpose()?,
        received_at: row.try_get("received_at")?,
        status: parse_work_status("inbound_admission", &message_id, row.try_get("status")?)?,
        lease_owner: row.try_get("lease_owner")?,
        lease_expires_at: row.try_get("lease_expires_at")?,
        revision: i64_to_u64("inbound_admissions", &message_id, row.try_get("revision")?)?,
        finished_at: row.try_get("finished_at")?,
        last_error: row.try_get("last_error")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn inbound_rejection_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> Result<StoredInboundRejection, StoreError> {
    let rejection_id: String = row.try_get("rejection_id")?;
    let reason_raw: String = row.try_get("reason")?;
    let reason = ErrorCode::from_str(&reason_raw).map_err(|_| {
        StoreError::corrupt(
            "inbound_rejection",
            &rejection_id,
            format!("unknown reason {reason_raw}"),
        )
    })?;
    Ok(StoredInboundRejection {
        rejection_id: rejection_id.clone(),
        message_id: row.try_get::<String, _>("message_id")?.into(),
        session_id: row.try_get::<String, _>("session_id")?.into(),
        client_id: row.try_get::<String, _>("client_id")?.into(),
        account_id: row.try_get::<String, _>("account_id")?.into(),
        terminal_id: row
            .try_get::<Option<String>, _>("terminal_id")?
            .map(TerminalId::from),
        message_type: row.try_get("message_type")?,
        schema_version: row.try_get("schema_version")?,
        sequence: i64_to_u64("inbound_rejection", &rejection_id, row.try_get("sequence")?)?,
        correlation_id: row
            .try_get::<Option<String>, _>("correlation_id")?
            .map(CorrelationId::from),
        causation_id: row
            .try_get::<Option<String>, _>("causation_id")?
            .map(CausationId::from),
        envelope: CanonicalJson::from_stored(
            "inbound_rejection",
            &rejection_id,
            row.try_get("envelope_json")?,
            row.try_get("envelope_hash")?,
        )?,
        reason,
        received_at: row.try_get("received_at")?,
        created_at: row.try_get("created_at")?,
    })
}

fn resume_from_row(
    row: sqlx::sqlite::SqliteRow,
) -> Result<StoredSessionResumeAdmission, StoreError> {
    let hello_message_id: String = row.try_get("hello_message_id")?;
    Ok(StoredSessionResumeAdmission {
        hello_message_id: hello_message_id.as_str().into(),
        session_id: row.try_get::<String, _>("session_id")?.into(),
        client_id: row.try_get::<String, _>("client_id")?.into(),
        account_id: row.try_get::<String, _>("account_id")?.into(),
        terminal_id: row
            .try_get::<Option<String>, _>("terminal_id")?
            .map(TerminalId::from),
        cursor: CanonicalJson::from_stored(
            "session_resume_admission",
            &hello_message_id,
            row.try_get("cursor_json")?,
            row.try_get("cursor_hash")?,
        )?,
        received_at: row.try_get("received_at")?,
        status: parse_work_status(
            "session_resume_admission",
            &hello_message_id,
            row.try_get("status")?,
        )?,
        lease_owner: row.try_get("lease_owner")?,
        lease_expires_at: row.try_get("lease_expires_at")?,
        revision: i64_to_u64(
            "session_resume_admission",
            &hello_message_id,
            row.try_get("revision")?,
        )?,
        reconciliation_request_id: row
            .try_get::<Option<String>, _>("reconciliation_request_id")?
            .map(RequestId::from),
        finished_at: row.try_get("finished_at")?,
        last_error: row.try_get("last_error")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

fn parse_work_status(
    entity: &'static str,
    key: &str,
    value: String,
) -> Result<DurableWorkStatus, StoreError> {
    match value.as_str() {
        "PENDING" => Ok(DurableWorkStatus::Pending),
        "PROCESSING" => Ok(DurableWorkStatus::Processing),
        "HANDLED" => Ok(DurableWorkStatus::Handled),
        "FAILED" => Ok(DurableWorkStatus::Failed),
        _ => Err(StoreError::corrupt(
            entity,
            key,
            format!("unknown durable work status {value}"),
        )),
    }
}

fn u64_to_i64(field: &'static str, value: u64) -> Result<i64, StoreError> {
    i64::try_from(value).map_err(|_| StoreError::InvalidInteger { field, value })
}

fn i64_to_u64(entity: &'static str, key: &str, value: i64) -> Result<u64, StoreError> {
    u64::try_from(value)
        .map_err(|_| StoreError::corrupt(entity, key, format!("negative integer value {value}")))
}
