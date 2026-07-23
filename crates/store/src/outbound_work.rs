//! Durable scheduling for business-owned outbound delivery work.

use sinan_types::{
    CausationId, CommandId, CorrelationId, ExecutionCommand, ExecutionCommandState, IntentId,
    MessageId, RequestId,
};
use sqlx::{Row, SqliteConnection};

use crate::{
    connection::{SqliteStateStore, WriteTransaction},
    reconciliation::{fetch_reconciliation_run_on, StoredReconciliationRun},
    repository::{fetch_execution_command_by_id, fetch_trade_intent_by_id},
    StoreError, StoredExecutionCommand,
};

const WORK_COLUMNS: &str = "work_id, command_id, request_id, generation, message_id, status, \
    delivery_attempts, next_attempt_at, lease_owner, lease_expires_at, revision, last_outcome, \
    last_error, completed_at, created_at, updated_at";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutboundDeliveryWorkStatus {
    Pending,
    Processing,
    Delivered,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutboundDeliveryWorkOutcome {
    Sent,
    Unconfirmed,
    Rejected,
    DefinitelyNotWritten,
    InfrastructureError,
    Superseded,
    Expired,
    PermanentRejection,
}

impl OutboundDeliveryWorkOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Sent => "SENT",
            Self::Unconfirmed => "UNCONFIRMED",
            Self::Rejected => "REJECTED",
            Self::DefinitelyNotWritten => "DEFINITELY_NOT_WRITTEN",
            Self::InfrastructureError => "INFRASTRUCTURE_ERROR",
            Self::Superseded => "SUPERSEDED",
            Self::Expired => "EXPIRED",
            Self::PermanentRejection => "PERMANENT_REJECTION",
        }
    }

    const fn may_complete(self) -> bool {
        matches!(
            self,
            Self::Sent
                | Self::Unconfirmed
                | Self::Superseded
                | Self::Expired
                | Self::PermanentRejection
        )
    }

    const fn may_retry(self) -> bool {
        matches!(
            self,
            Self::Rejected | Self::DefinitelyNotWritten | Self::InfrastructureError
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OutboundDeliveryWorkSubject {
    ExecutionCommand(CommandId),
    ReconciliationRequest(RequestId),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredOutboundDeliveryWork {
    pub work_id: String,
    pub subject: OutboundDeliveryWorkSubject,
    pub generation: u64,
    pub message_id: MessageId,
    pub status: OutboundDeliveryWorkStatus,
    pub delivery_attempts: u64,
    pub next_attempt_at: Option<i64>,
    pub lease_owner: Option<String>,
    pub lease_expires_at: Option<i64>,
    pub revision: u64,
    pub last_outcome: Option<OutboundDeliveryWorkOutcome>,
    pub last_error: Option<String>,
    pub completed_at: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaimOutboundDeliveryWork {
    pub worker_id: String,
    pub claimed_at: i64,
    pub lease_expires_at: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ClaimedOutboundDelivery {
    ExecutionCommand {
        work: StoredOutboundDeliveryWork,
        command: StoredExecutionCommand,
        state: ExecutionCommandState,
        correlation_id: Option<CorrelationId>,
        causation_id: Option<CausationId>,
    },
    ReconciliationRequest {
        work: StoredOutboundDeliveryWork,
        run: StoredReconciliationRun,
    },
}

impl ClaimedOutboundDelivery {
    pub fn work(&self) -> &StoredOutboundDeliveryWork {
        match self {
            Self::ExecutionCommand { work, .. } | Self::ReconciliationRequest { work, .. } => work,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompleteOutboundDeliveryWork {
    pub work_id: String,
    pub expected_revision: u64,
    pub worker_id: String,
    pub completed_at: i64,
    pub outcome: OutboundDeliveryWorkOutcome,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetryOutboundDeliveryWork {
    pub work_id: String,
    pub expected_revision: u64,
    pub worker_id: String,
    pub failed_at: i64,
    pub retry_at: i64,
    pub outcome: OutboundDeliveryWorkOutcome,
    pub error: String,
    /// Advance only when transport evidence proves that no bytes can have
    /// reached the peer. Unknown infrastructure failures replay this generation.
    pub advance_generation: bool,
}

impl SqliteStateStore {
    pub async fn claim_next_outbound_delivery(
        &self,
        claim: ClaimOutboundDeliveryWork,
    ) -> Result<Option<ClaimedOutboundDelivery>, StoreError> {
        validate_claim(&claim)?;
        let mut transaction = self.begin_write().await?;
        let claimed = claim_next_on(transaction.connection(), &claim).await?;
        transaction.commit().await?;
        Ok(claimed)
    }

    pub async fn get_outbound_delivery_work(
        &self,
        work_id: &str,
    ) -> Result<Option<StoredOutboundDeliveryWork>, StoreError> {
        let mut connection = self.pool().acquire().await?;
        fetch_work_on(&mut connection, work_id).await
    }
}

impl WriteTransaction {
    pub async fn complete_outbound_delivery_work(
        &mut self,
        completion: CompleteOutboundDeliveryWork,
    ) -> Result<StoredOutboundDeliveryWork, StoreError> {
        complete_on(self.connection(), completion).await
    }

    pub async fn retry_outbound_delivery_work(
        &mut self,
        retry: RetryOutboundDeliveryWork,
    ) -> Result<StoredOutboundDeliveryWork, StoreError> {
        retry_on(self.connection(), retry).await
    }
}

async fn claim_next_on(
    connection: &mut SqliteConnection,
    claim: &ClaimOutboundDeliveryWork,
) -> Result<Option<ClaimedOutboundDelivery>, StoreError> {
    let candidate = sqlx::query(
        "WITH candidates AS (\
           SELECT CASE WHEN w.command_id IS NOT NULL \
                  THEN 'EXECUTION_COMMAND' ELSE 'RECONCILIATION_REQUEST' END AS kind, \
                  COALESCE(w.command_id, w.request_id) AS subject_id, \
                  0 AS existing_rank, w.created_at AS due_at \
           FROM outbound_delivery_work w \
           WHERE (w.status = 'PENDING' AND w.next_attempt_at <= ?) \
              OR (w.status = 'PROCESSING' AND w.lease_expires_at <= ?) \
           UNION ALL \
           SELECT 'EXECUTION_COMMAND' AS kind, c.command_id AS subject_id, \
                  1 AS existing_rank, c.created_at AS due_at \
           FROM execution_commands c \
           JOIN execution_command_states s ON s.command_id = c.command_id \
           WHERE s.status = 'CREATED' AND NOT EXISTS (\
             SELECT 1 FROM outbound_delivery_work w WHERE w.command_id = c.command_id\
           ) \
           UNION ALL \
           SELECT 'RECONCILIATION_REQUEST', r.request_id, 1, r.requested_at \
           FROM reconciliation_runs r \
           WHERE r.status = 'REQUESTED' AND NOT EXISTS (\
             SELECT 1 FROM outbound_delivery_work w WHERE w.request_id = r.request_id\
           )\
         ) \
         SELECT kind, subject_id FROM candidates \
         ORDER BY existing_rank, due_at, kind, subject_id LIMIT 1",
    )
    .bind(claim.claimed_at)
    .bind(claim.claimed_at)
    .fetch_optional(&mut *connection)
    .await?;
    let Some(candidate) = candidate else {
        return Ok(None);
    };
    let kind: String = candidate.try_get("kind")?;
    let subject_id: String = candidate.try_get("subject_id")?;
    let subject = match kind.as_str() {
        "EXECUTION_COMMAND" => {
            OutboundDeliveryWorkSubject::ExecutionCommand(CommandId::from(subject_id))
        }
        "RECONCILIATION_REQUEST" => {
            OutboundDeliveryWorkSubject::ReconciliationRequest(RequestId::from(subject_id))
        }
        _ => {
            return Err(StoreError::corrupt(
                "outbound_delivery_work",
                subject_id,
                format!("unknown candidate kind {kind}"),
            ))
        }
    };
    let work_id = work_id(&subject);
    if fetch_work_on(connection, &work_id).await?.is_none() {
        let message_id = message_id(&subject, 1);
        let (command_id, request_id) = subject_columns(&subject);
        sqlx::query(
            "INSERT INTO outbound_delivery_work (\
               work_id, command_id, request_id, generation, message_id, status, \
               delivery_attempts, next_attempt_at, revision, created_at, updated_at\
             ) VALUES (?, ?, ?, 1, ?, 'PENDING', 0, ?, 0, ?, ?)",
        )
        .bind(&work_id)
        .bind(command_id)
        .bind(request_id)
        .bind(message_id.as_str())
        .bind(claim.claimed_at)
        .bind(claim.claimed_at)
        .bind(claim.claimed_at)
        .execute(&mut *connection)
        .await?;
    }

    let current = required_work(connection, &work_id).await?;
    validate_work_message_id(&current)?;
    let next_revision = checked_increment("outbound_delivery_work.revision", current.revision)?;
    let next_attempts = checked_increment(
        "outbound_delivery_work.delivery_attempts",
        current.delivery_attempts,
    )?;
    let result = sqlx::query(
        "UPDATE outbound_delivery_work SET \
           status = 'PROCESSING', delivery_attempts = ?, next_attempt_at = NULL, \
           lease_owner = ?, lease_expires_at = ?, revision = ?, updated_at = ? \
         WHERE work_id = ? AND revision = ? AND (\
           (status = 'PENDING' AND next_attempt_at <= ?) \
           OR (status = 'PROCESSING' AND lease_expires_at <= ?)\
         )",
    )
    .bind(u64_to_i64(
        "outbound_delivery_work.delivery_attempts",
        next_attempts,
    )?)
    .bind(&claim.worker_id)
    .bind(claim.lease_expires_at)
    .bind(u64_to_i64(
        "outbound_delivery_work.revision",
        next_revision,
    )?)
    .bind(claim.claimed_at)
    .bind(&work_id)
    .bind(u64_to_i64(
        "outbound_delivery_work.revision",
        current.revision,
    )?)
    .bind(claim.claimed_at)
    .bind(claim.claimed_at)
    .execute(&mut *connection)
    .await?;
    if result.rows_affected() != 1 {
        return Err(StoreError::StaleWrite {
            entity: "outbound_delivery_work",
            key: work_id,
        });
    }
    let work = required_work(connection, &work_id).await?;
    match &work.subject {
        OutboundDeliveryWorkSubject::ExecutionCommand(command_id) => {
            let command = fetch_execution_command_by_id(&mut *connection, command_id)
                .await?
                .ok_or_else(|| StoreError::NotFound {
                    entity: "execution_command",
                    key: command_id.to_string(),
                })?;
            let state = crate::repository::fetch_execution_command_state_by_id(
                &mut *connection,
                command_id,
            )
            .await?
            .ok_or_else(|| StoreError::NotFound {
                entity: "execution_command_state",
                key: command_id.to_string(),
            })?;
            let (correlation_id, causation_id) =
                command_envelope_context(connection, &command.command).await?;
            Ok(Some(ClaimedOutboundDelivery::ExecutionCommand {
                work,
                command,
                state,
                correlation_id,
                causation_id,
            }))
        }
        OutboundDeliveryWorkSubject::ReconciliationRequest(request_id) => {
            let run = fetch_reconciliation_run_on(connection, request_id)
                .await?
                .ok_or_else(|| StoreError::NotFound {
                    entity: "reconciliation_run",
                    key: request_id.to_string(),
                })?;
            Ok(Some(ClaimedOutboundDelivery::ReconciliationRequest {
                work,
                run,
            }))
        }
    }
}

async fn command_envelope_context(
    connection: &mut SqliteConnection,
    command: &ExecutionCommand,
) -> Result<(Option<CorrelationId>, Option<CausationId>), StoreError> {
    let Some(plan_id) = command.plan_id.as_ref() else {
        return Ok((None, None));
    };
    let intent_id =
        sqlx::query_scalar::<_, String>("SELECT intent_id FROM execution_plans WHERE plan_id = ?")
            .bind(plan_id.as_str())
            .fetch_optional(&mut *connection)
            .await?
            .ok_or_else(|| {
                StoreError::corrupt(
                    "execution_command",
                    command.command_id.to_string(),
                    format!("parent plan {plan_id} has no trade intent"),
                )
            })?;
    let intent_id = IntentId::from(intent_id);
    let intent = fetch_trade_intent_by_id(&mut *connection, &intent_id)
        .await?
        .ok_or_else(|| {
            StoreError::corrupt(
                "execution_command",
                command.command_id.to_string(),
                format!("parent plan {plan_id} references missing intent {intent_id}"),
            )
        })?;
    Ok((
        Some(intent.intent.correlation_id),
        Some(CausationId::from(intent.intent.intent_id.as_str())),
    ))
}

async fn complete_on(
    connection: &mut SqliteConnection,
    completion: CompleteOutboundDeliveryWork,
) -> Result<StoredOutboundDeliveryWork, StoreError> {
    if !completion.outcome.may_complete() {
        return Err(invalid_work(
            &completion.work_id,
            "retryable outcome cannot complete work",
        ));
    }
    validate_error(
        completion.outcome,
        completion.error.as_deref(),
        &completion.work_id,
    )?;
    let current = validate_owned_work(
        connection,
        &completion.work_id,
        completion.expected_revision,
        &completion.worker_id,
        completion.completed_at,
    )
    .await?;
    let next_revision = checked_increment("outbound_delivery_work.revision", current.revision)?;
    let result = sqlx::query(
        "UPDATE outbound_delivery_work SET \
           status = 'DELIVERED', next_attempt_at = NULL, lease_owner = NULL, \
           lease_expires_at = NULL, revision = ?, last_outcome = ?, last_error = ?, \
           completed_at = ?, updated_at = ? \
         WHERE work_id = ? AND status = 'PROCESSING' AND revision = ? \
           AND lease_owner = ? AND lease_expires_at > ?",
    )
    .bind(u64_to_i64(
        "outbound_delivery_work.revision",
        next_revision,
    )?)
    .bind(completion.outcome.as_str())
    .bind(completion.error.as_deref())
    .bind(completion.completed_at)
    .bind(completion.completed_at)
    .bind(&completion.work_id)
    .bind(u64_to_i64(
        "outbound_delivery_work.revision",
        completion.expected_revision,
    )?)
    .bind(&completion.worker_id)
    .bind(completion.completed_at)
    .execute(&mut *connection)
    .await?;
    if result.rows_affected() != 1 {
        return Err(stale_work(&completion.work_id));
    }
    required_work(connection, &completion.work_id).await
}

async fn retry_on(
    connection: &mut SqliteConnection,
    retry: RetryOutboundDeliveryWork,
) -> Result<StoredOutboundDeliveryWork, StoreError> {
    if !retry.outcome.may_retry() {
        return Err(invalid_work(
            &retry.work_id,
            "delivered outcome cannot schedule a retry",
        ));
    }
    if retry.error.trim().is_empty() || retry.retry_at <= retry.failed_at {
        return Err(invalid_work(
            &retry.work_id,
            "retry requires a non-empty error and retry_at later than failed_at",
        ));
    }
    let current = validate_owned_work(
        connection,
        &retry.work_id,
        retry.expected_revision,
        &retry.worker_id,
        retry.failed_at,
    )
    .await?;
    let next_revision = checked_increment("outbound_delivery_work.revision", current.revision)?;
    let next_generation = if retry.advance_generation {
        checked_increment("outbound_delivery_work.generation", current.generation)?
    } else {
        current.generation
    };
    let next_message_id = message_id(&current.subject, next_generation);
    let result = sqlx::query(
        "UPDATE outbound_delivery_work SET \
           generation = ?, message_id = ?, status = 'PENDING', next_attempt_at = ?, \
           lease_owner = NULL, lease_expires_at = NULL, revision = ?, last_outcome = ?, \
           last_error = ?, completed_at = NULL, updated_at = ? \
         WHERE work_id = ? AND status = 'PROCESSING' AND revision = ? \
           AND lease_owner = ? AND lease_expires_at > ?",
    )
    .bind(u64_to_i64(
        "outbound_delivery_work.generation",
        next_generation,
    )?)
    .bind(next_message_id.as_str())
    .bind(retry.retry_at)
    .bind(u64_to_i64(
        "outbound_delivery_work.revision",
        next_revision,
    )?)
    .bind(retry.outcome.as_str())
    .bind(&retry.error)
    .bind(retry.failed_at)
    .bind(&retry.work_id)
    .bind(u64_to_i64(
        "outbound_delivery_work.revision",
        retry.expected_revision,
    )?)
    .bind(&retry.worker_id)
    .bind(retry.failed_at)
    .execute(&mut *connection)
    .await?;
    if result.rows_affected() != 1 {
        return Err(stale_work(&retry.work_id));
    }
    required_work(connection, &retry.work_id).await
}

async fn validate_owned_work(
    connection: &mut SqliteConnection,
    work_id: &str,
    expected_revision: u64,
    worker_id: &str,
    finished_at: i64,
) -> Result<StoredOutboundDeliveryWork, StoreError> {
    if worker_id.trim().is_empty() || finished_at < 0 {
        return Err(invalid_work(
            work_id,
            "invalid owner or completion timestamp",
        ));
    }
    let current = required_work(connection, work_id).await?;
    if current.status != OutboundDeliveryWorkStatus::Processing
        || current.revision != expected_revision
        || current.lease_owner.as_deref() != Some(worker_id)
        || current.updated_at > finished_at
        || current
            .lease_expires_at
            .is_none_or(|lease_expires_at| lease_expires_at <= finished_at)
    {
        return Err(stale_work(work_id));
    }
    Ok(current)
}

async fn required_work(
    connection: &mut SqliteConnection,
    work_id: &str,
) -> Result<StoredOutboundDeliveryWork, StoreError> {
    fetch_work_on(connection, work_id)
        .await?
        .ok_or_else(|| StoreError::NotFound {
            entity: "outbound_delivery_work",
            key: work_id.to_owned(),
        })
}

async fn fetch_work_on(
    connection: &mut SqliteConnection,
    work_id: &str,
) -> Result<Option<StoredOutboundDeliveryWork>, StoreError> {
    let row = sqlx::query(&format!(
        "SELECT {WORK_COLUMNS} FROM outbound_delivery_work WHERE work_id = ?"
    ))
    .bind(work_id)
    .fetch_optional(&mut *connection)
    .await?;
    row.map(work_from_row).transpose()
}

fn work_from_row(row: sqlx::sqlite::SqliteRow) -> Result<StoredOutboundDeliveryWork, StoreError> {
    let work_id: String = row.try_get("work_id")?;
    let command_id: Option<String> = row.try_get("command_id")?;
    let request_id: Option<String> = row.try_get("request_id")?;
    let subject = match (command_id, request_id) {
        (Some(command_id), None) => {
            OutboundDeliveryWorkSubject::ExecutionCommand(CommandId::from(command_id))
        }
        (None, Some(request_id)) => {
            OutboundDeliveryWorkSubject::ReconciliationRequest(RequestId::from(request_id))
        }
        _ => {
            return Err(StoreError::corrupt(
                "outbound_delivery_work",
                &work_id,
                "work must reference exactly one delivery subject",
            ))
        }
    };
    let status = match row.try_get::<String, _>("status")?.as_str() {
        "PENDING" => OutboundDeliveryWorkStatus::Pending,
        "PROCESSING" => OutboundDeliveryWorkStatus::Processing,
        "DELIVERED" => OutboundDeliveryWorkStatus::Delivered,
        value => {
            return Err(StoreError::corrupt(
                "outbound_delivery_work",
                &work_id,
                format!("unknown status {value}"),
            ))
        }
    };
    let last_outcome = row
        .try_get::<Option<String>, _>("last_outcome")?
        .map(|value| parse_outcome(&work_id, &value))
        .transpose()?;
    let work = StoredOutboundDeliveryWork {
        work_id,
        subject,
        generation: i64_to_u64(
            "outbound_delivery_work.generation",
            row.try_get("generation")?,
        )?,
        message_id: MessageId::from(row.try_get::<String, _>("message_id")?),
        status,
        delivery_attempts: i64_to_u64(
            "outbound_delivery_work.delivery_attempts",
            row.try_get("delivery_attempts")?,
        )?,
        next_attempt_at: row.try_get("next_attempt_at")?,
        lease_owner: row.try_get("lease_owner")?,
        lease_expires_at: row.try_get("lease_expires_at")?,
        revision: i64_to_u64("outbound_delivery_work.revision", row.try_get("revision")?)?,
        last_outcome,
        last_error: row.try_get("last_error")?,
        completed_at: row.try_get("completed_at")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    };
    validate_work_message_id(&work)?;
    Ok(work)
}

fn parse_outcome(work_id: &str, value: &str) -> Result<OutboundDeliveryWorkOutcome, StoreError> {
    match value {
        "SENT" => Ok(OutboundDeliveryWorkOutcome::Sent),
        "UNCONFIRMED" => Ok(OutboundDeliveryWorkOutcome::Unconfirmed),
        "REJECTED" => Ok(OutboundDeliveryWorkOutcome::Rejected),
        "DEFINITELY_NOT_WRITTEN" => Ok(OutboundDeliveryWorkOutcome::DefinitelyNotWritten),
        "INFRASTRUCTURE_ERROR" => Ok(OutboundDeliveryWorkOutcome::InfrastructureError),
        "SUPERSEDED" => Ok(OutboundDeliveryWorkOutcome::Superseded),
        "EXPIRED" => Ok(OutboundDeliveryWorkOutcome::Expired),
        "PERMANENT_REJECTION" => Ok(OutboundDeliveryWorkOutcome::PermanentRejection),
        _ => Err(StoreError::corrupt(
            "outbound_delivery_work",
            work_id,
            format!("unknown outcome {value}"),
        )),
    }
}

fn validate_claim(claim: &ClaimOutboundDeliveryWork) -> Result<(), StoreError> {
    if claim.worker_id.trim().is_empty()
        || claim.claimed_at < 0
        || claim.lease_expires_at <= claim.claimed_at
    {
        return Err(invalid_work(
            &claim.worker_id,
            "claim requires a non-empty owner and a future lease",
        ));
    }
    Ok(())
}

fn validate_error(
    outcome: OutboundDeliveryWorkOutcome,
    error: Option<&str>,
    work_id: &str,
) -> Result<(), StoreError> {
    let requires_error = outcome == OutboundDeliveryWorkOutcome::Unconfirmed;
    if requires_error && error.is_none_or(|error| error.trim().is_empty()) {
        return Err(invalid_work(
            work_id,
            "unconfirmed delivery requires an error",
        ));
    }
    if error.is_some_and(|error| error.trim().is_empty()) {
        return Err(invalid_work(work_id, "delivery error must not be empty"));
    }
    Ok(())
}

fn validate_work_message_id(work: &StoredOutboundDeliveryWork) -> Result<(), StoreError> {
    let expected = message_id(&work.subject, work.generation);
    if work.message_id == expected {
        Ok(())
    } else {
        Err(StoreError::corrupt(
            "outbound_delivery_work",
            &work.work_id,
            "message_id does not match subject and generation",
        ))
    }
}

fn work_id(subject: &OutboundDeliveryWorkSubject) -> String {
    match subject {
        OutboundDeliveryWorkSubject::ExecutionCommand(command_id) => {
            format!("execution.command:{}", command_id.as_str())
        }
        OutboundDeliveryWorkSubject::ReconciliationRequest(request_id) => {
            format!("reconciliation.request:{}", request_id.as_str())
        }
    }
}

pub fn outbound_delivery_message_id(
    subject: &OutboundDeliveryWorkSubject,
    generation: u64,
) -> MessageId {
    message_id(subject, generation)
}

fn message_id(subject: &OutboundDeliveryWorkSubject, generation: u64) -> MessageId {
    MessageId::from(format!("msg:{}:v{generation}", work_id(subject)))
}

fn subject_columns(subject: &OutboundDeliveryWorkSubject) -> (Option<&str>, Option<&str>) {
    match subject {
        OutboundDeliveryWorkSubject::ExecutionCommand(command_id) => {
            (Some(command_id.as_str()), None)
        }
        OutboundDeliveryWorkSubject::ReconciliationRequest(request_id) => {
            (None, Some(request_id.as_str()))
        }
    }
}

fn checked_increment(field: &'static str, value: u64) -> Result<u64, StoreError> {
    value
        .checked_add(1)
        .ok_or(StoreError::InvalidInteger { field, value })
}

fn u64_to_i64(field: &'static str, value: u64) -> Result<i64, StoreError> {
    i64::try_from(value).map_err(|_| StoreError::InvalidInteger { field, value })
}

fn i64_to_u64(field: &'static str, value: i64) -> Result<u64, StoreError> {
    u64::try_from(value)
        .map_err(|_| StoreError::corrupt(field, value.to_string(), "negative value"))
}

fn stale_work(work_id: &str) -> StoreError {
    StoreError::StaleWrite {
        entity: "outbound_delivery_work",
        key: work_id.to_owned(),
    }
}

fn invalid_work(work_id: &str, reason: impl Into<String>) -> StoreError {
    StoreError::InvalidRecord {
        entity: "outbound_delivery_work",
        key: work_id.to_owned(),
        reason: reason.into(),
    }
}
