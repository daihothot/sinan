use std::sync::Arc;

use sinan_execution::{
    project_plan, transition_command, CommandEvidence, CommandTransitionError, DeliveryOutcome,
    DeliveryRejectionReason, DeliveryRequest, OutboundDeliveryPort, ProjectionError, ServerClock,
};
use sinan_protocol::{
    ExecutionClientMessageType, ReconciliationReason, ReconciliationRequest, WireMessage,
    SUPPORTED_SCHEMA_VERSION,
};
use sinan_store::{
    ClaimOutboundDeliveryWork, ClaimedOutboundDelivery, CommandStateUpdate,
    CompleteOutboundDeliveryWork, CoreEventMetadata, ExecutionLifecycleUpdate,
    ExecutionProjectionSnapshot, LegStateUpdate, NewReconciliationRun, OutboundDeliveryWorkOutcome,
    OutboundDeliveryWorkSubject, PlanStateUpdate, ReconciliationRunStatus,
    RetryOutboundDeliveryWork, SqliteStateStore, StoreError, StoredOutboundDeliveryWork,
    WriteTransaction,
};
use sinan_types::{
    CausationId, CommandId, ExecutionCommandState, ExecutionCommandStatus, RequestId,
};
use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DurableOutboundConfig {
    pub worker_id: String,
    pub lease_duration_ms: i64,
    pub retry_base_delay_ms: i64,
    pub retry_max_delay_ms: i64,
}

impl DurableOutboundConfig {
    fn validate(&self) -> Result<(), OutboundWorkflowError> {
        if self.worker_id.trim().is_empty() {
            return Err(OutboundWorkflowError::InvalidConfiguration(
                "worker_id must not be empty".to_owned(),
            ));
        }
        if self.lease_duration_ms <= 0 {
            return Err(OutboundWorkflowError::InvalidConfiguration(
                "lease_duration_ms must be positive".to_owned(),
            ));
        }
        if self.retry_base_delay_ms <= 0 || self.retry_max_delay_ms < self.retry_base_delay_ms {
            return Err(OutboundWorkflowError::InvalidConfiguration(
                "retry delays must be positive and max must not be less than base".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DurableDeliveryDisposition {
    Sent,
    Unconfirmed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DurableOutboundProcessOutcome {
    NoWork,
    ExecutionCommandDelivered {
        command_id: CommandId,
        message_id: sinan_types::MessageId,
        disposition: DurableDeliveryDisposition,
    },
    ReconciliationRequestDelivered {
        request_id: RequestId,
        message_id: sinan_types::MessageId,
        disposition: DurableDeliveryDisposition,
    },
    RetryScheduled {
        subject: OutboundDeliveryWorkSubject,
        failed_message_id: sinan_types::MessageId,
        next_message_id: sinan_types::MessageId,
        retry_at: i64,
        reason: String,
    },
    DeliveryStopped {
        subject: OutboundDeliveryWorkSubject,
        message_id: sinan_types::MessageId,
        outcome: OutboundDeliveryWorkOutcome,
        reason: String,
    },
}

enum CommandDeliveryCommitOutcome {
    Applied,
    Superseded { reason: String },
}

#[derive(Debug, Error)]
pub enum OutboundWorkflowError {
    #[error(transparent)]
    Store(#[from] StoreError),

    #[error(transparent)]
    CommandTransition(#[from] CommandTransitionError),

    #[error(transparent)]
    Projection(#[from] ProjectionError),

    #[error("invalid durable outbound configuration: {0}")]
    InvalidConfiguration(String),

    #[error("server clock produced an invalid timestamp: {0}")]
    InvalidClock(String),

    #[error("durable outbound invariant failed: {0}")]
    Invariant(String),
}

#[derive(Clone)]
pub struct DurableOutboundProcessor {
    store: SqliteStateStore,
    delivery: Arc<dyn OutboundDeliveryPort>,
    clock: Arc<dyn ServerClock>,
    config: DurableOutboundConfig,
}

impl DurableOutboundProcessor {
    pub fn new(
        store: SqliteStateStore,
        delivery: Arc<dyn OutboundDeliveryPort>,
        clock: Arc<dyn ServerClock>,
        config: DurableOutboundConfig,
    ) -> Result<Self, OutboundWorkflowError> {
        config.validate()?;
        Ok(Self {
            store,
            delivery,
            clock,
            config,
        })
    }

    pub async fn process_next(
        &self,
    ) -> Result<DurableOutboundProcessOutcome, OutboundWorkflowError> {
        let claimed_at = self.now_ms()?;
        let lease_expires_at = claimed_at
            .checked_add(self.config.lease_duration_ms)
            .ok_or_else(|| {
                OutboundWorkflowError::InvalidClock("lease timestamp overflow".to_owned())
            })?;
        let Some(claimed) = self
            .store
            .claim_next_outbound_delivery(ClaimOutboundDeliveryWork {
                worker_id: self.config.worker_id.clone(),
                claimed_at,
                lease_expires_at,
            })
            .await?
        else {
            return Ok(DurableOutboundProcessOutcome::NoWork);
        };

        match claimed {
            ClaimedOutboundDelivery::ExecutionCommand {
                work,
                command,
                state,
                correlation_id,
                causation_id,
            } => {
                if state.status != ExecutionCommandStatus::Created {
                    let completed_at = self.now_ms()?;
                    return self
                        .stop_delivery(
                            work,
                            completed_at,
                            OutboundDeliveryWorkOutcome::Superseded,
                            None,
                            format!("command lifecycle already advanced to {}", state.status),
                        )
                        .await;
                }
                let request = DeliveryRequest {
                    account_id: command.command.account_id.clone(),
                    client_id: command.command.client_id.clone(),
                    terminal_id: command.command.terminal_id.clone(),
                    command_id: Some(command.command.command_id.clone()),
                    message: WireMessage {
                        message_id: work.message_id.clone(),
                        message_type: ExecutionClientMessageType::ExecutionCommand,
                        schema_version: SUPPORTED_SCHEMA_VERSION.to_string(),
                        client_id: command.command.client_id.clone(),
                        session_id: None,
                        correlation_id,
                        causation_id,
                        sent_at: None,
                        sequence: None,
                        payload: command.command.clone(),
                    },
                    expires_at: Some(command.command.expires_at),
                };
                let outcome = self.delivery.deliver_execution_command(request).await;
                let finished_at = self.now_ms()?;
                self.finish_command(work, command.command, outcome, finished_at)
                    .await
            }
            ClaimedOutboundDelivery::ReconciliationRequest { work, run } => {
                if run.status != ReconciliationRunStatus::Requested {
                    let completed_at = self.now_ms()?;
                    return self
                        .stop_delivery(
                            work,
                            completed_at,
                            OutboundDeliveryWorkOutcome::Superseded,
                            None,
                            format!("reconciliation run already advanced to {:?}", run.status),
                        )
                        .await;
                }
                let request = DeliveryRequest {
                    account_id: run.request.account_id.clone(),
                    client_id: run.request.client_id.clone(),
                    terminal_id: run.request.terminal_id.clone(),
                    command_id: None,
                    message: WireMessage {
                        message_id: work.message_id.clone(),
                        message_type: ExecutionClientMessageType::ReconciliationRequest,
                        schema_version: SUPPORTED_SCHEMA_VERSION.to_string(),
                        client_id: run.request.client_id.clone(),
                        session_id: None,
                        correlation_id: run.request_event.metadata.correlation_id.clone(),
                        causation_id: run.request_event.metadata.causation_id.clone(),
                        sent_at: None,
                        sequence: None,
                        payload: run.request.clone(),
                    },
                    expires_at: None,
                };
                let outcome = self.delivery.deliver_reconciliation_request(request).await;
                let finished_at = self.now_ms()?;
                self.finish_reconciliation(work, outcome, finished_at).await
            }
        }
    }

    async fn finish_command(
        &self,
        work: StoredOutboundDeliveryWork,
        command: sinan_types::ExecutionCommand,
        outcome: Result<DeliveryOutcome, sinan_execution::DeliveryInfrastructureError>,
        finished_at: i64,
    ) -> Result<DurableOutboundProcessOutcome, OutboundWorkflowError> {
        match outcome {
            Ok(DeliveryOutcome::Sent(receipt)) => {
                if let Err(error) = validate_delivery_evidence(
                    &work,
                    &receipt.message_id,
                    receipt.sent_at,
                    finished_at,
                    Some(command.expires_at),
                ) {
                    return self
                        .schedule_retry(
                            work,
                            finished_at,
                            OutboundDeliveryWorkOutcome::InfrastructureError,
                            error,
                            false,
                        )
                        .await;
                }
                let committed = self
                    .commit_command_delivery(
                        &work,
                        &command,
                        receipt.sent_at,
                        None,
                        finished_at,
                        OutboundDeliveryWorkOutcome::Sent,
                    )
                    .await?;
                Ok(command_delivery_process_outcome(
                    work,
                    command.command_id,
                    DurableDeliveryDisposition::Sent,
                    committed,
                ))
            }
            Ok(DeliveryOutcome::Unconfirmed(uncertainty)) => {
                if let Err(error) = validate_unconfirmed_evidence(
                    &work,
                    &uncertainty.message_id,
                    uncertainty.write_started_at,
                    uncertainty.observed_at,
                    finished_at,
                    command.expires_at,
                ) {
                    return self
                        .schedule_retry(
                            work,
                            finished_at,
                            OutboundDeliveryWorkOutcome::InfrastructureError,
                            error,
                            false,
                        )
                        .await;
                }
                if uncertainty.error.trim().is_empty() {
                    return self
                        .schedule_retry(
                            work,
                            finished_at,
                            OutboundDeliveryWorkOutcome::InfrastructureError,
                            "delivery port returned empty uncertainty evidence".to_owned(),
                            false,
                        )
                        .await;
                }
                let committed = self
                    .commit_command_delivery(
                        &work,
                        &command,
                        uncertainty.write_started_at,
                        Some((uncertainty.observed_at, &uncertainty.error)),
                        finished_at,
                        OutboundDeliveryWorkOutcome::Unconfirmed,
                    )
                    .await?;
                Ok(command_delivery_process_outcome(
                    work,
                    command.command_id,
                    DurableDeliveryDisposition::Unconfirmed,
                    committed,
                ))
            }
            Ok(DeliveryOutcome::Rejected(rejection)) => {
                if rejection.message_id != work.message_id {
                    return self
                        .schedule_retry(
                            work,
                            finished_at,
                            OutboundDeliveryWorkOutcome::InfrastructureError,
                            "delivery port returned a rejection for a different message_id"
                                .to_owned(),
                            false,
                        )
                        .await;
                }
                match rejection.reason {
                    DeliveryRejectionReason::Expired => {
                        self.expire_command(work, command, rejection.rejected_at, finished_at)
                            .await
                    }
                    DeliveryRejectionReason::IdentityMismatch { field } => {
                        self.stop_delivery(
                            work,
                            finished_at,
                            OutboundDeliveryWorkOutcome::PermanentRejection,
                            Some(format!("identity mismatch: {field}")),
                            format!("permanent delivery identity mismatch: {field}"),
                        )
                        .await
                    }
                    DeliveryRejectionReason::TransportRejected { reason } => {
                        self.stop_delivery(
                            work,
                            finished_at,
                            OutboundDeliveryWorkOutcome::PermanentRejection,
                            Some(reason.clone()),
                            format!("peer permanently rejected the transport message: {reason}"),
                        )
                        .await
                    }
                    reason => {
                        self.schedule_retry(
                            work,
                            finished_at,
                            OutboundDeliveryWorkOutcome::Rejected,
                            format!("{reason:?}"),
                            true,
                        )
                        .await
                    }
                }
            }
            Ok(DeliveryOutcome::DefinitelyNotWritten(failure)) => {
                if failure.message_id != work.message_id {
                    return self
                        .schedule_retry(
                            work,
                            finished_at,
                            OutboundDeliveryWorkOutcome::InfrastructureError,
                            "delivery port returned failure for a different message_id".to_owned(),
                            false,
                        )
                        .await;
                }
                self.schedule_retry(
                    work,
                    finished_at,
                    OutboundDeliveryWorkOutcome::DefinitelyNotWritten,
                    failure.error,
                    true,
                )
                .await
            }
            Err(error) => {
                self.schedule_retry(
                    work,
                    finished_at,
                    OutboundDeliveryWorkOutcome::InfrastructureError,
                    error.to_string(),
                    false,
                )
                .await
            }
        }
    }

    async fn finish_reconciliation(
        &self,
        work: StoredOutboundDeliveryWork,
        outcome: Result<DeliveryOutcome, sinan_execution::DeliveryInfrastructureError>,
        finished_at: i64,
    ) -> Result<DurableOutboundProcessOutcome, OutboundWorkflowError> {
        let request_id = match &work.subject {
            OutboundDeliveryWorkSubject::ReconciliationRequest(request_id) => request_id.clone(),
            _ => {
                return Err(OutboundWorkflowError::Invariant(
                    "reconciliation completion owns command work".to_owned(),
                ))
            }
        };
        match outcome {
            Ok(DeliveryOutcome::Sent(receipt)) => {
                if let Err(error) = validate_delivery_evidence(
                    &work,
                    &receipt.message_id,
                    receipt.sent_at,
                    finished_at,
                    None,
                ) {
                    return self
                        .schedule_retry(
                            work,
                            finished_at,
                            OutboundDeliveryWorkOutcome::InfrastructureError,
                            error,
                            false,
                        )
                        .await;
                }
                self.complete_work(&work, finished_at, OutboundDeliveryWorkOutcome::Sent, None)
                    .await?;
                Ok(
                    DurableOutboundProcessOutcome::ReconciliationRequestDelivered {
                        request_id,
                        message_id: work.message_id,
                        disposition: DurableDeliveryDisposition::Sent,
                    },
                )
            }
            Ok(DeliveryOutcome::Unconfirmed(uncertainty)) => {
                if let Err(error) = validate_unconfirmed_evidence_without_expiry(
                    &work,
                    &uncertainty.message_id,
                    uncertainty.write_started_at,
                    uncertainty.observed_at,
                    finished_at,
                ) {
                    return self
                        .schedule_retry(
                            work,
                            finished_at,
                            OutboundDeliveryWorkOutcome::InfrastructureError,
                            error,
                            false,
                        )
                        .await;
                }
                if uncertainty.error.trim().is_empty() {
                    return self
                        .schedule_retry(
                            work,
                            finished_at,
                            OutboundDeliveryWorkOutcome::InfrastructureError,
                            "delivery port returned empty uncertainty evidence".to_owned(),
                            false,
                        )
                        .await;
                }
                self.complete_work(
                    &work,
                    finished_at,
                    OutboundDeliveryWorkOutcome::Unconfirmed,
                    Some(uncertainty.error),
                )
                .await?;
                Ok(
                    DurableOutboundProcessOutcome::ReconciliationRequestDelivered {
                        request_id,
                        message_id: work.message_id,
                        disposition: DurableDeliveryDisposition::Unconfirmed,
                    },
                )
            }
            Ok(DeliveryOutcome::Rejected(rejection)) => {
                if rejection.message_id != work.message_id {
                    return self
                        .schedule_retry(
                            work,
                            finished_at,
                            OutboundDeliveryWorkOutcome::InfrastructureError,
                            "delivery port returned a rejection for a different message_id"
                                .to_owned(),
                            false,
                        )
                        .await;
                }
                match rejection.reason {
                    DeliveryRejectionReason::NoActiveSession
                    | DeliveryRejectionReason::AmbiguousRoute { .. }
                    | DeliveryRejectionReason::ClockUnhealthy
                    | DeliveryRejectionReason::Backpressure { .. }
                    | DeliveryRejectionReason::InflightLimit { .. } => {
                        self.schedule_retry(
                            work,
                            finished_at,
                            OutboundDeliveryWorkOutcome::Rejected,
                            format!("{:?}", rejection.reason),
                            true,
                        )
                        .await
                    }
                    reason => {
                        let detail = format!("permanent reconciliation rejection: {reason:?}");
                        self.stop_delivery(
                            work,
                            finished_at,
                            OutboundDeliveryWorkOutcome::PermanentRejection,
                            Some(detail.clone()),
                            detail,
                        )
                        .await
                    }
                }
            }
            Ok(DeliveryOutcome::DefinitelyNotWritten(failure)) => {
                if failure.message_id != work.message_id {
                    return self
                        .schedule_retry(
                            work,
                            finished_at,
                            OutboundDeliveryWorkOutcome::InfrastructureError,
                            "delivery port returned failure for a different message_id".to_owned(),
                            false,
                        )
                        .await;
                }
                self.schedule_retry(
                    work,
                    finished_at,
                    OutboundDeliveryWorkOutcome::DefinitelyNotWritten,
                    failure.error,
                    true,
                )
                .await
            }
            Err(error) => {
                self.schedule_retry(
                    work,
                    finished_at,
                    OutboundDeliveryWorkOutcome::InfrastructureError,
                    error.to_string(),
                    false,
                )
                .await
            }
        }
    }

    async fn commit_command_delivery(
        &self,
        work: &StoredOutboundDeliveryWork,
        command: &sinan_types::ExecutionCommand,
        dispatched_at: i64,
        uncertainty: Option<(i64, &str)>,
        completed_at: i64,
        outcome: OutboundDeliveryWorkOutcome,
    ) -> Result<CommandDeliveryCommitOutcome, OutboundWorkflowError> {
        let mut transaction = self.store.begin_write().await?;
        let current = transaction
            .get_execution_command_state(&command.command_id)
            .await?
            .ok_or_else(|| {
                OutboundWorkflowError::Invariant(format!(
                    "command {} has no lifecycle state",
                    command.command_id
                ))
            })?;
        if current.status != ExecutionCommandStatus::Created {
            let reason = format!(
                "stronger command lifecycle evidence advanced state to {} during delivery",
                current.status
            );
            transaction
                .complete_outbound_delivery_work(CompleteOutboundDeliveryWork {
                    work_id: work.work_id.clone(),
                    expected_revision: work.revision,
                    worker_id: self.config.worker_id.clone(),
                    completed_at,
                    outcome: OutboundDeliveryWorkOutcome::Superseded,
                    error: Some(reason.clone()),
                })
                .await?;
            transaction.commit().await?;
            return Ok(CommandDeliveryCommitOutcome::Superseded { reason });
        }
        if command.plan_id.is_some() {
            let mut snapshot = transaction
                .load_execution_projection(&command.command_id)
                .await?
                .ok_or_else(|| {
                    OutboundWorkflowError::Invariant(format!(
                        "command {} has no execution projection",
                        command.command_id
                    ))
                })?;
            apply_command_delivery_transition(
                &mut transaction,
                &mut snapshot,
                command,
                dispatched_at,
                uncertainty,
            )
            .await?;
        } else {
            if let Some(next) = next_delivery_state(command, &current, dispatched_at, uncertainty)?
            {
                transaction
                    .update_execution_command_state(CommandStateUpdate {
                        expected_status: current.status,
                        expected_updated_at: current.updated_at,
                        state: next,
                    })
                    .await?;
            }
        }
        let uncertainty_error = uncertainty.map(|(_, error)| error.to_owned());
        if let Some((observed_at, _)) = uncertainty {
            let current = transaction
                .get_execution_command_state(&command.command_id)
                .await?
                .ok_or_else(|| {
                    OutboundWorkflowError::Invariant(format!(
                        "command {} lost its lifecycle state",
                        command.command_id
                    ))
                })?;
            if current.status == ExecutionCommandStatus::DeliveryUnconfirmed {
                ensure_delivery_reconciliation(
                    &mut transaction,
                    work,
                    command,
                    observed_at,
                    completed_at,
                )
                .await?;
            }
        }
        transaction
            .complete_outbound_delivery_work(CompleteOutboundDeliveryWork {
                work_id: work.work_id.clone(),
                expected_revision: work.revision,
                worker_id: self.config.worker_id.clone(),
                completed_at,
                outcome,
                error: uncertainty_error,
            })
            .await?;
        transaction.commit().await?;
        Ok(CommandDeliveryCommitOutcome::Applied)
    }

    async fn complete_work(
        &self,
        work: &StoredOutboundDeliveryWork,
        completed_at: i64,
        outcome: OutboundDeliveryWorkOutcome,
        error: Option<String>,
    ) -> Result<(), OutboundWorkflowError> {
        let mut transaction = self.store.begin_write().await?;
        transaction
            .complete_outbound_delivery_work(CompleteOutboundDeliveryWork {
                work_id: work.work_id.clone(),
                expected_revision: work.revision,
                worker_id: self.config.worker_id.clone(),
                completed_at,
                outcome,
                error,
            })
            .await?;
        transaction.commit().await?;
        Ok(())
    }

    async fn expire_command(
        &self,
        work: StoredOutboundDeliveryWork,
        command: sinan_types::ExecutionCommand,
        rejected_at: i64,
        completed_at: i64,
    ) -> Result<DurableOutboundProcessOutcome, OutboundWorkflowError> {
        let expired_at = rejected_at.max(command.expires_at);
        if rejected_at < work.created_at || expired_at > completed_at {
            return self
                .schedule_retry(
                    work,
                    completed_at,
                    OutboundDeliveryWorkOutcome::InfrastructureError,
                    "expired rejection timestamp is outside durable delivery bounds".to_owned(),
                    false,
                )
                .await;
        }
        let mut transaction = self.store.begin_write().await?;
        let expired = if command.plan_id.is_some() {
            let mut snapshot = transaction
                .load_execution_projection(&command.command_id)
                .await?
                .ok_or_else(|| {
                    OutboundWorkflowError::Invariant(format!(
                        "command {} has no execution projection",
                        command.command_id
                    ))
                })?;
            apply_command_expiry_transition(&mut transaction, &mut snapshot, &command, expired_at)
                .await?
        } else {
            let current = transaction
                .get_execution_command_state(&command.command_id)
                .await?
                .ok_or_else(|| {
                    OutboundWorkflowError::Invariant(format!(
                        "command {} has no lifecycle state",
                        command.command_id
                    ))
                })?;
            if current.status == ExecutionCommandStatus::Created {
                let next = transition_command(
                    &command,
                    &current,
                    CommandEvidence::Expire { at: expired_at },
                )?
                .into_state();
                transaction
                    .update_execution_command_state(CommandStateUpdate {
                        expected_status: current.status,
                        expected_updated_at: current.updated_at,
                        state: next,
                    })
                    .await?;
                true
            } else {
                false
            }
        };
        let outcome = if expired {
            OutboundDeliveryWorkOutcome::Expired
        } else {
            OutboundDeliveryWorkOutcome::Superseded
        };
        let reason = if expired {
            "command expired before dispatch".to_owned()
        } else {
            "stronger command lifecycle evidence superseded expiry".to_owned()
        };
        transaction
            .complete_outbound_delivery_work(CompleteOutboundDeliveryWork {
                work_id: work.work_id.clone(),
                expected_revision: work.revision,
                worker_id: self.config.worker_id.clone(),
                completed_at,
                outcome,
                error: Some(reason.clone()),
            })
            .await?;
        transaction.commit().await?;
        Ok(DurableOutboundProcessOutcome::DeliveryStopped {
            subject: work.subject,
            message_id: work.message_id,
            outcome,
            reason,
        })
    }

    async fn schedule_retry(
        &self,
        work: StoredOutboundDeliveryWork,
        failed_at: i64,
        outcome: OutboundDeliveryWorkOutcome,
        reason: String,
        advance_generation: bool,
    ) -> Result<DurableOutboundProcessOutcome, OutboundWorkflowError> {
        let reason = if reason.trim().is_empty() {
            "outbound delivery failed without detail".to_owned()
        } else {
            reason
        };
        let delay = retry_delay(&self.config, work.delivery_attempts);
        let retry_at = failed_at.checked_add(delay).ok_or_else(|| {
            OutboundWorkflowError::InvalidClock("retry timestamp overflow".to_owned())
        })?;
        let failed_message_id = work.message_id.clone();
        let subject = work.subject.clone();
        let mut transaction = self.store.begin_write().await?;
        let superseded = match &work.subject {
            OutboundDeliveryWorkSubject::ExecutionCommand(command_id) => transaction
                .get_execution_command_state(command_id)
                .await?
                .is_some_and(|state| state.status != ExecutionCommandStatus::Created),
            OutboundDeliveryWorkSubject::ReconciliationRequest(request_id) => transaction
                .get_reconciliation_run(request_id)
                .await?
                .is_some_and(|run| run.status != ReconciliationRunStatus::Requested),
        };
        if superseded {
            let stopped_reason = format!("stronger business evidence superseded retry: {reason}");
            transaction
                .complete_outbound_delivery_work(CompleteOutboundDeliveryWork {
                    work_id: work.work_id,
                    expected_revision: work.revision,
                    worker_id: self.config.worker_id.clone(),
                    completed_at: failed_at,
                    outcome: OutboundDeliveryWorkOutcome::Superseded,
                    error: Some(stopped_reason.clone()),
                })
                .await?;
            transaction.commit().await?;
            return Ok(DurableOutboundProcessOutcome::DeliveryStopped {
                subject,
                message_id: failed_message_id,
                outcome: OutboundDeliveryWorkOutcome::Superseded,
                reason: stopped_reason,
            });
        }
        let pending = transaction
            .retry_outbound_delivery_work(RetryOutboundDeliveryWork {
                work_id: work.work_id,
                expected_revision: work.revision,
                worker_id: self.config.worker_id.clone(),
                failed_at,
                retry_at,
                outcome,
                error: reason.clone(),
                advance_generation,
            })
            .await?;
        transaction.commit().await?;
        Ok(DurableOutboundProcessOutcome::RetryScheduled {
            subject,
            failed_message_id,
            next_message_id: pending.message_id,
            retry_at,
            reason,
        })
    }

    async fn stop_delivery(
        &self,
        work: StoredOutboundDeliveryWork,
        completed_at: i64,
        outcome: OutboundDeliveryWorkOutcome,
        error: Option<String>,
        reason: String,
    ) -> Result<DurableOutboundProcessOutcome, OutboundWorkflowError> {
        self.complete_work(&work, completed_at, outcome, error)
            .await?;
        Ok(DurableOutboundProcessOutcome::DeliveryStopped {
            subject: work.subject,
            message_id: work.message_id,
            outcome,
            reason,
        })
    }

    fn now_ms(&self) -> Result<i64, OutboundWorkflowError> {
        let now = self.clock.now_ms();
        if now < 0 {
            Err(OutboundWorkflowError::InvalidClock(
                "timestamp must be non-negative".to_owned(),
            ))
        } else {
            Ok(now)
        }
    }
}

fn command_delivery_process_outcome(
    work: StoredOutboundDeliveryWork,
    command_id: CommandId,
    disposition: DurableDeliveryDisposition,
    committed: CommandDeliveryCommitOutcome,
) -> DurableOutboundProcessOutcome {
    match committed {
        CommandDeliveryCommitOutcome::Applied => {
            DurableOutboundProcessOutcome::ExecutionCommandDelivered {
                command_id,
                message_id: work.message_id,
                disposition,
            }
        }
        CommandDeliveryCommitOutcome::Superseded { reason } => {
            DurableOutboundProcessOutcome::DeliveryStopped {
                subject: work.subject,
                message_id: work.message_id,
                outcome: OutboundDeliveryWorkOutcome::Superseded,
                reason,
            }
        }
    }
}

async fn apply_command_delivery_transition(
    transaction: &mut WriteTransaction,
    snapshot: &mut ExecutionProjectionSnapshot,
    command: &sinan_types::ExecutionCommand,
    dispatched_at: i64,
    uncertainty: Option<(i64, &str)>,
) -> Result<(), OutboundWorkflowError> {
    let current = snapshot
        .workflow
        .command_states
        .iter()
        .find(|state| state.command_id == command.command_id)
        .cloned()
        .ok_or_else(|| {
            OutboundWorkflowError::Invariant(format!(
                "execution projection omitted command {}",
                command.command_id
            ))
        })?;
    let Some(next) = next_delivery_state(command, &current, dispatched_at, uncertainty)? else {
        return Ok(());
    };
    transaction
        .update_execution_command_state(CommandStateUpdate {
            expected_status: current.status,
            expected_updated_at: current.updated_at,
            state: next.clone(),
        })
        .await?;
    let projected_state = snapshot
        .workflow
        .command_states
        .iter_mut()
        .find(|state| state.command_id == command.command_id)
        .ok_or_else(|| {
            OutboundWorkflowError::Invariant(
                "execution projection changed during update".to_owned(),
            )
        })?;
    *projected_state = next;
    let projection_at = uncertainty
        .map(|(observed_at, _)| observed_at)
        .unwrap_or(dispatched_at);
    persist_plan_projection(transaction, snapshot, projection_at).await
}

fn next_delivery_state(
    command: &sinan_types::ExecutionCommand,
    current: &ExecutionCommandState,
    dispatched_at: i64,
    uncertainty: Option<(i64, &str)>,
) -> Result<Option<ExecutionCommandState>, OutboundWorkflowError> {
    let mut next = current.clone();
    if next.status == ExecutionCommandStatus::Created {
        next = transition_command(
            command,
            &next,
            CommandEvidence::Dispatched { at: dispatched_at },
        )?
        .into_state();
    }
    if let Some((observed_at, error)) = uncertainty {
        if next.status == ExecutionCommandStatus::Dispatched {
            next = transition_command(
                command,
                &next,
                CommandEvidence::DeliveryUnconfirmed {
                    at: observed_at,
                    error,
                },
            )?
            .into_state();
        }
    }
    Ok((next != *current).then_some(next))
}

async fn apply_command_expiry_transition(
    transaction: &mut WriteTransaction,
    snapshot: &mut ExecutionProjectionSnapshot,
    command: &sinan_types::ExecutionCommand,
    expired_at: i64,
) -> Result<bool, OutboundWorkflowError> {
    let current = snapshot
        .workflow
        .command_states
        .iter()
        .find(|state| state.command_id == command.command_id)
        .cloned()
        .ok_or_else(|| {
            OutboundWorkflowError::Invariant(format!(
                "execution projection omitted command {}",
                command.command_id
            ))
        })?;
    if current.status != ExecutionCommandStatus::Created {
        return Ok(false);
    }
    let next = transition_command(
        command,
        &current,
        CommandEvidence::Expire { at: expired_at },
    )?
    .into_state();
    transaction
        .update_execution_command_state(CommandStateUpdate {
            expected_status: current.status,
            expected_updated_at: current.updated_at,
            state: next.clone(),
        })
        .await?;
    let projected_state = snapshot
        .workflow
        .command_states
        .iter_mut()
        .find(|state| state.command_id == command.command_id)
        .ok_or_else(|| {
            OutboundWorkflowError::Invariant(
                "execution projection changed during expiry".to_owned(),
            )
        })?;
    *projected_state = next;
    persist_plan_projection(transaction, snapshot, expired_at).await?;
    Ok(true)
}

async fn ensure_delivery_reconciliation(
    transaction: &mut WriteTransaction,
    work: &StoredOutboundDeliveryWork,
    command: &sinan_types::ExecutionCommand,
    observed_at: i64,
    requested_at: i64,
) -> Result<(), OutboundWorkflowError> {
    let request_id = RequestId::from(format!(
        "reconciliation:delivery-unconfirmed:{}",
        command.command_id
    ));
    let request = ReconciliationRequest {
        request_id: request_id.clone(),
        account_id: command.account_id.clone(),
        terminal_id: command.terminal_id.clone(),
        client_id: command.client_id.clone(),
        reason: ReconciliationReason::DeliveryUnconfirmed,
        command_ids: Some(vec![command.command_id.clone()]),
        since_server_time: Some(observed_at),
    };
    transaction
        .create_reconciliation_run(NewReconciliationRun {
            request,
            requested_at,
            event_metadata: CoreEventMetadata {
                event_id: format!("{}:requested", request_id),
                event_type: ExecutionClientMessageType::ReconciliationRequest.to_string(),
                aggregate_type: "reconciliation".to_owned(),
                aggregate_id: request_id.to_string(),
                message_id: Some(work.message_id.clone()),
                schema_version: SUPPORTED_SCHEMA_VERSION.to_string(),
                correlation_id: None,
                causation_id: Some(CausationId::from(work.message_id.as_str())),
                account_id: Some(command.account_id.clone()),
                client_id: command.client_id.clone(),
                terminal_id: command.terminal_id.clone(),
                strategy_id: None,
                intent_id: None,
                plan_id: None,
                leg_id: None,
                command_id: None,
                idempotency_key: None,
                event_at: requested_at,
                received_at: requested_at,
                created_at: requested_at,
                source: "core-outbound-delivery".to_owned(),
            },
        })
        .await?;
    Ok(())
}

async fn persist_plan_projection(
    transaction: &mut WriteTransaction,
    snapshot: &ExecutionProjectionSnapshot,
    evidence_at: i64,
) -> Result<(), OutboundWorkflowError> {
    let current = &snapshot.workflow.plan;
    let events: Vec<_> = snapshot
        .events
        .iter()
        .map(|event| event.event.clone())
        .collect();
    let projected = project_plan(&current.plan, &snapshot.workflow.command_states, &events)?;
    let mut changed = Vec::new();
    let mut latest_projection_at = current.updated_at;
    for projected_leg in &projected.legs {
        let current_leg = current
            .plan
            .legs
            .iter()
            .find(|leg| leg.definition.leg_id == projected_leg.definition.leg_id)
            .ok_or_else(|| {
                OutboundWorkflowError::Invariant(
                    "projected execution leg is outside the stored plan".to_owned(),
                )
            })?;
        if projected_leg.state == current_leg.state {
            continue;
        }
        let stored = transaction
            .get_execution_leg(&projected_leg.definition.leg_id)
            .await?
            .ok_or_else(|| {
                OutboundWorkflowError::Invariant("stored execution leg does not exist".to_owned())
            })?;
        latest_projection_at = latest_projection_at.max(stored.updated_at);
        changed.push((stored, projected_leg.state.clone()));
    }
    if changed.is_empty() {
        return Ok(());
    }
    let updated_at = latest_projection_at
        .max(evidence_at)
        .checked_add(1)
        .ok_or_else(|| {
            OutboundWorkflowError::Invariant("execution projection timestamp overflow".to_owned())
        })?;
    transaction
        .update_execution_lifecycle(ExecutionLifecycleUpdate {
            plan: PlanStateUpdate {
                plan_id: current.plan.definition.plan_id.clone(),
                expected_status: current.plan.state.status,
                expected_updated_at: current.updated_at,
                state: projected.state,
                updated_at,
            },
            legs: changed
                .into_iter()
                .map(|(stored, state)| LegStateUpdate {
                    plan_id: stored.plan_id,
                    leg_id: stored.leg.definition.leg_id,
                    expected_status: stored.leg.state.status,
                    expected_updated_at: stored.updated_at,
                    state,
                    updated_at,
                })
                .collect(),
        })
        .await?;
    Ok(())
}

fn validate_delivery_evidence(
    work: &StoredOutboundDeliveryWork,
    message_id: &sinan_types::MessageId,
    evidence_at: i64,
    finished_at: i64,
    expires_at: Option<i64>,
) -> Result<(), String> {
    if message_id != &work.message_id {
        return Err("delivery port returned evidence for a different message_id".to_owned());
    }
    if evidence_at < work.created_at || evidence_at > finished_at {
        return Err("delivery evidence timestamp is outside durable work bounds".to_owned());
    }
    if expires_at.is_some_and(|expires_at| evidence_at >= expires_at) {
        return Err("delivery port reported a send at or after command expiry".to_owned());
    }
    Ok(())
}

fn validate_unconfirmed_evidence(
    work: &StoredOutboundDeliveryWork,
    message_id: &sinan_types::MessageId,
    write_started_at: i64,
    observed_at: i64,
    finished_at: i64,
    expires_at: i64,
) -> Result<(), String> {
    validate_unconfirmed_evidence_without_expiry(
        work,
        message_id,
        write_started_at,
        observed_at,
        finished_at,
    )?;
    if write_started_at >= expires_at {
        return Err("delivery write began at or after command expiry".to_owned());
    }
    Ok(())
}

fn validate_unconfirmed_evidence_without_expiry(
    work: &StoredOutboundDeliveryWork,
    message_id: &sinan_types::MessageId,
    write_started_at: i64,
    observed_at: i64,
    finished_at: i64,
) -> Result<(), String> {
    if message_id != &work.message_id {
        return Err("delivery port returned evidence for a different message_id".to_owned());
    }
    if write_started_at < work.created_at
        || observed_at < write_started_at
        || observed_at > finished_at
    {
        return Err("uncertain delivery timestamps are outside durable work bounds".to_owned());
    }
    Ok(())
}

fn retry_delay(config: &DurableOutboundConfig, delivery_attempts: u64) -> i64 {
    let shift = u32::try_from(delivery_attempts.saturating_sub(1).min(62)).unwrap_or(62);
    config
        .retry_base_delay_ms
        .checked_mul(1_i64.checked_shl(shift).unwrap_or(i64::MAX))
        .unwrap_or(i64::MAX)
        .min(config.retry_max_delay_ms)
}
