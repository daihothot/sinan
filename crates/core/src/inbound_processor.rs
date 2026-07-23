use std::collections::BTreeSet;

use serde::de::DeserializeOwned;
use sinan_execution::{
    project_plan, transition_command, CommandEvidence, CommandTransitionOutcome,
};
use sinan_gateway::{
    DurableInboundHandler, DurableInboundHandlerFuture, DurableRecoveryHandlerError,
    DurableSessionResumeHandler, DurableSessionResumeHandlerFuture, SessionResumeHandlingOutcome,
};
use sinan_protocol::{
    decode_wire_message, CommandInboxStatus, CommandReceived, ExecutionClientMessageType,
    MarketTick, ProtocolReason, ReconciliationReason, ReconciliationResult, ResumeCursor,
    TransportAck, TransportAckStatus, WireMessage, SUPPORTED_SCHEMA_VERSION,
};
use sinan_reconciliation::{
    build_reconciliation_request, evaluate_reconciliation_result, ReconciliationCommand,
    ReconciliationDisposition as DomainReconciliationDisposition, ReconciliationRequestInput,
};
use sinan_store::{
    CanonicalJson, CommandReceivedAttemptUpdate, CommandStateUpdate, CoreEventMetadata,
    DeadletterReason, ExecutionLifecycleUpdate, ExecutionProjectionSnapshot, LegStateUpdate,
    NewCoreEvent, NewExecutionEvent, NewReconciliationResult, NewReconciliationRun,
    PlanStateUpdate, ReconciliationCompleteness, ReconciliationDisposition,
    ReconciliationEvaluation, StoreError, StoredInboundAdmission, StoredOutboundDelivery,
    StoredSessionResumeAdmission, TransportAckUpdate, WriteOutcome, WriteTransaction,
};
use sinan_types::{
    AccountId, CausationId, CommandDeliveryAttemptStatus, CommandId, ExecutionCommandState,
    ExecutionCommandStatus, ExecutionEvent, MarketBar, MarketSnapshot, MessageId, OrderSnapshot,
    RequestId, SymbolMetadataSnapshot, WireOutboxStatus,
};

const EXECUTION_CLIENT_SOURCE: &str = "execution-client";

#[derive(Clone, Copy, Debug, Default)]
pub struct CoreInboundProcessor;

impl DurableInboundHandler for CoreInboundProcessor {
    fn handle<'a>(
        &'a self,
        transaction: &'a mut WriteTransaction,
        admission: &'a StoredInboundAdmission,
    ) -> DurableInboundHandlerFuture<'a> {
        Box::pin(async move { handle_inbound(transaction, admission).await })
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct CoreSessionResumeProcessor;

impl DurableSessionResumeHandler for CoreSessionResumeProcessor {
    fn handle<'a>(
        &'a self,
        transaction: &'a mut WriteTransaction,
        admission: &'a StoredSessionResumeAdmission,
    ) -> DurableSessionResumeHandlerFuture<'a> {
        Box::pin(async move { handle_session_resume(transaction, admission).await })
    }
}

async fn handle_inbound(
    transaction: &mut WriteTransaction,
    admission: &StoredInboundAdmission,
) -> Result<(), DurableRecoveryHandlerError> {
    let message_type = admission
        .message_type
        .parse::<ExecutionClientMessageType>()
        .map_err(|error| {
            DurableRecoveryHandlerError::terminal_deadletter(
                DeadletterReason::UnknownType,
                error.to_string(),
            )
        })?;
    match message_type {
        ExecutionClientMessageType::TransportAck => {
            handle_transport_ack(transaction, admission).await
        }
        ExecutionClientMessageType::MarketTick => handle_market_tick(transaction, admission).await,
        ExecutionClientMessageType::MarketBar => {
            let message = decode_admission::<MarketBar>(admission, message_type)?;
            let metadata = snapshot_metadata(
                admission,
                &message,
                message.payload.timestamp,
                "market",
                format!(
                    "{}:{}:{}",
                    admission.account_id, message.payload.symbol, message.payload.timestamp
                ),
            );
            transaction
                .ingest_market_bar(metadata, &message.payload)
                .await
                .map_err(store_error)?;
            Ok(())
        }
        ExecutionClientMessageType::SymbolMetadata => {
            let message = decode_admission::<SymbolMetadataSnapshot>(admission, message_type)?;
            require_account(admission, &message.payload.account_id)?;
            let metadata = snapshot_metadata(
                admission,
                &message,
                message.payload.observed_at,
                "symbol",
                message.payload.symbol.to_string(),
            );
            transaction
                .ingest_symbol_metadata(metadata, &message.payload)
                .await
                .map_err(store_error)?;
            Ok(())
        }
        ExecutionClientMessageType::AccountSnapshot => {
            let message =
                decode_admission::<sinan_types::AccountSnapshot>(admission, message_type)?;
            require_account(admission, &message.payload.account_id)?;
            let metadata = snapshot_metadata(
                admission,
                &message,
                message.payload.observed_at,
                "account",
                message.payload.account_id.to_string(),
            );
            transaction
                .ingest_account_snapshot(metadata, &message.payload)
                .await
                .map_err(store_error)?;
            Ok(())
        }
        ExecutionClientMessageType::PositionSnapshot => {
            let message =
                decode_admission::<sinan_types::PositionSnapshot>(admission, message_type)?;
            require_account(admission, &message.payload.account_id)?;
            let metadata = snapshot_metadata(
                admission,
                &message,
                message.payload.observed_at,
                "position",
                message.payload.position_id.to_string(),
            );
            transaction
                .ingest_position_snapshot(metadata, &message.payload)
                .await
                .map_err(store_error)?;
            Ok(())
        }
        ExecutionClientMessageType::OrderSnapshot => {
            let message = decode_admission::<OrderSnapshot>(admission, message_type)?;
            require_account(admission, &message.payload.account_id)?;
            require_optional_route(
                admission,
                message.payload.client_id.as_ref(),
                message.payload.terminal_id.as_ref(),
            )?;
            let metadata = snapshot_metadata(
                admission,
                &message,
                message.payload.observed_at,
                "order",
                message.payload.broker_order_id.to_string(),
            );
            transaction
                .ingest_order_snapshot(metadata, &message.payload)
                .await
                .map_err(store_error)?;
            Ok(())
        }
        ExecutionClientMessageType::CommandReceived => {
            handle_command_received(transaction, admission).await
        }
        ExecutionClientMessageType::ExecutionEvent => {
            handle_execution_event(transaction, admission).await
        }
        ExecutionClientMessageType::ReconciliationResult => {
            handle_reconciliation_result(transaction, admission).await
        }
        _ => Err(terminal_error(format!(
            "message type {message_type} has no durable inbound owner"
        ))),
    }
}

async fn handle_transport_ack(
    transaction: &mut WriteTransaction,
    admission: &StoredInboundAdmission,
) -> Result<(), DurableRecoveryHandlerError> {
    let message =
        decode_admission::<TransportAck>(admission, ExecutionClientMessageType::TransportAck)?;
    if message.payload.received_at < 0 || message.payload.received_at > admission.received_at {
        return Err(terminal_error(
            "transport.ack received_at is outside the trusted receive-time boundary",
        ));
    }
    match message.payload.status {
        TransportAckStatus::Accepted | TransportAckStatus::Duplicate
            if message
                .payload
                .reason
                .as_ref()
                .is_some_and(|reason| !matches!(reason, ProtocolReason::Ok)) =>
        {
            return Err(terminal_error(
                "accepted transport.ack carries a rejection reason",
            ));
        }
        TransportAckStatus::Rejected
            if !matches!(
                message.payload.reason.as_ref(),
                Some(ProtocolReason::Error(_))
            ) =>
        {
            return Err(terminal_error(
                "rejected transport.ack requires a typed error reason",
            ));
        }
        _ => {}
    }
    if message
        .causation_id
        .as_ref()
        .is_some_and(|value| value.as_str() != message.payload.acked_message_id.as_str())
    {
        return Err(terminal_error(
            "transport.ack causation_id differs from acked_message_id",
        ));
    }
    let reason = message.payload.reason.as_ref().map(protocol_reason_name);
    transaction
        .record_transport_ack(TransportAckUpdate {
            message_id: message.payload.acked_message_id,
            session_id: admission.session_id.clone(),
            message_type: message.payload.acked_message_type.to_string(),
            status: message.payload.status,
            reason,
            acked_at: message.payload.received_at,
        })
        .await
        .map_err(store_error)?;
    Ok(())
}

async fn handle_market_tick(
    transaction: &mut WriteTransaction,
    admission: &StoredInboundAdmission,
) -> Result<(), DurableRecoveryHandlerError> {
    let message =
        decode_admission::<MarketTick>(admission, ExecutionClientMessageType::MarketTick)?;
    let tick = message.payload;
    require_account(admission, &tick.account_id)?;
    if tick.observed_at < 0
        || !tick.bid.is_finite()
        || !tick.ask.is_finite()
        || tick.bid <= 0.0
        || tick.ask < tick.bid
        || tick
            .last
            .is_some_and(|value| !value.is_finite() || value <= 0.0)
        || tick
            .volume
            .is_some_and(|value| !value.is_finite() || value < 0.0)
    {
        return Err(terminal_error(
            "market.tick contains invalid price or time data",
        ));
    }
    let snapshot = MarketSnapshot {
        symbol: tick.symbol,
        broker_symbol: tick.broker_symbol,
        bid: tick.bid,
        ask: tick.ask,
        spread: tick.ask - tick.bid,
        observed_at: tick.observed_at,
    };
    transaction
        .update_market_snapshot(&tick.account_id, &snapshot, admission.received_at)
        .await
        .map_err(store_error)?;
    Ok(())
}

async fn handle_command_received(
    transaction: &mut WriteTransaction,
    admission: &StoredInboundAdmission,
) -> Result<(), DurableRecoveryHandlerError> {
    let message = decode_admission::<CommandReceived>(
        admission,
        ExecutionClientMessageType::CommandReceived,
    )?;
    let receipt = &message.payload;
    require_account(admission, &receipt.account_id)?;
    require_optional_route(
        admission,
        receipt.client_id.as_ref(),
        receipt.terminal_id.as_ref(),
    )?;
    let source_message_id = message
        .causation_id
        .as_ref()
        .map(|value| MessageId::from(value.as_str()))
        .ok_or_else(|| {
            terminal_error("command.received requires execution.command causation_id")
        })?;
    let delivery = transaction
        .get_outbound_delivery(&source_message_id)
        .await
        .map_err(store_error)?
        .ok_or_else(|| terminal_error("command.received source delivery does not exist"))?;
    if delivery.outbox.message_type != ExecutionClientMessageType::ExecutionCommand.as_str()
        || delivery.outbox.session_id.as_ref() != Some(&admission.session_id)
        || delivery.attempt.subject.command_id() != Some(&receipt.command_id)
    {
        return Err(terminal_error(
            "command.received causation does not identify its execution.command delivery",
        ));
    }

    let mut snapshot = transaction
        .load_execution_projection(&receipt.command_id)
        .await
        .map_err(store_error)?
        .ok_or_else(|| terminal_error("command.received command projection does not exist"))?;
    let command = snapshot_command(&snapshot, &receipt.command_id)?.clone();
    if receipt.idempotency_key != command.command.idempotency_key
        || receipt.account_id != command.command.account_id
        || receipt.client_id != command.command.client_id
        || receipt.terminal_id != command.command.terminal_id
    {
        return Err(terminal_error(
            "command.received identity differs from its immutable execution.command",
        ));
    }
    if receipt.received_at < 0 || receipt.received_at > admission.received_at {
        return Err(terminal_error(
            "command.received timestamp is outside the trusted receive-time boundary",
        ));
    }
    match receipt.inbox_status {
        CommandInboxStatus::Recorded | CommandInboxStatus::Duplicate
            if receipt
                .reason
                .as_ref()
                .is_some_and(|reason| !matches!(reason, ProtocolReason::Ok)) =>
        {
            return Err(terminal_error(
                "accepted command.received carries a rejection reason",
            ));
        }
        CommandInboxStatus::Expired | CommandInboxStatus::Rejected if receipt.reason.is_none() => {
            return Err(terminal_error(
                "rejected command.received requires a durable reason",
            ));
        }
        _ => {}
    }
    transaction
        .append_core_event(NewCoreEvent {
            metadata: command_event_metadata(
                admission,
                &message,
                &command,
                receipt.received_at,
                ExecutionClientMessageType::CommandReceived.as_str(),
            ),
            payload: CanonicalJson::from_serializable(receipt).map_err(terminal_error)?,
        })
        .await
        .map_err(store_error)?;

    let evidence = match receipt.inbox_status {
        CommandInboxStatus::Recorded => Some(CommandEvidence::ReceivedRecorded(receipt)),
        CommandInboxStatus::Duplicate => {
            Some(CommandEvidence::ReceivedDuplicateKnownSamePayload(receipt))
        }
        CommandInboxStatus::Expired | CommandInboxStatus::Rejected => None,
    };
    let Some(evidence) = evidence else {
        return Ok(());
    };
    let dispatched_at = delivery_dispatch_evidence_at(&delivery, receipt.received_at)?;

    transaction
        .record_command_received_attempt(CommandReceivedAttemptUpdate {
            source_message_id,
            session_id: admission.session_id.clone(),
            command_id: receipt.command_id.clone(),
            received_at: receipt.received_at,
        })
        .await
        .map_err(store_error)?;

    let current = snapshot_state(&snapshot, &receipt.command_id)?.clone();
    if current.status == ExecutionCommandStatus::Created {
        let transition = transition_command(
            &command.command,
            &current,
            CommandEvidence::Dispatched { at: dispatched_at },
        )
        .map_err(terminal_error)?;
        persist_command_transition(
            transaction,
            &mut snapshot,
            &receipt.command_id,
            current,
            transition,
            dispatched_at,
        )
        .await?;
    }

    let current = snapshot_state(&snapshot, &receipt.command_id)?.clone();
    let transition =
        transition_command(&command.command, &current, evidence).map_err(terminal_error)?;
    persist_command_transition(
        transaction,
        &mut snapshot,
        &receipt.command_id,
        current,
        transition,
        receipt.received_at,
    )
    .await
}

fn delivery_dispatch_evidence_at(
    delivery: &StoredOutboundDelivery,
    received_at: i64,
) -> Result<i64, DurableRecoveryHandlerError> {
    let may_have_reached_transport = matches!(
        delivery.outbox.status,
        WireOutboxStatus::WriteStarted | WireOutboxStatus::Sent | WireOutboxStatus::Acked
    ) || delivery.outbox.sent_at.is_some()
        || matches!(
            delivery.attempt.status,
            CommandDeliveryAttemptStatus::Sent
                | CommandDeliveryAttemptStatus::Acked
                | CommandDeliveryAttemptStatus::Unconfirmed
        );
    if !may_have_reached_transport {
        return Err(terminal_error(
            "command.received source delivery has no transport-write evidence",
        ));
    }
    let dispatched_at = delivery
        .outbox
        .created_at
        .max(delivery.attempt.attempted_at);
    if dispatched_at < 0 || dispatched_at > received_at {
        return Err(terminal_error(
            "command.received predates its durable dispatch evidence",
        ));
    }
    Ok(dispatched_at)
}

async fn handle_execution_event(
    transaction: &mut WriteTransaction,
    admission: &StoredInboundAdmission,
) -> Result<(), DurableRecoveryHandlerError> {
    let message =
        decode_admission::<ExecutionEvent>(admission, ExecutionClientMessageType::ExecutionEvent)?;
    let event = &message.payload;
    require_account(admission, &event.account_id)?;
    require_optional_route(
        admission,
        event.client_id.as_ref(),
        event.terminal_id.as_ref(),
    )?;
    let mut snapshot = transaction
        .load_execution_projection(&event.command_id)
        .await
        .map_err(store_error)?
        .ok_or_else(|| terminal_error("execution.event command projection does not exist"))?;
    let command = snapshot_command(&snapshot, &event.command_id)?.clone();

    transaction
        .append_core_event(NewCoreEvent {
            metadata: command_event_metadata(
                admission,
                &message,
                &command,
                event.event_at,
                ExecutionClientMessageType::ExecutionEvent.as_str(),
            ),
            payload: CanonicalJson::from_serializable(event).map_err(terminal_error)?,
        })
        .await
        .map_err(store_error)?;
    let event_outcome = transaction
        .append_execution_event(NewExecutionEvent {
            event: event.clone(),
            created_at: admission.received_at,
        })
        .await
        .map_err(store_error)?;
    if matches!(event_outcome, WriteOutcome::Duplicate(_)) {
        return Ok(());
    }
    snapshot.events.push(event_outcome.into_record());

    let current = snapshot_state(&snapshot, &event.command_id)?.clone();
    let transition = transition_command(
        &command.command,
        &current,
        CommandEvidence::ExecutionEvent(event),
    )
    .map_err(terminal_error)?;
    persist_command_transition(
        transaction,
        &mut snapshot,
        &event.command_id,
        current,
        transition,
        event.event_at,
    )
    .await
}

async fn persist_command_transition(
    transaction: &mut WriteTransaction,
    snapshot: &mut ExecutionProjectionSnapshot,
    command_id: &CommandId,
    current: ExecutionCommandState,
    transition: CommandTransitionOutcome,
    evidence_at: i64,
) -> Result<(), DurableRecoveryHandlerError> {
    let next = transition.into_state();
    if next != current {
        transaction
            .update_execution_command_state(CommandStateUpdate {
                expected_status: current.status,
                expected_updated_at: current.updated_at,
                state: next.clone(),
            })
            .await
            .map_err(store_error)?;
        let stored = snapshot
            .workflow
            .command_states
            .iter_mut()
            .find(|state| state.command_id == *command_id)
            .ok_or_else(|| terminal_error("execution workflow lost its command state"))?;
        *stored = next;
    }
    persist_plan_projection(transaction, snapshot, evidence_at).await
}

async fn persist_plan_projection(
    transaction: &mut WriteTransaction,
    snapshot: &mut ExecutionProjectionSnapshot,
    evidence_at: i64,
) -> Result<(), DurableRecoveryHandlerError> {
    let current = &snapshot.workflow.plan;
    let events: Vec<_> = snapshot
        .events
        .iter()
        .map(|event| event.event.clone())
        .collect();
    let projected = project_plan(&current.plan, &snapshot.workflow.command_states, &events)
        .map_err(terminal_error)?;

    let mut changed = Vec::new();
    let mut latest_projection_at = current.updated_at;
    for projected_leg in &projected.legs {
        let current_leg = current
            .plan
            .legs
            .iter()
            .find(|leg| leg.definition.leg_id == projected_leg.definition.leg_id)
            .ok_or_else(|| terminal_error("projected execution leg is outside the stored plan"))?;
        if projected_leg.state == current_leg.state {
            continue;
        }
        let stored = transaction
            .get_execution_leg(&projected_leg.definition.leg_id)
            .await
            .map_err(store_error)?
            .ok_or_else(|| terminal_error("stored execution leg does not exist"))?;
        latest_projection_at = latest_projection_at.max(stored.updated_at);
        changed.push((stored, projected_leg.state.clone()));
    }
    if changed.is_empty() {
        return Ok(());
    }
    let updated_at = latest_projection_at
        .max(evidence_at)
        .checked_add(1)
        .ok_or_else(|| terminal_error("execution projection timestamp overflow"))?;
    let updated = transaction
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
        .await
        .map_err(store_error)?;
    snapshot.workflow.plan = updated;
    Ok(())
}

async fn handle_reconciliation_result(
    transaction: &mut WriteTransaction,
    admission: &StoredInboundAdmission,
) -> Result<(), DurableRecoveryHandlerError> {
    let message = decode_admission::<ReconciliationResult>(
        admission,
        ExecutionClientMessageType::ReconciliationResult,
    )?;
    let result = message.payload;
    require_account(admission, &result.account_id)?;
    require_optional_route(
        admission,
        result.client_id.as_ref(),
        result.terminal_id.as_ref(),
    )?;
    let snapshot = transaction
        .load_reconciliation_evaluation_snapshot(&result.request_id)
        .await
        .map_err(store_error)?
        .ok_or_else(|| terminal_error("reconciliation.result request does not exist"))?;
    if let Some(existing) = &snapshot.run.result {
        let existing = CanonicalJson::from_serializable(existing).map_err(terminal_error)?;
        let incoming = CanonicalJson::from_serializable(&result).map_err(terminal_error)?;
        return if existing == incoming {
            Ok(())
        } else {
            Err(terminal_error(
                "reconciliation.result conflicts with the completed durable run",
            ))
        };
    }
    let context = sinan_reconciliation::ReconciliationRequestContext {
        request: snapshot.run.request.clone(),
        requested_at: snapshot.run.requested_at,
    };
    let commands: Vec<_> = snapshot
        .commands
        .iter()
        .map(|item| ReconciliationCommand {
            command: item.command.command.clone(),
            state: item.state.clone(),
        })
        .collect();
    let events: Vec<_> = snapshot
        .events
        .iter()
        .map(|item| item.event.clone())
        .collect();
    let evaluated =
        evaluate_reconciliation_result(&context, result, &commands, &events, admission.received_at)
            .map_err(terminal_error)?;
    let evaluation = ReconciliationEvaluation {
        request_id: evaluated.evaluation.request_id,
        account_id: evaluated.evaluation.account_id,
        observed_at: evaluated.evaluation.observed_at,
        disposition: match evaluated.evaluation.disposition {
            DomainReconciliationDisposition::Completed => ReconciliationDisposition::Completed,
            DomainReconciliationDisposition::PendingEvidence => {
                ReconciliationDisposition::PendingEvidence
            }
            DomainReconciliationDisposition::ManualRequired => {
                ReconciliationDisposition::ManualRequired
            }
        },
        command_ids: evaluated.evaluation.command_ids,
        findings: evaluated
            .evaluation
            .findings
            .into_iter()
            .map(|finding| serde_json::to_value(finding).map_err(terminal_error))
            .collect::<Result<_, _>>()?,
    };
    let command_scope_complete = context.request.command_ids.is_none()
        && context.request.client_id.is_none()
        && context.request.terminal_id.is_none();
    let event_at = evaluated.result.observed_at;
    transaction
        .commit_reconciliation_result(NewReconciliationResult {
            result: evaluated.result,
            evaluation,
            completeness: ReconciliationCompleteness {
                symbol_metadata_complete: false,
                command_scope_complete,
            },
            event_metadata: CoreEventMetadata {
                event_id: inbound_event_id(&admission.message_id),
                event_type: ExecutionClientMessageType::ReconciliationResult.to_string(),
                aggregate_type: "reconciliation".to_owned(),
                aggregate_id: context.request.request_id.to_string(),
                message_id: Some(admission.message_id.clone()),
                schema_version: admission.schema_version.clone(),
                correlation_id: admission.correlation_id.clone(),
                causation_id: admission.causation_id.clone(),
                account_id: Some(context.request.account_id.clone()),
                client_id: context.request.client_id.clone(),
                terminal_id: context.request.terminal_id.clone(),
                strategy_id: None,
                intent_id: None,
                plan_id: None,
                leg_id: None,
                command_id: None,
                idempotency_key: None,
                event_at,
                received_at: admission.received_at,
                created_at: admission.received_at,
                source: EXECUTION_CLIENT_SOURCE.to_owned(),
            },
        })
        .await
        .map_err(store_error)?;
    Ok(())
}

async fn handle_session_resume(
    transaction: &mut WriteTransaction,
    admission: &StoredSessionResumeAdmission,
) -> Result<SessionResumeHandlingOutcome, DurableRecoveryHandlerError> {
    let cursor: ResumeCursor =
        serde_json::from_str(admission.cursor.as_str()).map_err(terminal_error)?;
    normalized_pending_commands(cursor.pending_command_ids)?;
    let request_id = RequestId::new(format!(
        "reconciliation:resume:{}",
        admission.hello_message_id
    ));
    let context = build_reconciliation_request(ReconciliationRequestInput {
        request_id: request_id.clone(),
        account_id: admission.account_id.clone(),
        terminal_id: admission.terminal_id.clone(),
        client_id: Some(admission.client_id.clone()),
        reason: ReconciliationReason::ConnectionRestored,
        command_ids: None,
        since_server_time: None,
        requested_at: admission.received_at,
    })
    .map_err(terminal_error)?;
    transaction
        .create_reconciliation_run(NewReconciliationRun {
            request: context.request,
            requested_at: context.requested_at,
            event_metadata: CoreEventMetadata {
                event_id: format!(
                    "resume:{}:reconciliation.request",
                    admission.hello_message_id
                ),
                event_type: ExecutionClientMessageType::ReconciliationRequest.to_string(),
                aggregate_type: "reconciliation".to_owned(),
                aggregate_id: request_id.to_string(),
                message_id: Some(admission.hello_message_id.clone()),
                schema_version: "ecp.v1.0".to_owned(),
                correlation_id: None,
                causation_id: Some(CausationId::from(admission.hello_message_id.as_str())),
                account_id: Some(admission.account_id.clone()),
                client_id: Some(admission.client_id.clone()),
                terminal_id: admission.terminal_id.clone(),
                strategy_id: None,
                intent_id: None,
                plan_id: None,
                leg_id: None,
                command_id: None,
                idempotency_key: None,
                event_at: admission.received_at,
                received_at: admission.received_at,
                created_at: admission.received_at,
                source: "gateway-session-resume".to_owned(),
            },
        })
        .await
        .map_err(store_error)?;
    Ok(SessionResumeHandlingOutcome {
        reconciliation_request_id: Some(request_id),
    })
}

fn decode_admission<T: DeserializeOwned>(
    admission: &StoredInboundAdmission,
    expected_type: ExecutionClientMessageType,
) -> Result<WireMessage<T>, DurableRecoveryHandlerError> {
    let message = decode_wire_message::<T>(
        admission.envelope.as_str().as_bytes(),
        SUPPORTED_SCHEMA_VERSION,
    )
    .map_err(terminal_error)?;
    if message.message_type != expected_type
        || admission.message_type != expected_type.as_str()
        || message.message_id != admission.message_id
        || message.session_id.as_ref() != Some(&admission.session_id)
        || message
            .client_id
            .as_ref()
            .is_some_and(|client_id| client_id != &admission.client_id)
        || message.schema_version != admission.schema_version
        || message.sequence != Some(admission.sequence)
        || message.correlation_id != admission.correlation_id
        || message.causation_id != admission.causation_id
    {
        return Err(terminal_error(
            "durable admission identity differs from its canonical envelope",
        ));
    }
    Ok(message)
}

fn snapshot_metadata<T>(
    admission: &StoredInboundAdmission,
    message: &WireMessage<T>,
    event_at: i64,
    aggregate_type: &str,
    aggregate_id: String,
) -> CoreEventMetadata {
    CoreEventMetadata {
        event_id: inbound_event_id(&admission.message_id),
        event_type: message.message_type.to_string(),
        aggregate_type: aggregate_type.to_owned(),
        aggregate_id,
        message_id: Some(admission.message_id.clone()),
        schema_version: admission.schema_version.clone(),
        correlation_id: admission.correlation_id.clone(),
        causation_id: admission.causation_id.clone(),
        account_id: Some(admission.account_id.clone()),
        client_id: Some(admission.client_id.clone()),
        terminal_id: admission.terminal_id.clone(),
        strategy_id: None,
        intent_id: None,
        plan_id: None,
        leg_id: None,
        command_id: None,
        idempotency_key: None,
        event_at,
        received_at: admission.received_at,
        created_at: admission.received_at,
        source: EXECUTION_CLIENT_SOURCE.to_owned(),
    }
}

fn command_event_metadata<T>(
    admission: &StoredInboundAdmission,
    message: &WireMessage<T>,
    command: &sinan_store::StoredExecutionCommand,
    event_at: i64,
    event_type: &str,
) -> CoreEventMetadata {
    CoreEventMetadata {
        event_id: inbound_event_id(&admission.message_id),
        event_type: event_type.to_owned(),
        aggregate_type: "execution.command".to_owned(),
        aggregate_id: command.command.command_id.to_string(),
        message_id: Some(admission.message_id.clone()),
        schema_version: admission.schema_version.clone(),
        correlation_id: message.correlation_id.clone(),
        causation_id: message.causation_id.clone(),
        account_id: Some(command.command.account_id.clone()),
        client_id: command.command.client_id.clone(),
        terminal_id: command.command.terminal_id.clone(),
        strategy_id: Some(command.command.strategy_id.clone()),
        intent_id: None,
        plan_id: command.command.plan_id.clone(),
        leg_id: command.command.leg_id.clone(),
        command_id: Some(command.command.command_id.clone()),
        idempotency_key: Some(command.command.idempotency_key.clone()),
        event_at,
        received_at: admission.received_at,
        created_at: admission.received_at,
        source: EXECUTION_CLIENT_SOURCE.to_owned(),
    }
}

fn snapshot_command<'a>(
    snapshot: &'a ExecutionProjectionSnapshot,
    command_id: &CommandId,
) -> Result<&'a sinan_store::StoredExecutionCommand, DurableRecoveryHandlerError> {
    snapshot
        .workflow
        .commands
        .iter()
        .find(|command| command.command.command_id == *command_id)
        .ok_or_else(|| terminal_error("execution snapshot does not contain the command"))
}

fn snapshot_state<'a>(
    snapshot: &'a ExecutionProjectionSnapshot,
    command_id: &CommandId,
) -> Result<&'a ExecutionCommandState, DurableRecoveryHandlerError> {
    snapshot
        .workflow
        .command_states
        .iter()
        .find(|state| state.command_id == *command_id)
        .ok_or_else(|| terminal_error("execution snapshot does not contain the command state"))
}

fn require_account(
    admission: &StoredInboundAdmission,
    account_id: &AccountId,
) -> Result<(), DurableRecoveryHandlerError> {
    if &admission.account_id == account_id {
        Ok(())
    } else {
        Err(terminal_error(
            "payload account_id differs from the authenticated admission route",
        ))
    }
}

fn require_optional_route(
    admission: &StoredInboundAdmission,
    client_id: Option<&sinan_types::ClientId>,
    terminal_id: Option<&sinan_types::TerminalId>,
) -> Result<(), DurableRecoveryHandlerError> {
    if client_id.is_some_and(|value| value != &admission.client_id)
        || terminal_id.is_some_and(|value| Some(value) != admission.terminal_id.as_ref())
    {
        Err(terminal_error(
            "payload route differs from the authenticated admission route",
        ))
    } else {
        Ok(())
    }
}

fn normalized_pending_commands(
    command_ids: Option<Vec<CommandId>>,
) -> Result<Option<Vec<CommandId>>, DurableRecoveryHandlerError> {
    let Some(command_ids) = command_ids.filter(|values| !values.is_empty()) else {
        return Ok(None);
    };
    let unique: BTreeSet<_> = command_ids.iter().cloned().collect();
    if unique.len() != command_ids.len() {
        return Err(terminal_error(
            "resume pending_command_ids contains duplicate command identities",
        ));
    }
    Ok(Some(unique.into_iter().collect()))
}

fn protocol_reason_name(reason: &ProtocolReason) -> String {
    match reason {
        ProtocolReason::Ok => "OK".to_owned(),
        ProtocolReason::Error(error) => error.as_str().to_owned(),
    }
}

fn inbound_event_id(message_id: &MessageId) -> String {
    format!("inbound:{message_id}")
}

fn terminal_error(error: impl std::fmt::Display) -> DurableRecoveryHandlerError {
    DurableRecoveryHandlerError::terminal_deadletter(
        DeadletterReason::SchemaValidationFailed,
        error.to_string(),
    )
}

fn store_error(error: StoreError) -> DurableRecoveryHandlerError {
    match error {
        terminal @ (StoreError::IdentityConflict { .. }
        | StoreError::ObservationConflict { .. }
        | StoreError::NotFound { .. }
        | StoreError::InvalidRecord { .. }
        | StoreError::InvalidInteger { .. }
        | StoreError::InvalidSequence { .. }
        | StoreError::Serialization(_)) => terminal_error(terminal),
        retryable @ (StoreError::StaleWrite { .. }
        | StoreError::CorruptData { .. }
        | StoreError::SnapshotUnavailable { .. }
        | StoreError::Initialization(_)
        | StoreError::Database(_)) => {
            DurableRecoveryHandlerError::retryable_infrastructure(retryable.to_string())
        }
    }
}
