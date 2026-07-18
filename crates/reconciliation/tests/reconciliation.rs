use sinan_execution::CommandTransitionOutcome;
use sinan_protocol::{ReconciliationReason, ReconciliationResult};
use sinan_reconciliation::{
    build_reconciliation_request, escalate_manual_reconciliation,
    escalate_missing_reconciliation_result, evaluate_reconciliation_result,
    plan_reconciliation_request, ManualEscalationEvidence, ReconciliationCommand,
    ReconciliationDisposition, ReconciliationError, ReconciliationFinding,
    ReconciliationRequestInput,
};
use sinan_types::{
    AccountId, AccountSnapshot, BrokerOrderId, ClientId, CommandId, ErrorCodeOrString,
    ExecutionAction, ExecutionCommand, ExecutionCommandState, ExecutionCommandStatus,
    ExecutionEvent, ExecutionEventStatus, ExecutionId, IdempotencyKey, LegId, OrderSnapshot,
    OrderSnapshotStatus, OrderType, PlanId, PositionId, PositionSide, PositionSnapshot, RequestId,
    StrategyId, SymbolCode, SymbolMetadataSnapshot, SymbolTradeMode, TerminalId,
};

const CREATED_AT: i64 = 900;
const REQUESTED_AT: i64 = 1_000;
const OBSERVED_AT: i64 = 1_100;
const RECEIVED_AT: i64 = 1_200;

#[test]
fn targeted_request_is_sorted_and_only_eligible_commands_enter_reconciling() {
    let uncertain = command_context("cmd-a", ExecutionCommandStatus::DeliveryUnconfirmed);
    let mut already_received = command_context("cmd-b", ExecutionCommandStatus::CommandReceived);
    already_received.state.updated_at = 995;
    let plan = plan_reconciliation_request(
        request_input(Some(vec![
            CommandId::from("cmd-b"),
            CommandId::from("cmd-a"),
        ])),
        &[already_received, uncertain],
    )
    .expect("request plan");

    assert_eq!(
        plan.context.request.command_ids,
        Some(vec![CommandId::from("cmd-a"), CommandId::from("cmd-b")])
    );
    assert_eq!(plan.command_transitions.len(), 1);
    assert_eq!(plan.command_transitions[0].command_id.as_str(), "cmd-a");
    assert_eq!(
        plan.command_transitions[0].outcome.state().status,
        ExecutionCommandStatus::Reconciling
    );
}

#[test]
fn account_wide_request_keeps_none_scope_and_does_not_regress_advanced_states() {
    let mut context = command_context("cmd-a", ExecutionCommandStatus::OrderSent);
    context.state.updated_at = 995;
    let plan = plan_reconciliation_request(request_input(None), &[context]).expect("plan");

    assert_eq!(plan.context.request.command_ids, None);
    assert!(plan.command_transitions.is_empty());
}

#[test]
fn targeted_scope_rejects_empty_and_duplicate_ids() {
    let empty = build_reconciliation_request(request_input(Some(Vec::new())))
        .expect_err("empty Some must fail");
    assert!(matches!(
        empty,
        ReconciliationError::InvalidRequest {
            field: "command_ids",
            ..
        }
    ));

    let duplicate = build_reconciliation_request(request_input(Some(vec![
        CommandId::from("cmd-a"),
        CommandId::from("cmd-a"),
    ])))
    .expect_err("duplicate must fail");
    assert!(matches!(
        duplicate,
        ReconciliationError::InvalidRequest {
            field: "command_ids",
            ..
        }
    ));
}

#[test]
fn targeted_scope_requires_exact_command_context() {
    let request = build_reconciliation_request(request_input(Some(vec![CommandId::from("cmd-a")])))
        .expect("request");
    let result = result_for(&request, Vec::new());
    let error = evaluate_reconciliation_result(
        &request,
        result,
        &[command_context(
            "cmd-b",
            ExecutionCommandStatus::DeliveryUnconfirmed,
        )],
        &[],
        RECEIVED_AT,
    )
    .expect_err("wrong command context must fail");

    assert!(matches!(error, ReconciliationError::InvalidContext { .. }));
}

#[test]
fn result_requires_exact_request_route_and_server_time_window() {
    let request = request();
    let mut wrong_route = result_for(&request, Vec::new());
    wrong_route.client_id = Some(ClientId::from("other-client"));
    assert!(matches!(
        evaluate_reconciliation_result(&request, wrong_route, &[], &[], RECEIVED_AT),
        Err(ReconciliationError::InvalidResult {
            field: "client_id",
            ..
        })
    ));

    let mut stale = result_for(&request, Vec::new());
    stale.observed_at = REQUESTED_AT - 1;
    assert!(matches!(
        evaluate_reconciliation_result(&request, stale, &[], &[], RECEIVED_AT),
        Err(ReconciliationError::InvalidResult {
            field: "observed_at",
            ..
        })
    ));
}

#[test]
fn empty_position_and_order_vectors_are_valid_full_sets() {
    let request = request();
    let evaluated = evaluate_reconciliation_result(
        &request,
        result_for(&request, Vec::new()),
        &[],
        &[],
        RECEIVED_AT,
    )
    .expect("empty account full set");

    assert_eq!(
        evaluated.evaluation.disposition,
        ReconciliationDisposition::Completed
    );
    assert!(evaluated.result.positions.is_empty());
    assert!(evaluated.result.orders.is_empty());
}

#[test]
fn result_sets_are_normalized_for_deterministic_persistence() {
    let request = request();
    let mut result = result_for(
        &request,
        vec![CommandId::from("unknown-z"), CommandId::from("unknown-a")],
    );
    let mut position_z = position(OBSERVED_AT);
    position_z.position_id = PositionId::from("position-z");
    let mut position_a = position(OBSERVED_AT);
    position_a.position_id = PositionId::from("position-a");
    result.positions = vec![position_z, position_a];
    result.orders = vec![order("order-z", None), order("order-a", None)];
    let mut metadata_z = metadata();
    metadata_z.broker_symbol = "Z.a".to_owned();
    metadata_z.symbol = SymbolCode::from("Z");
    let mut metadata_a = metadata();
    metadata_a.broker_symbol = "A.a".to_owned();
    metadata_a.symbol = SymbolCode::from("A");
    result.symbol_metadata = vec![metadata_z, metadata_a];

    let evaluated = evaluate_reconciliation_result(&request, result, &[], &[], RECEIVED_AT)
        .expect("valid sets should be normalized");

    assert_eq!(
        evaluated
            .result
            .positions
            .iter()
            .map(|value| value.position_id.as_str())
            .collect::<Vec<_>>(),
        vec!["position-a", "position-z"]
    );
    assert_eq!(
        evaluated
            .result
            .orders
            .iter()
            .map(|value| value.broker_order_id.as_str())
            .collect::<Vec<_>>(),
        vec!["order-a", "order-z"]
    );
    assert_eq!(
        evaluated.result.unresolved_command_ids,
        vec![CommandId::from("unknown-a"), CommandId::from("unknown-z")]
    );
}

#[test]
fn full_set_rows_must_share_account_and_observed_at_and_unique_keys() {
    let request = request();
    let mut wrong_time = result_for(&request, Vec::new());
    wrong_time.positions.push(position(OBSERVED_AT - 1));
    assert!(matches!(
        evaluate_reconciliation_result(&request, wrong_time, &[], &[], RECEIVED_AT),
        Err(ReconciliationError::InvalidResult {
            field: "positions[]",
            ..
        })
    ));

    let mut duplicate = result_for(&request, Vec::new());
    duplicate.orders = vec![order("ord-1", Some("cmd-a")), order("ord-1", None)];
    assert!(matches!(
        evaluate_reconciliation_result(&request, duplicate, &[], &[], RECEIVED_AT),
        Err(ReconciliationError::InvalidResult {
            field: "orders[].broker_order_id",
            ..
        })
    ));
}

#[test]
fn snapshot_order_never_promotes_an_uncertain_command() {
    let request = targeted_request("cmd-a");
    let command = command_context("cmd-a", ExecutionCommandStatus::Reconciling);
    let mut result = result_for(&request, Vec::new());
    let mut snapshot = order("ord-1", Some("cmd-a"));
    snapshot.status = OrderSnapshotStatus::Filled;
    snapshot.filled_lots = 1.0;
    snapshot.remaining_lots = 0.0;
    result.orders.push(snapshot);

    let evaluated = evaluate_reconciliation_result(&request, result, &[command], &[], RECEIVED_AT)
        .expect("observational result");

    assert_eq!(
        evaluated.evaluation.disposition,
        ReconciliationDisposition::PendingEvidence
    );
    assert_eq!(
        evaluated.evaluation.command_ids,
        vec![CommandId::from("cmd-a")]
    );
    assert!(evaluated.evaluation.findings.iter().any(|finding| {
        matches!(
            finding,
            ReconciliationFinding::MissingAuthoritativeExecutionEvidence {
                command_id,
                observed_broker_order_ids,
            } if command_id.as_str() == "cmd-a"
                && observed_broker_order_ids == &[BrokerOrderId::from("ord-1")]
        )
    }));
}

#[test]
fn unresolved_is_pending_until_explicit_manual_escalation() {
    let request = targeted_request("cmd-a");
    let command = command_context("cmd-a", ExecutionCommandStatus::Reconciling);
    let evaluated = evaluate_reconciliation_result(
        &request,
        result_for(&request, vec![CommandId::from("cmd-a")]),
        std::slice::from_ref(&command),
        &[],
        RECEIVED_AT,
    )
    .expect("pending result");
    assert_eq!(
        evaluated.evaluation.disposition,
        ReconciliationDisposition::PendingEvidence
    );

    let escalation = escalate_manual_reconciliation(
        &request,
        evaluated.evaluation,
        &[command],
        ManualEscalationEvidence {
            request_id: RequestId::from("recon-1"),
            escalated_at: RECEIVED_AT + 1,
            reason: "execution evidence did not converge".to_owned(),
        },
    )
    .expect("explicit escalation");

    assert_eq!(
        escalation.evaluation.disposition,
        ReconciliationDisposition::ManualRequired
    );
    assert_eq!(escalation.command_transitions.len(), 1);
    assert_eq!(
        escalation.command_transitions[0].outcome.state().status,
        ExecutionCommandStatus::ManualReconciliationRequired
    );
    assert!(matches!(
        escalation.command_transitions[0].outcome,
        CommandTransitionOutcome::Applied(_)
    ));
}

#[test]
fn evaluation_has_a_stable_audit_serialization() {
    let request = targeted_request("cmd-a");
    let evaluated = evaluate_reconciliation_result(
        &request,
        result_for(&request, vec![CommandId::from("cmd-a")]),
        &[command_context(
            "cmd-a",
            ExecutionCommandStatus::Reconciling,
        )],
        &[],
        RECEIVED_AT,
    )
    .expect("pending result");

    let json = serde_json::to_value(&evaluated.evaluation).unwrap();
    assert_eq!(json["disposition"], "PENDING_EVIDENCE");
    assert_eq!(json["findings"][0]["type"], "CLIENT_REPORTED_UNRESOLVED");
}

#[test]
fn manual_escalation_rejects_empty_reason_and_completed_result() {
    let request = request();
    let completed = evaluate_reconciliation_result(
        &request,
        result_for(&request, Vec::new()),
        &[],
        &[],
        RECEIVED_AT,
    )
    .expect("completed");
    let error = escalate_manual_reconciliation(
        &request,
        completed.evaluation,
        &[],
        ManualEscalationEvidence {
            request_id: RequestId::from("recon-1"),
            escalated_at: RECEIVED_AT + 1,
            reason: String::new(),
        },
    )
    .expect_err("invalid escalation");
    assert!(matches!(
        error,
        ReconciliationError::InvalidManualEscalation {
            field: "reason",
            ..
        }
    ));
}

#[test]
fn missing_result_requires_explicit_evidence_and_enters_manual_state() {
    let request = targeted_request("cmd-a");
    let command = command_context("cmd-a", ExecutionCommandStatus::Reconciling);
    let escalation = escalate_missing_reconciliation_result(
        &request,
        &[command],
        ManualEscalationEvidence {
            request_id: RequestId::from("recon-1"),
            escalated_at: RECEIVED_AT,
            reason: "no reconciliation.result arrived".to_owned(),
        },
    )
    .expect("explicit missing-result escalation");

    assert_eq!(escalation.evaluation.observed_at, None);
    assert_eq!(
        escalation.evaluation.disposition,
        ReconciliationDisposition::ManualRequired
    );
    assert!(matches!(
        escalation.evaluation.findings.as_slice(),
        [ReconciliationFinding::ReconciliationResultMissing { escalated_at }]
            if *escalated_at == RECEIVED_AT
    ));
    assert_eq!(
        escalation.command_transitions[0].outcome.state().status,
        ExecutionCommandStatus::ManualReconciliationRequired
    );
}

#[test]
fn missing_result_account_scope_can_escalate_without_any_command() {
    let request = request();
    let escalation = escalate_missing_reconciliation_result(
        &request,
        &[],
        ManualEscalationEvidence {
            request_id: RequestId::from("recon-1"),
            escalated_at: RECEIVED_AT,
            reason: "account refresh timed out".to_owned(),
        },
    )
    .expect("run-level escalation");

    assert!(escalation.evaluation.command_ids.is_empty());
    assert!(escalation.command_transitions.is_empty());
    assert_eq!(
        escalation.evaluation.disposition,
        ReconciliationDisposition::ManualRequired
    );
}

#[test]
fn authoritative_state_does_not_override_client_unresolved_observation() {
    let request = targeted_request("cmd-a");
    let command = command_context("cmd-a", ExecutionCommandStatus::CommandReceived);
    let evaluated = evaluate_reconciliation_result(
        &request,
        result_for(&request, vec![CommandId::from("cmd-a")]),
        &[command],
        &[],
        RECEIVED_AT,
    )
    .expect("authoritative state");

    assert_eq!(
        evaluated.evaluation.disposition,
        ReconciliationDisposition::PendingEvidence
    );
    assert_eq!(
        evaluated.evaluation.command_ids,
        vec![CommandId::from("cmd-a")]
    );
    assert!(evaluated.evaluation.findings.iter().any(|finding| {
        matches!(
            finding,
            ReconciliationFinding::ClientReportedUnresolvedDespiteAuthoritativeState {
                command_id,
                status: ExecutionCommandStatus::CommandReceived,
            } if command_id.as_str() == "cmd-a"
        )
    }));
}

#[test]
fn existing_manual_command_stays_pending_until_this_run_has_explicit_evidence() {
    let request = targeted_request("cmd-a");
    let command = command_context(
        "cmd-a",
        ExecutionCommandStatus::ManualReconciliationRequired,
    );
    let evaluated = evaluate_reconciliation_result(
        &request,
        result_for(&request, Vec::new()),
        &[command],
        &[],
        RECEIVED_AT,
    )
    .expect("existing manual state is an observation, not evidence for this run");

    assert_eq!(
        evaluated.evaluation.disposition,
        ReconciliationDisposition::PendingEvidence
    );
    assert_eq!(
        evaluated.evaluation.command_ids,
        vec![CommandId::from("cmd-a")]
    );
    assert!(evaluated.evaluation.findings.iter().any(|finding| {
        matches!(
            finding,
            ReconciliationFinding::CommandAlreadyRequiresManualReconciliation { command_id }
                if command_id.as_str() == "cmd-a"
        )
    }));
}

#[test]
fn terminal_projection_without_receipt_or_event_does_not_resolve_delivery_uncertainty() {
    let request = targeted_request("cmd-a");
    let command = command_context("cmd-a", ExecutionCommandStatus::Expired);
    let evaluated = evaluate_reconciliation_result(
        &request,
        result_for(&request, Vec::new()),
        std::slice::from_ref(&command),
        &[],
        RECEIVED_AT,
    )
    .expect("terminal projection is not itself delivery evidence");

    assert_eq!(
        evaluated.evaluation.disposition,
        ReconciliationDisposition::PendingEvidence
    );
    let escalation = escalate_manual_reconciliation(
        &request,
        evaluated.evaluation,
        &[command],
        ManualEscalationEvidence {
            request_id: RequestId::from("recon-1"),
            escalated_at: RECEIVED_AT + 1,
            reason: "terminal projection has no authoritative evidence".to_owned(),
        },
    )
    .expect("run-level manual escalation must not regress a terminal command");
    assert_eq!(
        escalation.evaluation.disposition,
        ReconciliationDisposition::ManualRequired
    );
    assert!(escalation.command_transitions.is_empty());
}

#[test]
fn execution_event_wins_over_conflicting_order_snapshot_with_a_finding() {
    let request = targeted_request("cmd-a");
    let command = command_context("cmd-a", ExecutionCommandStatus::Filled);
    let event = filled_event(&command.command);
    let mut result = result_for(&request, Vec::new());
    result.orders.push(order("ord-1", Some("cmd-a")));

    let evaluated =
        evaluate_reconciliation_result(&request, result, &[command], &[event], RECEIVED_AT)
            .expect("conflicting observation");

    assert_eq!(
        evaluated.evaluation.disposition,
        ReconciliationDisposition::Completed
    );
    assert!(evaluated.evaluation.findings.iter().any(|finding| {
        matches!(
            finding,
            ReconciliationFinding::SnapshotConflictsWithExecutionEvent {
                event_status: ExecutionEventStatus::Filled,
                snapshot_status: OrderSnapshotStatus::Placed,
                ..
            }
        )
    }));
}

#[test]
fn authoritative_event_with_lagging_projection_remains_pending() {
    let request = targeted_request("cmd-a");
    let command = command_context("cmd-a", ExecutionCommandStatus::Reconciling);
    let event = filled_event(&command.command);
    let evaluated = evaluate_reconciliation_result(
        &request,
        result_for(&request, Vec::new()),
        &[command],
        &[event],
        RECEIVED_AT,
    )
    .expect("projection lag");

    assert_eq!(
        evaluated.evaluation.disposition,
        ReconciliationDisposition::PendingEvidence
    );
    assert!(evaluated.evaluation.findings.iter().any(|finding| {
        matches!(
            finding,
            ReconciliationFinding::ExecutionProjectionPending {
                event_status: ExecutionEventStatus::Filled,
                ..
            }
        )
    }));
}

#[test]
fn account_wide_unknown_client_command_stays_pending_and_escalates_at_run_level() {
    let request = request();
    let evaluated = evaluate_reconciliation_result(
        &request,
        result_for(&request, vec![CommandId::from("client-only")]),
        &[],
        &[],
        RECEIVED_AT,
    )
    .expect("unknown client journal command");
    assert_eq!(
        evaluated.evaluation.disposition,
        ReconciliationDisposition::PendingEvidence
    );
    assert!(matches!(
        evaluated.evaluation.findings.as_slice(),
        [ReconciliationFinding::UnknownCommandReportedUnresolved { command_id }]
            if command_id.as_str() == "client-only"
    ));

    let escalation = escalate_manual_reconciliation(
        &request,
        evaluated.evaluation,
        &[],
        ManualEscalationEvidence {
            request_id: RequestId::from("recon-1"),
            escalated_at: RECEIVED_AT + 1,
            reason: "client journal differs from Core".to_owned(),
        },
    )
    .expect("run-level escalation");
    assert_eq!(
        escalation.evaluation.disposition,
        ReconciliationDisposition::ManualRequired
    );
    assert!(escalation.command_transitions.is_empty());
}

#[test]
fn account_wide_order_for_unknown_core_command_stays_pending() {
    let request = request();
    let mut result = result_for(&request, Vec::new());
    result
        .orders
        .push(order("client-order", Some("client-only")));

    let evaluated = evaluate_reconciliation_result(&request, result, &[], &[], RECEIVED_AT)
        .expect("unknown broker-side command is a reconciliation finding");

    assert_eq!(
        evaluated.evaluation.disposition,
        ReconciliationDisposition::PendingEvidence
    );
    assert_eq!(
        evaluated.evaluation.command_ids,
        vec![CommandId::from("client-only")]
    );
    assert!(evaluated.evaluation.findings.iter().any(|finding| {
        matches!(
            finding,
            ReconciliationFinding::UnknownCommandObservedInOrderSnapshot {
                command_id,
                broker_order_ids,
            } if command_id.as_str() == "client-only"
                && broker_order_ids == &[BrokerOrderId::from("client-order")]
        )
    }));
}

#[test]
fn route_scoped_reconciliation_ignores_known_other_route_order_scope() {
    let request = request();
    let mut result = result_for(&request, Vec::new());
    let mut other_route = order("other-route-order", Some("other-route-command"));
    other_route.terminal_id = Some(TerminalId::from("terminal-2"));
    other_route.client_id = Some(ClientId::from("client-2"));
    result.orders.push(other_route);

    let evaluated = evaluate_reconciliation_result(&request, result, &[], &[], RECEIVED_AT)
        .expect("full account order set may contain commands from another route");

    assert_eq!(
        evaluated.evaluation.disposition,
        ReconciliationDisposition::Completed
    );
    assert!(evaluated.evaluation.command_ids.is_empty());
    assert!(evaluated.evaluation.findings.is_empty());
}

#[test]
fn known_command_order_route_drift_is_a_finding_and_stays_pending() {
    let request = targeted_request("cmd-a");
    let command = command_context("cmd-a", ExecutionCommandStatus::Reconciling);
    let mut mismatched = order("ord-1", Some("cmd-a"));
    mismatched.terminal_id = Some(TerminalId::from("terminal-2"));
    mismatched.client_id = Some(ClientId::from("client-2"));
    let mut result = result_for(&request, Vec::new());
    result.orders.push(mismatched);

    let evaluated = evaluate_reconciliation_result(&request, result, &[command], &[], RECEIVED_AT)
        .expect("route drift remains an observational finding");

    assert_eq!(
        evaluated.evaluation.disposition,
        ReconciliationDisposition::PendingEvidence
    );
    for expected_field in ["terminal_id", "client_id"] {
        assert!(evaluated.evaluation.findings.iter().any(|finding| {
            matches!(
                finding,
                ReconciliationFinding::OrderIdentityConflict {
                    command_id,
                    broker_order_id,
                    field,
                } if command_id.as_str() == "cmd-a"
                    && broker_order_id.as_str() == "ord-1"
                    && *field == expected_field
            )
        }));
    }
}

#[test]
fn manual_escalation_rejects_forged_attention_scope() {
    let request = targeted_request("cmd-a");
    let command = command_context("cmd-a", ExecutionCommandStatus::Reconciling);
    let mut evaluated = evaluate_reconciliation_result(
        &request,
        result_for(&request, vec![CommandId::from("cmd-a")]),
        std::slice::from_ref(&command),
        &[],
        RECEIVED_AT,
    )
    .expect("pending result");
    evaluated.evaluation.command_ids = vec![CommandId::from("cmd-b")];

    let error = escalate_manual_reconciliation(
        &request,
        evaluated.evaluation,
        &[command],
        ManualEscalationEvidence {
            request_id: RequestId::from("recon-1"),
            escalated_at: RECEIVED_AT + 1,
            reason: "forged scope".to_owned(),
        },
    )
    .expect_err("attention scope must remain inside the targeted request");

    assert!(matches!(
        error,
        ReconciliationError::InvalidManualEscalation {
            field: "evaluation.command_ids",
            ..
        }
    ));
}

#[test]
fn targeted_result_rejects_unresolved_command_outside_scope() {
    let request = targeted_request("cmd-a");
    let error = evaluate_reconciliation_result(
        &request,
        result_for(&request, vec![CommandId::from("cmd-b")]),
        &[command_context(
            "cmd-a",
            ExecutionCommandStatus::Reconciling,
        )],
        &[],
        RECEIVED_AT,
    )
    .expect_err("outside scope");
    assert!(matches!(
        error,
        ReconciliationError::InvalidResult {
            field: "unresolved_command_ids",
            ..
        }
    ));
}

#[test]
fn order_identity_drift_is_a_finding_and_not_execution_evidence() {
    let request = targeted_request("cmd-a");
    let command = command_context("cmd-a", ExecutionCommandStatus::Reconciling);
    let mut mismatched = order("ord-1", Some("cmd-a"));
    mismatched.idempotency_key = Some(IdempotencyKey::from("wrong-key"));
    let mut result = result_for(&request, Vec::new());
    result.orders.push(mismatched);

    let evaluated = evaluate_reconciliation_result(&request, result, &[command], &[], RECEIVED_AT)
        .expect("broker observation retained");
    assert_eq!(
        evaluated.evaluation.disposition,
        ReconciliationDisposition::PendingEvidence
    );
    assert!(evaluated.evaluation.findings.iter().any(|finding| {
        matches!(
            finding,
            ReconciliationFinding::OrderIdentityConflict {
                field: "idempotency_key",
                ..
            }
        )
    }));
}

#[test]
fn malformed_authoritative_event_identity_is_rejected() {
    let request = targeted_request("cmd-a");
    let command = command_context("cmd-a", ExecutionCommandStatus::Filled);
    let mut event = filled_event(&command.command);
    event.account_id = AccountId::from("other-account");
    let error = evaluate_reconciliation_result(
        &request,
        result_for(&request, Vec::new()),
        &[command],
        &[event],
        RECEIVED_AT,
    )
    .expect_err("event identity corruption");
    assert!(matches!(
        error,
        ReconciliationError::InvalidContext {
            field: "execution_events[]",
            ..
        }
    ));
}

#[test]
fn malformed_authoritative_fill_quantities_are_rejected() {
    let request = targeted_request("cmd-a");
    let command = command_context("cmd-a", ExecutionCommandStatus::Filled);

    let mut missing_fill = filled_event(&command.command);
    missing_fill.filled_lots = None;
    let missing_error = evaluate_reconciliation_result(
        &request,
        result_for(&request, Vec::new()),
        std::slice::from_ref(&command),
        &[missing_fill],
        RECEIVED_AT,
    )
    .expect_err("fill without filled_lots is not authoritative");
    assert!(matches!(
        missing_error,
        ReconciliationError::InvalidContext {
            field: "execution_events[]",
            ..
        }
    ));

    let mut invalid_numeric = filled_event(&command.command);
    invalid_numeric.remaining_lots = Some(-1.0);
    let numeric_error = evaluate_reconciliation_result(
        &request,
        result_for(&request, Vec::new()),
        &[command],
        &[invalid_numeric],
        RECEIVED_AT,
    )
    .expect_err("negative event quantities are not authoritative");
    assert!(matches!(
        numeric_error,
        ReconciliationError::InvalidContext {
            field: "execution_events[]",
            ..
        }
    ));
}

fn request() -> sinan_reconciliation::ReconciliationRequestContext {
    build_reconciliation_request(request_input(None)).expect("request")
}

fn targeted_request(command_id: &str) -> sinan_reconciliation::ReconciliationRequestContext {
    build_reconciliation_request(request_input(Some(vec![CommandId::from(command_id)])))
        .expect("targeted request")
}

fn request_input(command_ids: Option<Vec<CommandId>>) -> ReconciliationRequestInput {
    ReconciliationRequestInput {
        request_id: RequestId::from("recon-1"),
        account_id: AccountId::from("account-1"),
        terminal_id: Some(TerminalId::from("terminal-1")),
        client_id: Some(ClientId::from("client-1")),
        reason: ReconciliationReason::DeliveryUnconfirmed,
        command_ids,
        since_server_time: Some(CREATED_AT),
        requested_at: REQUESTED_AT,
    }
}

fn result_for(
    request: &sinan_reconciliation::ReconciliationRequestContext,
    unresolved_command_ids: Vec<CommandId>,
) -> ReconciliationResult {
    ReconciliationResult {
        request_id: request.request.request_id.clone(),
        account_id: request.request.account_id.clone(),
        terminal_id: request.request.terminal_id.clone(),
        client_id: request.request.client_id.clone(),
        observed_at: OBSERVED_AT,
        account: Some(AccountSnapshot {
            account_id: request.request.account_id.clone(),
            balance: 10_000.0,
            equity: 10_050.0,
            margin: 100.0,
            free_margin: 9_950.0,
            currency: "USD".to_owned(),
            observed_at: OBSERVED_AT,
        }),
        positions: Vec::new(),
        orders: Vec::new(),
        symbol_metadata: vec![metadata()],
        unresolved_command_ids,
    }
}

fn command_context(command_id: &str, status: ExecutionCommandStatus) -> ReconciliationCommand {
    let command = command(command_id);
    let terminal = matches!(
        status,
        ExecutionCommandStatus::DeliveryFailed
            | ExecutionCommandStatus::Rejected
            | ExecutionCommandStatus::Filled
            | ExecutionCommandStatus::Failed
            | ExecutionCommandStatus::Expired
            | ExecutionCommandStatus::Cancelled
    );
    let updated_at = match status {
        ExecutionCommandStatus::Created => CREATED_AT,
        ExecutionCommandStatus::Dispatched | ExecutionCommandStatus::DeliveryUnconfirmed => 990,
        ExecutionCommandStatus::Reconciling
        | ExecutionCommandStatus::ManualReconciliationRequired => REQUESTED_AT,
        _ => 1_050,
    };
    ReconciliationCommand {
        state: ExecutionCommandState {
            command_id: command.command_id.clone(),
            account_id: command.account_id.clone(),
            plan_id: command.plan_id.clone(),
            leg_id: command.leg_id.clone(),
            status,
            delivery_attempts: u32::from(status != ExecutionCommandStatus::Created),
            last_delivery_error: (status == ExecutionCommandStatus::DeliveryUnconfirmed)
                .then(|| "command.received timeout".to_owned()),
            created_at: CREATED_AT,
            dispatched_at: (status != ExecutionCommandStatus::Created).then_some(950),
            command_received_at: matches!(
                status,
                ExecutionCommandStatus::CommandReceived
                    | ExecutionCommandStatus::Accepted
                    | ExecutionCommandStatus::Rejected
                    | ExecutionCommandStatus::OrderSent
                    | ExecutionCommandStatus::PartiallyFilled
                    | ExecutionCommandStatus::Filled
                    | ExecutionCommandStatus::Failed
                    | ExecutionCommandStatus::Cancelled
            )
            .then_some(980),
            reconciling_at: matches!(
                status,
                ExecutionCommandStatus::Reconciling
                    | ExecutionCommandStatus::ManualReconciliationRequired
            )
            .then_some(REQUESTED_AT),
            completed_at: terminal.then_some(updated_at),
            updated_at,
        },
        command,
    }
}

fn command(command_id: &str) -> ExecutionCommand {
    ExecutionCommand {
        command_id: CommandId::from(command_id),
        plan_id: Some(PlanId::from("plan-1")),
        leg_id: Some(LegId::from("leg-1")),
        strategy_id: StrategyId::from("strategy-1"),
        account_id: AccountId::from("account-1"),
        terminal_id: Some(TerminalId::from("terminal-1")),
        client_id: Some(ClientId::from("client-1")),
        symbol: SymbolCode::from("EURUSD"),
        broker_symbol: Some("EURUSD.a".to_owned()),
        action: ExecutionAction::Buy,
        order_type: Some(OrderType::Market),
        lots: Some(1.0),
        price: None,
        sl: Some(1.05),
        tp: Some(1.15),
        deviation_points: Some(10),
        magic: 42,
        comment: None,
        position_ticket: None,
        broker_order_id: None,
        filling_policy: None,
        time_policy: None,
        expiration_time: None,
        expires_at: 5_000,
        idempotency_key: IdempotencyKey::from(format!("idem-{command_id}")),
        hmac: "a".repeat(64),
    }
}

fn order(broker_order_id: &str, command_id: Option<&str>) -> OrderSnapshot {
    OrderSnapshot {
        account_id: AccountId::from("account-1"),
        terminal_id: Some(TerminalId::from("terminal-1")),
        client_id: Some(ClientId::from("client-1")),
        symbol: SymbolCode::from("EURUSD"),
        broker_symbol: Some("EURUSD.a".to_owned()),
        broker_order_id: BrokerOrderId::from(broker_order_id),
        position_ticket: None,
        command_id: command_id.map(CommandId::from),
        plan_id: Some(PlanId::from("plan-1")),
        leg_id: Some(LegId::from("leg-1")),
        idempotency_key: command_id.map(|id| IdempotencyKey::from(format!("idem-{id}"))),
        side: PositionSide::Buy,
        order_type: OrderType::Market,
        status: OrderSnapshotStatus::Placed,
        requested_lots: 1.0,
        filled_lots: 0.0,
        remaining_lots: 1.0,
        price: None,
        sl: Some(1.05),
        tp: Some(1.15),
        created_at: Some(1_010),
        updated_at: Some(1_080),
        observed_at: OBSERVED_AT,
    }
}

fn position(observed_at: i64) -> PositionSnapshot {
    PositionSnapshot {
        account_id: AccountId::from("account-1"),
        symbol: SymbolCode::from("EURUSD"),
        position_id: PositionId::from("position-1"),
        side: PositionSide::Buy,
        lots: 1.0,
        open_price: 1.1,
        sl: Some(1.05),
        tp: Some(1.15),
        floating_pnl: 10.0,
        observed_at,
    }
}

fn metadata() -> SymbolMetadataSnapshot {
    SymbolMetadataSnapshot {
        account_id: AccountId::from("account-1"),
        symbol: SymbolCode::from("EURUSD"),
        broker_symbol: "EURUSD.a".to_owned(),
        digits: 5,
        point: 0.00001,
        tick_size: 0.00001,
        tick_value_loss: 1.0,
        contract_size: 100_000.0,
        volume_min: 0.01,
        volume_max: 100.0,
        volume_step: 0.01,
        stops_level_points: 0,
        freeze_level_points: 0,
        margin_initial: Some(1_000.0),
        margin_maintenance: None,
        trade_mode: SymbolTradeMode::Full,
        observed_at: OBSERVED_AT,
    }
}

fn filled_event(command: &ExecutionCommand) -> ExecutionEvent {
    ExecutionEvent {
        execution_id: ExecutionId::from("exec-1"),
        command_id: command.command_id.clone(),
        plan_id: command.plan_id.clone(),
        leg_id: command.leg_id.clone(),
        account_id: command.account_id.clone(),
        terminal_id: command.terminal_id.clone(),
        client_id: command.client_id.clone(),
        symbol: command.symbol.clone(),
        broker_symbol: command.broker_symbol.clone(),
        status: ExecutionEventStatus::Filled,
        broker_order_id: Some(BrokerOrderId::from("ord-1")),
        broker_deal_id: None,
        position_ticket: None,
        idempotency_key: Some(command.idempotency_key.clone()),
        requested_lots: command.lots,
        fill_price: Some(1.1),
        filled_lots: Some(1.0),
        remaining_lots: Some(0.0),
        event_at: 1_050,
        filled_at: Some(1_040),
        broker_filled_at: Some(1_039),
        error_code: None::<ErrorCodeOrString>,
        message: None,
    }
}
