use std::collections::{HashMap, HashSet, VecDeque};

use sinan_protocol::{sign_execution_command, CommandSigningFormat, SigningError};
use sinan_risk::{risk_request_hash, RiskRequest};
use sinan_types::{
    AdjustedRiskLeg, AdjustedRiskLegAction, ClientId, CommandId, ExecutionAction, ExecutionCommand,
    ExecutionCommandState, ExecutionLeg, ExecutionLegDefinition, ExecutionLegState, ExecutionPlan,
    ExecutionPlanDefinition, ExecutionPlanMode, ExecutionPlanState, ExecutionPlanStatus,
    ExecutionPolicy, FillingPolicy, IdempotencyKey, LegId, OrderType, PlanId, RiskResult,
    SymbolMetadataSnapshot, TerminalId, TimePolicy, TradeIntentAction, TradeIntentLegAction,
};
use thiserror::Error;

use crate::initial_command_state;

#[derive(Clone, Debug, PartialEq)]
pub struct ResolvedLegExecution {
    pub leg_id: LegId,
    pub dependency: Vec<LegId>,
    pub command_id: CommandId,
    pub idempotency_key: IdempotencyKey,
    pub terminal_id: Option<TerminalId>,
    pub client_id: Option<ClientId>,
    pub order_type: OrderType,
    pub price: Option<f64>,
    pub deviation_points: Option<i64>,
    pub magic: i64,
    pub comment: Option<String>,
    pub filling_policy: Option<FillingPolicy>,
    pub time_policy: Option<TimePolicy>,
    pub expiration_time: Option<i64>,
}

pub struct ExecutionBuildRequest<'a> {
    pub plan_id: PlanId,
    pub now_ms: i64,
    pub risk_request: &'a RiskRequest,
    pub risk_result: &'a RiskResult,
    pub policy: &'a ExecutionPolicy,
    pub resolved_legs: &'a [ResolvedLegExecution],
    pub signing_secret: &'a [u8],
}

#[derive(Clone, Debug, PartialEq)]
pub struct ExecutionBundle {
    pub plan: ExecutionPlan,
    pub commands: Vec<ExecutionCommand>,
    pub command_states: Vec<ExecutionCommandState>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ExecutionBuildOutcome {
    NoOp,
    Built(ExecutionBundle),
}

#[derive(Debug, Error)]
pub enum ExecutionBuildError {
    #[error("invalid execution build input at {field}: {reason}")]
    InvalidInput { field: &'static str, reason: String },

    #[error("risk approval is not executable: {0}")]
    RiskNotApproved(String),

    #[error("execution parameters have drifted from the risk approval: {0}")]
    RequiresReRisk(String),

    #[error(transparent)]
    Signing(#[from] SigningError),

    #[error(transparent)]
    InitialState(#[from] crate::CommandTransitionError),
}

impl ExecutionBuildError {
    fn invalid(field: &'static str, reason: impl Into<String>) -> Self {
        Self::InvalidInput {
            field,
            reason: reason.into(),
        }
    }
}

#[derive(Clone, Copy)]
struct IntentLeg<'a> {
    leg_id: &'a LegId,
    symbol: &'a sinan_types::SymbolCode,
    action: ExecutionAction,
    ratio: f64,
    tp: Option<f64>,
}

pub fn build_execution(
    request: ExecutionBuildRequest<'_>,
) -> Result<ExecutionBuildOutcome, ExecutionBuildError> {
    validate_common(&request)?;
    let intent = &request.risk_request.intent;

    if intent.action == TradeIntentAction::Hold {
        if !request.resolved_legs.is_empty() || request.risk_result.adjusted_legs.is_some() {
            return Err(ExecutionBuildError::invalid(
                "resolved_legs",
                "approved HOLD must remain a no-op",
            ));
        }
        return Ok(ExecutionBuildOutcome::NoOp);
    }
    if intent.action == TradeIntentAction::Close {
        return Err(ExecutionBuildError::RiskNotApproved(
            "v1 CLOSE lacks a provable target and close quantity".to_owned(),
        ));
    }

    let intent_legs = intent_legs(request.risk_request)?;
    validate_topology(request.policy.mode, &intent_legs, request.resolved_legs)?;
    let adjusted = index_adjusted_legs(request.risk_result, &intent_legs)?;
    validate_result_candidates(request.risk_request, request.risk_result)?;
    let metadata = index_metadata(request.risk_request, &intent_legs)?;
    let resolved = index_resolved(request.resolved_legs, &intent_legs)?;

    let ttl_boundary = request
        .now_ms
        .checked_add(request.policy.max_command_ttl_ms)
        .ok_or_else(|| ExecutionBuildError::invalid("max_command_ttl_ms", "timestamp overflow"))?;
    let expires_at = intent
        .signal_expires_at
        .min(request.risk_result.valid_until)
        .min(ttl_boundary);
    if expires_at <= request.now_ms {
        return Err(ExecutionBuildError::RequiresReRisk(
            "approval has no remaining execution window".to_owned(),
        ));
    }

    let mut legs = Vec::with_capacity(intent_legs.len());
    let mut commands = Vec::with_capacity(intent_legs.len());
    let mut command_states = Vec::with_capacity(intent_legs.len());
    for intent_leg in intent_legs {
        let approved = adjusted[&intent_leg.leg_id];
        let resolved = resolved[&intent_leg.leg_id];
        let metadata = metadata[&intent_leg.leg_id];
        validate_leg_binding(intent_leg, approved, resolved, metadata)?;

        let leg = ExecutionLeg {
            definition: ExecutionLegDefinition {
                leg_id: intent_leg.leg_id.clone(),
                symbol: intent_leg.symbol.clone(),
                action: intent_leg.action,
                lots: Some(approved.lots),
                sl: Some(approved.approved_sl),
                tp: intent_leg.tp,
                ratio: intent_leg.ratio,
                dependency: resolved.dependency.clone(),
            },
            state: ExecutionLegState {
                status: sinan_types::ExecutionLegStatus::Pending,
            },
        };

        let mut command = ExecutionCommand {
            command_id: resolved.command_id.clone(),
            plan_id: Some(request.plan_id.clone()),
            leg_id: Some(intent_leg.leg_id.clone()),
            strategy_id: intent.strategy_id.clone(),
            account_id: intent.account_id.clone(),
            terminal_id: resolved.terminal_id.clone(),
            client_id: resolved.client_id.clone(),
            symbol: intent_leg.symbol.clone(),
            broker_symbol: Some(metadata.broker_symbol.clone()),
            action: intent_leg.action,
            order_type: Some(resolved.order_type),
            lots: Some(approved.lots),
            price: resolved.price,
            sl: Some(approved.approved_sl),
            tp: intent_leg.tp,
            deviation_points: resolved.deviation_points,
            magic: resolved.magic,
            comment: resolved.comment.clone(),
            position_ticket: None,
            broker_order_id: None,
            filling_policy: resolved.filling_policy,
            time_policy: resolved.time_policy,
            expiration_time: resolved.expiration_time,
            expires_at,
            idempotency_key: resolved.idempotency_key.clone(),
            hmac: String::new(),
        };
        let format = CommandSigningFormat::from_symbol_metadata(metadata)?;
        command.hmac = sign_execution_command(&command, request.signing_secret, format)?;
        let state = initial_command_state(&command, request.now_ms)?;
        legs.push(leg);
        commands.push(command);
        command_states.push(state);
    }

    let plan = ExecutionPlan {
        definition: ExecutionPlanDefinition {
            plan_id: request.plan_id,
            account_id: intent.account_id.clone(),
            strategy_id: intent.strategy_id.clone(),
            mode: request.policy.mode,
            failure_policy: request.policy.failure_policy,
            rollback_policy: request.policy.rollback_policy.clone(),
        },
        legs,
        state: ExecutionPlanState {
            status: ExecutionPlanStatus::Pending,
            filled_legs: Vec::new(),
            failed_legs: Vec::new(),
        },
    };
    plan.validate()
        .map_err(|error| ExecutionBuildError::invalid(error.field(), error.reason()))?;
    Ok(ExecutionBuildOutcome::Built(ExecutionBundle {
        plan,
        commands,
        command_states,
    }))
}

fn validate_common(request: &ExecutionBuildRequest<'_>) -> Result<(), ExecutionBuildError> {
    non_empty("plan_id", request.plan_id.as_str())?;
    if request.now_ms < 0 {
        return Err(ExecutionBuildError::invalid(
            "now_ms",
            "must be a non-negative server timestamp",
        ));
    }
    if request.policy.timeout_ms <= 0 || request.policy.max_command_ttl_ms <= 0 {
        return Err(ExecutionBuildError::invalid(
            "policy",
            "timeout_ms and max_command_ttl_ms must be positive",
        ));
    }
    if request
        .policy
        .rollback_policy
        .as_ref()
        .and_then(|policy| policy.max_retry_attempts)
        == Some(0)
    {
        return Err(ExecutionBuildError::invalid(
            "rollback_policy.max_retry_attempts",
            "must be positive when present",
        ));
    }
    if request.signing_secret.is_empty() {
        return Err(ExecutionBuildError::invalid(
            "signing_secret",
            "must not be empty",
        ));
    }
    request
        .risk_result
        .validate()
        .map_err(|error| ExecutionBuildError::invalid(error.field(), error.reason()))?;
    if !request.risk_result.approved {
        return Err(ExecutionBuildError::RiskNotApproved(
            request.risk_result.reason.to_string(),
        ));
    }
    let risk_request = request.risk_request;
    let result = request.risk_result;
    if result.risk_id != risk_request.risk_id
        || result.request_id != risk_request.request_id
        || result.intent_id != risk_request.intent.intent_id
        || result.account_id != risk_request.intent.account_id
        || result.decision_id != risk_request.intent.decision_id
        || result.evaluated_at != risk_request.evaluated_at
    {
        return Err(ExecutionBuildError::RequiresReRisk(
            "risk result identity does not match its request".to_owned(),
        ));
    }
    if risk_request_hash(risk_request) != result.risk_request_hash {
        return Err(ExecutionBuildError::RequiresReRisk(
            "risk request hash does not match the approval".to_owned(),
        ));
    }
    if request.now_ms < result.evaluated_at || request.now_ms >= result.valid_until {
        return Err(ExecutionBuildError::RequiresReRisk(
            "risk approval is not currently valid".to_owned(),
        ));
    }
    Ok(())
}

fn intent_legs(request: &RiskRequest) -> Result<Vec<IntentLeg<'_>>, ExecutionBuildError> {
    if let Some(legs) = &request.intent.proposed_legs {
        let mut result = Vec::with_capacity(legs.len());
        for leg in legs {
            let action = match leg.action {
                TradeIntentLegAction::Buy => ExecutionAction::Buy,
                TradeIntentLegAction::Sell => ExecutionAction::Sell,
                TradeIntentLegAction::Close => {
                    return Err(ExecutionBuildError::RiskNotApproved(format!(
                        "CLOSE leg {} is not executable in v1",
                        leg.leg_id
                    )))
                }
            };
            result.push(IntentLeg {
                leg_id: &leg.leg_id,
                symbol: &leg.symbol,
                action,
                ratio: leg.ratio,
                tp: leg.proposed_tp,
            });
        }
        return Ok(result);
    }
    let action = match request.intent.action {
        TradeIntentAction::Buy => ExecutionAction::Buy,
        TradeIntentAction::Sell => ExecutionAction::Sell,
        _ => {
            return Err(ExecutionBuildError::RiskNotApproved(
                "only BUY/SELL can produce an actionable plan".to_owned(),
            ))
        }
    };
    let expected = sinan_types::single_leg_id(&request.intent.intent_id);
    let candidate = request
        .sizing_candidates
        .iter()
        .find(|candidate| candidate.leg_id == expected)
        .ok_or_else(|| {
            ExecutionBuildError::RequiresReRisk("single leg candidate missing".into())
        })?;
    Ok(vec![IntentLeg {
        leg_id: &candidate.leg_id,
        symbol: &request.intent.symbol,
        action,
        ratio: 1.0,
        tp: request.intent.proposed_tp,
    }])
}

fn index_adjusted_legs<'a>(
    result: &'a RiskResult,
    intent_legs: &[IntentLeg<'_>],
) -> Result<HashMap<&'a LegId, &'a AdjustedRiskLeg>, ExecutionBuildError> {
    let legs = result.adjusted_legs.as_ref().ok_or_else(|| {
        ExecutionBuildError::RiskNotApproved("approved actionable result has no legs".into())
    })?;
    if legs.len() != intent_legs.len() {
        return Err(ExecutionBuildError::RequiresReRisk(
            "approved and intended leg counts differ".into(),
        ));
    }
    let mut indexed = HashMap::with_capacity(legs.len());
    for leg in legs {
        if indexed.insert(&leg.leg_id, leg).is_some() {
            return Err(ExecutionBuildError::RequiresReRisk(format!(
                "duplicate approved leg {}",
                leg.leg_id
            )));
        }
    }
    Ok(indexed)
}

fn index_metadata<'a>(
    request: &'a RiskRequest,
    intent_legs: &[IntentLeg<'a>],
) -> Result<HashMap<&'a LegId, &'a SymbolMetadataSnapshot>, ExecutionBuildError> {
    let mut by_symbol = HashMap::new();
    for metadata in &request.symbol_metadata {
        if by_symbol.insert(&metadata.symbol, metadata).is_some() {
            return Err(ExecutionBuildError::RequiresReRisk(format!(
                "duplicate metadata for {}",
                metadata.symbol
            )));
        }
    }
    let mut result = HashMap::with_capacity(intent_legs.len());
    for leg in intent_legs {
        let metadata = by_symbol.get(leg.symbol).copied().ok_or_else(|| {
            ExecutionBuildError::RequiresReRisk(format!("metadata missing for {}", leg.symbol))
        })?;
        if metadata.account_id != request.intent.account_id
            || metadata.broker_symbol.trim().is_empty()
        {
            return Err(ExecutionBuildError::RequiresReRisk(format!(
                "metadata identity differs for {}",
                leg.symbol
            )));
        }
        result.insert(leg.leg_id, metadata);
    }
    Ok(result)
}

fn index_resolved<'a>(
    resolved: &'a [ResolvedLegExecution],
    intent_legs: &[IntentLeg<'_>],
) -> Result<HashMap<&'a LegId, &'a ResolvedLegExecution>, ExecutionBuildError> {
    if resolved.len() != intent_legs.len() {
        return Err(ExecutionBuildError::invalid(
            "resolved_legs",
            "must correspond one-to-one with intended legs",
        ));
    }
    let mut command_ids = HashSet::new();
    let mut idempotency_keys = HashSet::new();
    let mut indexed = HashMap::with_capacity(resolved.len());
    for leg in resolved {
        non_empty("resolved_legs[].leg_id", leg.leg_id.as_str())?;
        non_empty("resolved_legs[].command_id", leg.command_id.as_str())?;
        non_empty(
            "resolved_legs[].idempotency_key",
            leg.idempotency_key.as_str(),
        )?;
        optional_non_empty("resolved_legs[].terminal_id", leg.terminal_id.as_deref())?;
        optional_non_empty("resolved_legs[].client_id", leg.client_id.as_deref())?;
        if !command_ids.insert(leg.command_id.as_str())
            || !idempotency_keys.insert(leg.idempotency_key.as_str())
            || indexed.insert(&leg.leg_id, leg).is_some()
        {
            return Err(ExecutionBuildError::invalid(
                "resolved_legs",
                "leg, command, and idempotency identities must be unique",
            ));
        }
    }
    Ok(indexed)
}

fn validate_result_candidates(
    request: &RiskRequest,
    result: &RiskResult,
) -> Result<(), ExecutionBuildError> {
    let approved = result.sizing_candidates.as_ref().ok_or_else(|| {
        ExecutionBuildError::RiskNotApproved("approved actionable result has no candidates".into())
    })?;
    if approved.len() != request.sizing_candidates.len() {
        return Err(ExecutionBuildError::RequiresReRisk(
            "approved candidate set differs from risk request".into(),
        ));
    }
    for candidate in &request.sizing_candidates {
        let Some(bound) = approved
            .iter()
            .find(|bound| bound.leg_id == candidate.leg_id)
        else {
            return Err(ExecutionBuildError::RequiresReRisk(format!(
                "candidate {} is not bound to the approval",
                candidate.leg_id
            )));
        };
        if bound.symbol != candidate.symbol
            || bound.action != candidate.action
            || !same_f64(bound.ratio, candidate.ratio)
            || !same_f64(bound.worst_entry_price, candidate.worst_entry_price)
            || !same_f64(bound.stop_loss_price, candidate.stop_loss_price)
            || !same_f64(
                bound.estimated_cost_per_lot,
                candidate.estimated_cost_per_lot,
            )
        {
            return Err(ExecutionBuildError::RequiresReRisk(format!(
                "candidate {} changed after approval",
                candidate.leg_id
            )));
        }
    }
    Ok(())
}

fn validate_leg_binding(
    intent: IntentLeg<'_>,
    approved: &AdjustedRiskLeg,
    resolved: &ResolvedLegExecution,
    metadata: &SymbolMetadataSnapshot,
) -> Result<(), ExecutionBuildError> {
    let expected_action = match approved.action {
        AdjustedRiskLegAction::Buy => ExecutionAction::Buy,
        AdjustedRiskLegAction::Sell => ExecutionAction::Sell,
    };
    if approved.leg_id != *intent.leg_id
        || approved.symbol != *intent.symbol
        || expected_action != intent.action
        || resolved.leg_id != *intent.leg_id
        || metadata.symbol != *intent.symbol
    {
        return Err(ExecutionBuildError::RequiresReRisk(format!(
            "leg {} identity changed after approval",
            intent.leg_id
        )));
    }
    for (field, value) in [
        ("lots", approved.lots),
        ("approved_sl", approved.approved_sl),
        ("ratio", intent.ratio),
    ] {
        if !value.is_finite() || value <= 0.0 {
            return Err(ExecutionBuildError::invalid(
                field,
                "must be positive and finite",
            ));
        }
    }
    if intent
        .tp
        .is_some_and(|value| !value.is_finite() || value <= 0.0)
    {
        return Err(ExecutionBuildError::invalid(
            "tp",
            "must be positive and finite when present",
        ));
    }
    match resolved.order_type {
        OrderType::Market if resolved.price.is_some() => {
            return Err(ExecutionBuildError::invalid(
                "resolved_legs[].price",
                "MARKET commands must not carry a price",
            ))
        }
        OrderType::Market => {}
        _ => {
            let price = resolved.price.ok_or_else(|| {
                ExecutionBuildError::invalid(
                    "resolved_legs[].price",
                    "non-MARKET commands require a price",
                )
            })?;
            if !price.is_finite() || price <= 0.0 {
                return Err(ExecutionBuildError::invalid(
                    "resolved_legs[].price",
                    "must be positive and finite",
                ));
            }
            let within_bound = match approved.action {
                AdjustedRiskLegAction::Buy => price <= approved.sizing_entry_price,
                AdjustedRiskLegAction::Sell => price >= approved.sizing_entry_price,
            };
            if !within_bound {
                return Err(ExecutionBuildError::RequiresReRisk(format!(
                    "entry price for {} exceeds the approved worst-entry bound",
                    intent.leg_id
                )));
            }
        }
    }
    if resolved
        .expiration_time
        .is_some_and(|expiration| expiration < 0)
        || resolved.deviation_points.is_some_and(|value| value < 0)
    {
        return Err(ExecutionBuildError::invalid(
            "resolved_legs",
            "expiration_time and deviation_points must be non-negative",
        ));
    }
    Ok(())
}

fn validate_topology(
    mode: ExecutionPlanMode,
    intent_legs: &[IntentLeg<'_>],
    resolved: &[ResolvedLegExecution],
) -> Result<(), ExecutionBuildError> {
    let ids: HashSet<&LegId> = intent_legs.iter().map(|leg| leg.leg_id).collect();
    if ids.len() != intent_legs.len() {
        return Err(ExecutionBuildError::invalid(
            "legs[].leg_id",
            "must be globally unique within a plan",
        ));
    }
    if mode == ExecutionPlanMode::Simultaneous
        && resolved.iter().any(|leg| !leg.dependency.is_empty())
    {
        return Err(ExecutionBuildError::invalid(
            "resolved_legs[].dependency",
            "simultaneous plans cannot contain dependencies",
        ));
    }
    let mut indegree: HashMap<&LegId, usize> = ids.iter().map(|id| (*id, 0)).collect();
    let mut outgoing: HashMap<&LegId, Vec<&LegId>> = HashMap::new();
    for leg in resolved {
        let mut unique = HashSet::new();
        for dependency in &leg.dependency {
            if dependency == &leg.leg_id || !ids.contains(dependency) || !unique.insert(dependency)
            {
                return Err(ExecutionBuildError::invalid(
                    "resolved_legs[].dependency",
                    "dependencies must be unique, known, and cannot reference the same leg",
                ));
            }
            *indegree.entry(&leg.leg_id).or_default() += 1;
            outgoing.entry(dependency).or_default().push(&leg.leg_id);
        }
    }
    let mut queue: VecDeque<&LegId> = indegree
        .iter()
        .filter_map(|(id, count)| (*count == 0).then_some(*id))
        .collect();
    let mut visited = 0;
    while let Some(id) = queue.pop_front() {
        visited += 1;
        for target in outgoing.get(id).into_iter().flatten() {
            let count = indegree.get_mut(target).expect("target was validated");
            *count -= 1;
            if *count == 0 {
                queue.push_back(target);
            }
        }
    }
    if visited != ids.len() {
        return Err(ExecutionBuildError::invalid(
            "resolved_legs[].dependency",
            "dependency graph must be acyclic",
        ));
    }
    Ok(())
}

fn non_empty(field: &'static str, value: &str) -> Result<(), ExecutionBuildError> {
    if value.trim().is_empty() {
        Err(ExecutionBuildError::invalid(field, "must not be empty"))
    } else {
        Ok(())
    }
}

fn optional_non_empty(field: &'static str, value: Option<&str>) -> Result<(), ExecutionBuildError> {
    if value.is_some_and(|value| value.trim().is_empty()) {
        Err(ExecutionBuildError::invalid(
            field,
            "must not be empty when present",
        ))
    } else {
        Ok(())
    }
}

fn same_f64(left: f64, right: f64) -> bool {
    left.is_finite() && right.is_finite() && left.to_bits() == right.to_bits()
}
