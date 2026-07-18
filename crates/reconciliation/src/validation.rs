use std::collections::{BTreeMap, BTreeSet, HashSet};

use sinan_execution::{validate_command_state, validate_execution_event};
use sinan_protocol::ReconciliationResult;
use sinan_types::{ExecutionAction, ExecutionEvent, OrderSnapshot, SymbolMetadataSnapshot};

use crate::{ReconciliationCommand, ReconciliationError, ReconciliationRequestContext};

pub(crate) fn validate_request_context(
    context: &ReconciliationRequestContext,
) -> Result<(), ReconciliationError> {
    require_request_text("request_id", context.request.request_id.as_str())?;
    require_request_text("account_id", context.request.account_id.as_str())?;
    if let Some(terminal_id) = &context.request.terminal_id {
        require_request_text("terminal_id", terminal_id.as_str())?;
    }
    if let Some(client_id) = &context.request.client_id {
        require_request_text("client_id", client_id.as_str())?;
    }
    if context.requested_at < 0 {
        return Err(ReconciliationError::request(
            "requested_at",
            "must be a non-negative server-time timestamp",
        ));
    }
    if let Some(since) = context.request.since_server_time {
        if since < 0 || since > context.requested_at {
            return Err(ReconciliationError::request(
                "since_server_time",
                "must be non-negative and not later than requested_at",
            ));
        }
    }
    if let Some(command_ids) = &context.request.command_ids {
        if command_ids.is_empty() {
            return Err(ReconciliationError::request(
                "command_ids",
                "Some scope must not be empty; use None for account-wide scope",
            ));
        }
        for command_id in command_ids {
            require_request_text("command_ids[]", command_id.as_str())?;
        }
        if command_ids.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(ReconciliationError::request(
                "command_ids",
                "must be unique and sorted in ascending command_id order",
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_command_scope<'a>(
    context: &ReconciliationRequestContext,
    commands: &'a [ReconciliationCommand],
    upper_time_bound: i64,
) -> Result<BTreeMap<sinan_types::CommandId, &'a ReconciliationCommand>, ReconciliationError> {
    validate_request_context(context)?;
    if upper_time_bound < context.requested_at {
        return Err(ReconciliationError::context(
            "upper_time_bound",
            "must not predate requested_at",
        ));
    }

    let mut indexed = BTreeMap::new();
    for item in commands {
        validate_command_state(&item.command, &item.state).map_err(|error| {
            ReconciliationError::context(
                "commands[].state",
                format!("command {} is invalid: {error}", item.command.command_id),
            )
        })?;
        if item.command.account_id != context.request.account_id {
            return Err(ReconciliationError::context(
                "commands[].account_id",
                format!(
                    "command {} is outside request account {}",
                    item.command.command_id, context.request.account_id
                ),
            ));
        }
        validate_route_constraint(
            "commands[].terminal_id",
            context
                .request
                .terminal_id
                .as_ref()
                .map(|value| value.as_str()),
            item.command
                .terminal_id
                .as_ref()
                .map(|value| value.as_str()),
        )?;
        validate_route_constraint(
            "commands[].client_id",
            context
                .request
                .client_id
                .as_ref()
                .map(|value| value.as_str()),
            item.command.client_id.as_ref().map(|value| value.as_str()),
        )?;
        if item.state.updated_at > upper_time_bound {
            return Err(ReconciliationError::context(
                "commands[].state.updated_at",
                format!(
                    "command {} state is newer than the evaluation boundary",
                    item.command.command_id
                ),
            ));
        }
        if indexed
            .insert(item.command.command_id.clone(), item)
            .is_some()
        {
            return Err(ReconciliationError::context(
                "commands[].command_id",
                format!("duplicate command {}", item.command.command_id),
            ));
        }
    }

    if let Some(scoped_ids) = &context.request.command_ids {
        let actual: Vec<_> = indexed.keys().cloned().collect();
        if &actual != scoped_ids {
            return Err(ReconciliationError::context(
                "commands",
                "must contain exactly the targeted request command_ids",
            ));
        }
    }
    Ok(indexed)
}

pub(crate) fn validate_result(
    context: &ReconciliationRequestContext,
    result: &ReconciliationResult,
    received_at: i64,
) -> Result<(), ReconciliationError> {
    validate_request_context(context)?;
    if result.request_id != context.request.request_id {
        return Err(ReconciliationError::result(
            "request_id",
            "does not match the reconciliation request",
        ));
    }
    if result.account_id != context.request.account_id {
        return Err(ReconciliationError::result(
            "account_id",
            "does not match the reconciliation request",
        ));
    }
    if result.terminal_id != context.request.terminal_id {
        return Err(ReconciliationError::result(
            "terminal_id",
            "must exactly match the reconciliation request route",
        ));
    }
    if result.client_id != context.request.client_id {
        return Err(ReconciliationError::result(
            "client_id",
            "must exactly match the reconciliation request route",
        ));
    }
    if result.observed_at < context.requested_at || result.observed_at > received_at {
        return Err(ReconciliationError::result(
            "observed_at",
            "must be between requested_at and server received_at",
        ));
    }

    if let Some(account) = &result.account {
        if account.account_id != result.account_id || account.observed_at != result.observed_at {
            return Err(ReconciliationError::result(
                "account",
                "account_id and observed_at must match the result full-set identity",
            ));
        }
        for (field, value) in [
            ("account.balance", account.balance),
            ("account.equity", account.equity),
            ("account.margin", account.margin),
            ("account.free_margin", account.free_margin),
        ] {
            require_result_finite(field, value)?;
        }
        require_result_text("account.currency", &account.currency)?;
    }

    validate_positions(result)?;
    validate_orders(result)?;
    validate_symbol_metadata(result)?;

    let mut unresolved = BTreeSet::new();
    for command_id in &result.unresolved_command_ids {
        require_result_text("unresolved_command_ids[]", command_id.as_str())?;
        if !unresolved.insert(command_id.clone()) {
            return Err(ReconciliationError::result(
                "unresolved_command_ids",
                format!("contains duplicate command {command_id}"),
            ));
        }
    }
    if let Some(scoped_ids) = &context.request.command_ids {
        let scope: BTreeSet<_> = scoped_ids.iter().collect();
        if result
            .unresolved_command_ids
            .iter()
            .any(|command_id| !scope.contains(command_id))
        {
            return Err(ReconciliationError::result(
                "unresolved_command_ids",
                "contains a command outside the targeted request scope",
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_execution_events<'a>(
    commands: &BTreeMap<sinan_types::CommandId, &'a ReconciliationCommand>,
    events: &'a [ExecutionEvent],
    received_at: i64,
) -> Result<BTreeMap<sinan_types::CommandId, Vec<&'a ExecutionEvent>>, ReconciliationError> {
    let mut execution_ids = HashSet::with_capacity(events.len());
    let mut grouped: BTreeMap<_, Vec<_>> = BTreeMap::new();
    for event in events {
        if event.execution_id.as_str().trim().is_empty()
            || !execution_ids.insert(event.execution_id.clone())
        {
            return Err(ReconciliationError::context(
                "execution_events[].execution_id",
                "must be non-empty and unique",
            ));
        }
        let item = commands.get(&event.command_id).ok_or_else(|| {
            ReconciliationError::context(
                "execution_events[].command_id",
                format!("event references unknown command {}", event.command_id),
            )
        })?;
        let command = &item.command;
        validate_execution_event(command, event).map_err(|error| {
            ReconciliationError::context(
                "execution_events[]",
                format!(
                    "event is invalid for command {}: {error}",
                    command.command_id
                ),
            )
        })?;
        if event.event_at < item.state.created_at || event.event_at > received_at {
            return Err(ReconciliationError::context(
                "execution_events[].event_at",
                format!("event time is invalid for command {}", command.command_id),
            ));
        }
        if event
            .filled_at
            .is_some_and(|filled_at| filled_at < item.state.created_at)
        {
            return Err(ReconciliationError::context(
                "execution_events[].filled_at",
                format!("fill timing is invalid for command {}", command.command_id),
            ));
        }
        grouped
            .entry(event.command_id.clone())
            .or_default()
            .push(event);
    }
    for events in grouped.values_mut() {
        events.sort_by(|left, right| {
            left.event_at
                .cmp(&right.event_at)
                .then_with(|| left.execution_id.cmp(&right.execution_id))
        });
    }
    Ok(grouped)
}

fn validate_positions(result: &ReconciliationResult) -> Result<(), ReconciliationError> {
    let mut keys = HashSet::with_capacity(result.positions.len());
    for position in &result.positions {
        if position.account_id != result.account_id || position.observed_at != result.observed_at {
            return Err(ReconciliationError::result(
                "positions[]",
                "account_id and observed_at must match the result full-set identity",
            ));
        }
        require_result_text("positions[].position_id", position.position_id.as_str())?;
        require_result_text("positions[].symbol", position.symbol.as_str())?;
        if !keys.insert(position.position_id.clone()) {
            return Err(ReconciliationError::result(
                "positions[].position_id",
                format!("duplicate position {}", position.position_id),
            ));
        }
        require_result_positive("positions[].lots", position.lots)?;
        require_result_positive("positions[].open_price", position.open_price)?;
        require_result_finite("positions[].floating_pnl", position.floating_pnl)?;
        validate_optional_positive("positions[].sl", position.sl)?;
        validate_optional_positive("positions[].tp", position.tp)?;
    }
    Ok(())
}

fn validate_orders(result: &ReconciliationResult) -> Result<(), ReconciliationError> {
    let mut keys = HashSet::with_capacity(result.orders.len());
    for order in &result.orders {
        if order.account_id != result.account_id || order.observed_at != result.observed_at {
            return Err(ReconciliationError::result(
                "orders[]",
                "account_id and observed_at must match the result full-set identity",
            ));
        }
        require_result_text("orders[].broker_order_id", order.broker_order_id.as_str())?;
        require_result_text("orders[].symbol", order.symbol.as_str())?;
        if let Some(value) = &order.broker_symbol {
            require_result_text("orders[].broker_symbol", value)?;
        }
        for (field, value) in [
            (
                "orders[].command_id",
                order.command_id.as_ref().map(|value| value.as_str()),
            ),
            (
                "orders[].plan_id",
                order.plan_id.as_ref().map(|value| value.as_str()),
            ),
            (
                "orders[].leg_id",
                order.leg_id.as_ref().map(|value| value.as_str()),
            ),
            (
                "orders[].idempotency_key",
                order.idempotency_key.as_ref().map(|value| value.as_str()),
            ),
        ] {
            if let Some(value) = value {
                require_result_text(field, value)?;
            }
        }
        if !keys.insert(order.broker_order_id.clone()) {
            return Err(ReconciliationError::result(
                "orders[].broker_order_id",
                format!("duplicate broker order {}", order.broker_order_id),
            ));
        }
        require_result_positive("orders[].requested_lots", order.requested_lots)?;
        require_result_non_negative("orders[].filled_lots", order.filled_lots)?;
        require_result_non_negative("orders[].remaining_lots", order.remaining_lots)?;
        if order.filled_lots > order.requested_lots || order.remaining_lots > order.requested_lots {
            return Err(ReconciliationError::result(
                "orders[].lots",
                "filled_lots and remaining_lots must not exceed requested_lots",
            ));
        }
        validate_optional_positive("orders[].price", order.price)?;
        validate_optional_positive("orders[].sl", order.sl)?;
        validate_optional_positive("orders[].tp", order.tp)?;
        validate_order_times(order, result.observed_at)?;
    }
    Ok(())
}

fn validate_order_times(
    order: &OrderSnapshot,
    observed_at: i64,
) -> Result<(), ReconciliationError> {
    for (field, value) in [
        ("orders[].created_at", order.created_at),
        ("orders[].updated_at", order.updated_at),
    ] {
        if value.is_some_and(|value| value < 0 || value > observed_at) {
            return Err(ReconciliationError::result(
                field,
                "must be a non-negative timestamp not later than observed_at",
            ));
        }
    }
    if matches!((order.created_at, order.updated_at), (Some(created), Some(updated)) if updated < created)
    {
        return Err(ReconciliationError::result(
            "orders[].updated_at",
            "must not predate created_at",
        ));
    }
    Ok(())
}

fn validate_symbol_metadata(result: &ReconciliationResult) -> Result<(), ReconciliationError> {
    let mut broker_symbols = HashSet::with_capacity(result.symbol_metadata.len());
    for metadata in &result.symbol_metadata {
        if metadata.account_id != result.account_id || metadata.observed_at != result.observed_at {
            return Err(ReconciliationError::result(
                "symbol_metadata[]",
                "account_id and observed_at must match the result full-set identity",
            ));
        }
        require_result_text("symbol_metadata[].symbol", metadata.symbol.as_str())?;
        require_result_text("symbol_metadata[].broker_symbol", &metadata.broker_symbol)?;
        if !broker_symbols.insert(metadata.broker_symbol.clone()) {
            return Err(ReconciliationError::result(
                "symbol_metadata[].broker_symbol",
                format!("duplicate broker symbol {}", metadata.broker_symbol),
            ));
        }
        validate_metadata_numbers(metadata)?;
    }
    Ok(())
}

fn validate_metadata_numbers(metadata: &SymbolMetadataSnapshot) -> Result<(), ReconciliationError> {
    for (field, value) in [
        ("symbol_metadata[].point", metadata.point),
        ("symbol_metadata[].tick_size", metadata.tick_size),
        (
            "symbol_metadata[].tick_value_loss",
            metadata.tick_value_loss,
        ),
        ("symbol_metadata[].contract_size", metadata.contract_size),
        ("symbol_metadata[].volume_min", metadata.volume_min),
        ("symbol_metadata[].volume_max", metadata.volume_max),
        ("symbol_metadata[].volume_step", metadata.volume_step),
    ] {
        require_result_positive(field, value)?;
    }
    if metadata.volume_min > metadata.volume_max {
        return Err(ReconciliationError::result(
            "symbol_metadata[].volume_min",
            "must not exceed volume_max",
        ));
    }
    validate_optional_positive("symbol_metadata[].margin_initial", metadata.margin_initial)?;
    validate_optional_positive(
        "symbol_metadata[].margin_maintenance",
        metadata.margin_maintenance,
    )?;
    Ok(())
}

pub(crate) fn order_identity_conflicts(
    order: &OrderSnapshot,
    command: &sinan_types::ExecutionCommand,
) -> Vec<&'static str> {
    let mut fields = Vec::new();
    if order.symbol != command.symbol {
        fields.push("symbol");
    }
    if order
        .broker_symbol
        .as_ref()
        .is_some_and(|value| Some(value) != command.broker_symbol.as_ref())
    {
        fields.push("broker_symbol");
    }
    if order
        .plan_id
        .as_ref()
        .is_some_and(|value| Some(value) != command.plan_id.as_ref())
    {
        fields.push("plan_id");
    }
    if order
        .leg_id
        .as_ref()
        .is_some_and(|value| Some(value) != command.leg_id.as_ref())
    {
        fields.push("leg_id");
    }
    if order
        .idempotency_key
        .as_ref()
        .is_some_and(|value| value != &command.idempotency_key)
    {
        fields.push("idempotency_key");
    }
    if order
        .terminal_id
        .as_ref()
        .is_some_and(|value| Some(value) != command.terminal_id.as_ref())
    {
        fields.push("terminal_id");
    }
    if order
        .client_id
        .as_ref()
        .is_some_and(|value| Some(value) != command.client_id.as_ref())
    {
        fields.push("client_id");
    }
    if command
        .lots
        .is_some_and(|lots| lots.to_bits() != order.requested_lots.to_bits())
    {
        fields.push("requested_lots");
    }
    let side_conflicts = matches!(
        (command.action, order.side),
        (ExecutionAction::Buy, sinan_types::PositionSide::Sell)
            | (ExecutionAction::Sell, sinan_types::PositionSide::Buy)
    );
    if side_conflicts {
        fields.push("side");
    }
    fields
}

fn validate_route_constraint(
    field: &'static str,
    requested: Option<&str>,
    actual: Option<&str>,
) -> Result<(), ReconciliationError> {
    if requested.is_some() && requested != actual {
        Err(ReconciliationError::context(
            field,
            "does not match the constrained request route",
        ))
    } else {
        Ok(())
    }
}

fn require_request_text(field: &'static str, value: &str) -> Result<(), ReconciliationError> {
    if value.trim().is_empty() {
        Err(ReconciliationError::request(field, "must not be empty"))
    } else {
        Ok(())
    }
}

fn require_result_text(field: &'static str, value: &str) -> Result<(), ReconciliationError> {
    if value.trim().is_empty() {
        Err(ReconciliationError::result(field, "must not be empty"))
    } else {
        Ok(())
    }
}

fn require_result_finite(field: &'static str, value: f64) -> Result<(), ReconciliationError> {
    if value.is_finite() {
        Ok(())
    } else {
        Err(ReconciliationError::result(field, "must be finite"))
    }
}

fn require_result_non_negative(field: &'static str, value: f64) -> Result<(), ReconciliationError> {
    if value.is_finite() && value >= 0.0 {
        Ok(())
    } else {
        Err(ReconciliationError::result(
            field,
            "must be finite and non-negative",
        ))
    }
}

fn require_result_positive(field: &'static str, value: f64) -> Result<(), ReconciliationError> {
    if value.is_finite() && value > 0.0 {
        Ok(())
    } else {
        Err(ReconciliationError::result(
            field,
            "must be finite and positive",
        ))
    }
}

fn validate_optional_positive(
    field: &'static str,
    value: Option<f64>,
) -> Result<(), ReconciliationError> {
    if let Some(value) = value {
        require_result_positive(field, value)?;
    }
    Ok(())
}
