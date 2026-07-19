use sinan_execution::{DeliveryRejectionReason, DeliveryRequest};
use sinan_protocol::{
    ExecutionClientMessageType, ReconciliationRequest, WireMessage, SUPPORTED_SCHEMA_VERSION,
};
use sinan_types::ExecutionCommand;

pub(crate) fn validate_command_request(
    request: &DeliveryRequest<ExecutionCommand>,
    server_now: i64,
) -> Result<(), DeliveryRejectionReason> {
    validate_draft_envelope(
        &request.message,
        ExecutionClientMessageType::ExecutionCommand,
        request.client_id.as_deref(),
    )?;
    let command = &request.message.payload;
    if request.account_id != command.account_id {
        return identity_mismatch("account_id");
    }
    if request.client_id != command.client_id {
        return identity_mismatch("client_id");
    }
    if request.terminal_id != command.terminal_id {
        return identity_mismatch("terminal_id");
    }
    if request.command_id.as_ref() != Some(&command.command_id) {
        return identity_mismatch("command_id");
    }
    if request.expires_at != Some(command.expires_at) {
        return identity_mismatch("expires_at");
    }
    if server_now < 0 || server_now >= command.expires_at {
        return Err(DeliveryRejectionReason::Expired);
    }
    Ok(())
}

pub(crate) fn validate_reconciliation_request(
    request: &DeliveryRequest<ReconciliationRequest>,
) -> Result<(), DeliveryRejectionReason> {
    validate_draft_envelope(
        &request.message,
        ExecutionClientMessageType::ReconciliationRequest,
        request.client_id.as_deref(),
    )?;
    let reconciliation = &request.message.payload;
    if request.account_id != reconciliation.account_id {
        return identity_mismatch("account_id");
    }
    if request.client_id != reconciliation.client_id {
        return identity_mismatch("client_id");
    }
    if request.terminal_id != reconciliation.terminal_id {
        return identity_mismatch("terminal_id");
    }
    if request.command_id.is_some() {
        return identity_mismatch("command_id");
    }
    if request.expires_at.is_some() {
        return identity_mismatch("expires_at");
    }
    Ok(())
}

fn validate_draft_envelope<T>(
    message: &WireMessage<T>,
    expected_type: ExecutionClientMessageType,
    request_client_id: Option<&str>,
) -> Result<(), DeliveryRejectionReason> {
    if message.message_id.as_str().trim().is_empty() {
        return identity_mismatch("message_id");
    }
    if message.message_type != expected_type {
        return identity_mismatch("message_type");
    }
    if message.session_id.is_some() {
        return identity_mismatch("session_id");
    }
    if message.sequence.is_some() {
        return identity_mismatch("sequence");
    }
    if message.sent_at.is_some() {
        return identity_mismatch("sent_at");
    }
    if message.client_id.as_deref() != request_client_id {
        return identity_mismatch("envelope.client_id");
    }
    for (field, value) in [
        ("correlation_id", message.correlation_id.as_deref()),
        ("causation_id", message.causation_id.as_deref()),
    ] {
        if value.is_some_and(|value| value.trim().is_empty()) {
            return identity_mismatch(field);
        }
    }
    if message
        .schema()
        .and_then(|version| version.compatibility_with(SUPPORTED_SCHEMA_VERSION))
        .is_err()
    {
        return identity_mismatch("schema_version");
    }
    Ok(())
}

fn identity_mismatch<T>(field: &'static str) -> Result<T, DeliveryRejectionReason> {
    Err(DeliveryRejectionReason::IdentityMismatch { field })
}

#[cfg(test)]
mod tests {
    use sinan_protocol::{ReconciliationReason, WireMessage};
    use sinan_types::{
        AccountId, ClientId, CommandId, ExecutionAction, IdempotencyKey, MessageId, RequestId,
        StrategyId, SymbolCode, TerminalId,
    };

    use super::*;

    fn command() -> ExecutionCommand {
        ExecutionCommand {
            command_id: CommandId::from("command_1"),
            plan_id: None,
            leg_id: None,
            strategy_id: StrategyId::from("strategy_1"),
            account_id: AccountId::from("account_1"),
            terminal_id: Some(TerminalId::from("terminal_1")),
            client_id: Some(ClientId::from("client_1")),
            symbol: SymbolCode::from("EURUSD"),
            broker_symbol: Some("EURUSD".to_owned()),
            action: ExecutionAction::Cancel,
            order_type: None,
            lots: None,
            price: None,
            sl: None,
            tp: None,
            deviation_points: None,
            magic: 1,
            comment: None,
            position_ticket: None,
            broker_order_id: Some("broker_order_1".into()),
            filling_policy: None,
            time_policy: None,
            expiration_time: None,
            expires_at: 2_000,
            idempotency_key: IdempotencyKey::from("command_key_1"),
            hmac: "a".repeat(64),
        }
    }

    fn command_request() -> DeliveryRequest<ExecutionCommand> {
        let command = command();
        DeliveryRequest {
            account_id: command.account_id.clone(),
            client_id: command.client_id.clone(),
            terminal_id: command.terminal_id.clone(),
            command_id: Some(command.command_id.clone()),
            message: WireMessage {
                message_id: MessageId::from("message_1"),
                message_type: ExecutionClientMessageType::ExecutionCommand,
                schema_version: "ecp.v1.0".to_owned(),
                client_id: command.client_id.clone(),
                session_id: None,
                correlation_id: None,
                causation_id: None,
                sent_at: None,
                sequence: None,
                payload: command,
            },
            expires_at: Some(2_000),
        }
    }

    #[test]
    fn command_draft_requires_gateway_owned_envelope_fields_to_be_empty() {
        let mut request = command_request();
        assert_eq!(validate_command_request(&request, 1_000), Ok(()));

        request.message.session_id = Some("prebound".into());
        assert_eq!(
            validate_command_request(&request, 1_000),
            Err(DeliveryRejectionReason::IdentityMismatch {
                field: "session_id"
            })
        );
    }

    #[test]
    fn command_route_and_expiry_fail_closed() {
        let mut request = command_request();
        request.terminal_id = Some(TerminalId::from("another_terminal"));
        assert_eq!(
            validate_command_request(&request, 1_000),
            Err(DeliveryRejectionReason::IdentityMismatch {
                field: "terminal_id"
            })
        );

        let request = command_request();
        assert_eq!(
            validate_command_request(&request, 2_000),
            Err(DeliveryRejectionReason::Expired)
        );
    }

    #[test]
    fn reconciliation_has_no_command_or_expiry_identity() {
        let payload = ReconciliationRequest {
            request_id: RequestId::from("request_1"),
            account_id: AccountId::from("account_1"),
            terminal_id: None,
            client_id: Some(ClientId::from("client_1")),
            reason: ReconciliationReason::ManualRequest,
            command_ids: None,
            since_server_time: None,
        };
        let mut request = DeliveryRequest {
            account_id: payload.account_id.clone(),
            client_id: payload.client_id.clone(),
            terminal_id: None,
            command_id: None,
            message: WireMessage {
                message_id: MessageId::from("message_1"),
                message_type: ExecutionClientMessageType::ReconciliationRequest,
                schema_version: "ecp.v1.0".to_owned(),
                client_id: payload.client_id.clone(),
                session_id: None,
                correlation_id: None,
                causation_id: None,
                sent_at: None,
                sequence: None,
                payload,
            },
            expires_at: None,
        };
        assert_eq!(validate_reconciliation_request(&request), Ok(()));

        request.command_id = Some(CommandId::from("command_1"));
        assert_eq!(
            validate_reconciliation_request(&request),
            Err(DeliveryRejectionReason::IdentityMismatch {
                field: "command_id"
            })
        );
    }
}
