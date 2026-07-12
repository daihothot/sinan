use sinan_protocol::{
    build_execution_command_signing_string, decode_wire_message, sign_execution_command,
    verify_execution_command_hmac, CommandReceived, CommandSigningFormat,
    ExecutionClientMessageType, HelloPayload, ProtocolReason, SigningError, WireMessage,
    SUPPORTED_SCHEMA_VERSION,
};
use sinan_types::{
    AccountId, ExecutionAction, ExecutionCommand, OrderType, SymbolCode, SymbolMetadataSnapshot,
    SymbolTradeMode,
};

const SESSION_HELLO: &str =
    include_str!("../../../tests/golden/execution_client_protocol/session_hello.json");
const EXECUTION_COMMAND: &str = include_str!(
    "../../../tests/golden/execution_client_protocol/execution_command_buy_market.json"
);
const COMMAND_RECEIVED: &str =
    include_str!("../../../tests/golden/execution_client_protocol/command_received.json");

const GOLDEN_SIGNING_STRING: &str = "command_id=cmd_20260526_000001&plan_id=plan_20260526_000001&leg_id=leg_1&strategy_id=trend_xau_h4_v1&account_id=acct_mt5_001&terminal_id=mt5_terminal_001&client_id=mt5_client_001&symbol=XAUUSD&broker_symbol=XAUUSD&action=BUY&order_type=MARKET&lots=0.10&price=&sl=2320.50&tp=2365.50&deviation_points=20&magic=26052601&comment=trend_xau_h4&position_ticket=&broker_order_id=&filling_policy=IOC&time_policy=GTC&expiration_time=&expires_at=1779800123000&idempotency_key=idem_cmd_20260526_000001";
const GOLDEN_HMAC: &str = "044916a7aac911c86b107a0fb7ddb21529f2e8dcb755d3d0183d8fd3589f1d2e";

fn golden_command() -> ExecutionCommand {
    decode_wire_message::<ExecutionCommand>(EXECUTION_COMMAND.as_bytes(), SUPPORTED_SCHEMA_VERSION)
        .unwrap()
        .payload
}

fn symbol_metadata(volume_step: f64) -> SymbolMetadataSnapshot {
    SymbolMetadataSnapshot {
        account_id: AccountId::from("acct_mt5_001"),
        symbol: SymbolCode::from("XAUUSD"),
        broker_symbol: "XAUUSD".to_owned(),
        digits: 2,
        point: 0.01,
        tick_size: 0.01,
        tick_value_loss: 1.0,
        contract_size: 100.0,
        volume_min: volume_step,
        volume_max: 100.0,
        volume_step,
        stops_level_points: 0,
        freeze_level_points: 0,
        margin_initial: None,
        margin_maintenance: None,
        trade_mode: SymbolTradeMode::Full,
        observed_at: 1_779_800_000_123,
    }
}

#[test]
fn parses_all_golden_json_samples() {
    let hello =
        decode_wire_message::<HelloPayload>(SESSION_HELLO.as_bytes(), SUPPORTED_SCHEMA_VERSION)
            .unwrap();
    assert_eq!(hello.message_type, ExecutionClientMessageType::SessionHello);
    assert_eq!(hello.payload.client_id.as_str(), "mt5_client_001");
    assert_eq!(
        hello.payload.resume.as_ref().unwrap().last_gateway_sequence,
        Some(41)
    );

    let command = decode_wire_message::<ExecutionCommand>(
        EXECUTION_COMMAND.as_bytes(),
        SUPPORTED_SCHEMA_VERSION,
    )
    .unwrap();
    assert_eq!(
        command.message_type,
        ExecutionClientMessageType::ExecutionCommand
    );

    let received = decode_wire_message::<CommandReceived>(
        COMMAND_RECEIVED.as_bytes(),
        SUPPORTED_SCHEMA_VERSION,
    )
    .unwrap();
    assert_eq!(
        received.message_type,
        ExecutionClientMessageType::CommandReceived
    );
    assert_eq!(received.payload.reason, Some(ProtocolReason::Ok));
}

#[test]
fn golden_hmac_and_signing_string_match_exactly() {
    let message: WireMessage<ExecutionCommand> = serde_json::from_str(EXECUTION_COMMAND).unwrap();
    let format = CommandSigningFormat::new(2, 2);

    assert_eq!(
        build_execution_command_signing_string(&message.payload, format).unwrap(),
        GOLDEN_SIGNING_STRING
    );
    assert_eq!(
        sign_execution_command(&message.payload, b"test_command_secret_v1", format).unwrap(),
        GOLDEN_HMAC
    );
    verify_execution_command_hmac(&message.payload, b"test_command_secret_v1", format).unwrap();
}

#[test]
fn absent_optional_fields_are_present_as_empty_signing_values() {
    let message: WireMessage<ExecutionCommand> = serde_json::from_str(EXECUTION_COMMAND).unwrap();
    let signing =
        build_execution_command_signing_string(&message.payload, CommandSigningFormat::new(2, 2))
            .unwrap();

    for empty_field in [
        "price=",
        "position_ticket=",
        "broker_order_id=",
        "expiration_time=",
    ] {
        assert!(signing.split('&').any(|part| part == empty_field));
    }
}

#[test]
fn hmac_field_itself_does_not_participate_in_signing() {
    let mut message: WireMessage<ExecutionCommand> =
        serde_json::from_str(EXECUTION_COMMAND).unwrap();
    message.payload.hmac = "0".repeat(64);

    assert_eq!(
        sign_execution_command(
            &message.payload,
            b"test_command_secret_v1",
            CommandSigningFormat::new(2, 2),
        )
        .unwrap(),
        GOLDEN_HMAC
    );
}

#[test]
fn wrong_secret_fails_hmac_verification() {
    let message: WireMessage<ExecutionCommand> = serde_json::from_str(EXECUTION_COMMAND).unwrap();
    assert!(verify_execution_command_hmac(
        &message.payload,
        b"wrong_secret",
        CommandSigningFormat::new(2, 2),
    )
    .is_err());
}

#[test]
fn non_finite_signing_numbers_are_rejected_before_json_conversion() {
    for field in ["lots", "price", "sl", "tp"] {
        let mut command = golden_command();
        match field {
            "lots" => command.lots = Some(f64::NAN),
            "price" => command.price = Some(f64::INFINITY),
            "sl" => command.sl = Some(f64::NEG_INFINITY),
            "tp" => command.tp = Some(f64::NAN),
            _ => unreachable!(),
        }

        assert!(matches!(
            build_execution_command_signing_string(
                &command,
                CommandSigningFormat::new(2, 2)
            ),
            Err(SigningError::NonFiniteNumber(actual)) if actual == field
        ));
    }

    let mut command = golden_command();
    command.lots = Some(f64::NAN);
    assert!(matches!(
        sign_execution_command(
            &command,
            b"test_command_secret_v1",
            CommandSigningFormat::new(2, 2)
        ),
        Err(SigningError::NonFiniteNumber("lots"))
    ));
    assert!(matches!(
        verify_execution_command_hmac(
            &command,
            b"test_command_secret_v1",
            CommandSigningFormat::new(2, 2)
        ),
        Err(SigningError::NonFiniteNumber("lots"))
    ));
}

#[test]
fn metadata_signing_format_rejects_unaligned_lots_without_rounding() {
    let quarter_step = CommandSigningFormat::from_symbol_metadata(&symbol_metadata(0.25)).unwrap();
    assert_eq!(quarter_step.volume_step(), Some(0.25));

    let mut command = golden_command();
    command.lots = Some(0.50);
    let signing = build_execution_command_signing_string(&command, quarter_step).unwrap();
    assert!(signing.split('&').any(|field| field == "lots=0.50"));

    command.lots = Some(0.30);
    assert!(matches!(
        build_execution_command_signing_string(&command, quarter_step),
        Err(SigningError::LotsNotAlignedToVolumeStep { .. })
    ));

    let cent_step = CommandSigningFormat::from_symbol_metadata(&symbol_metadata(0.01)).unwrap();
    command.lots = Some(0.01);
    assert!(build_execution_command_signing_string(&command, cent_step).is_ok());
    command.lots = Some(0.009);
    assert!(matches!(
        build_execution_command_signing_string(&command, cent_step),
        Err(SigningError::DecimalPrecisionExceeded {
            field: "lots",
            digits: 2
        })
    ));
}

#[test]
fn signing_precision_and_scaled_integer_overflow_fail_closed() {
    let command = golden_command();
    assert!(matches!(
        build_execution_command_signing_string(&command, CommandSigningFormat::new(2, 19)),
        Err(SigningError::PrecisionTooLarge(19))
    ));

    let mut invalid_metadata = symbol_metadata(0.01);
    invalid_metadata.digits = 19;
    assert!(matches!(
        CommandSigningFormat::from_symbol_metadata(&invalid_metadata),
        Err(SigningError::PrecisionTooLarge(19))
    ));

    invalid_metadata.digits = 2;
    invalid_metadata.volume_step = 10_000_000_000_000_000.0;
    assert!(matches!(
        CommandSigningFormat::from_symbol_metadata(&invalid_metadata),
        Err(SigningError::ScaledIntegerOverflow("volume_step"))
    ));

    let format = CommandSigningFormat::from_symbol_metadata(&symbol_metadata(0.01)).unwrap();
    let mut command = golden_command();
    command.lots = Some(100_000_000_000_000.0);
    assert!(matches!(
        build_execution_command_signing_string(&command, format),
        Err(SigningError::ScaledIntegerOverflow("lots"))
    ));
}

#[test]
fn action_specific_command_fields_fail_closed_before_signing() {
    let format = CommandSigningFormat::new(2, 2);

    let mut command = golden_command();
    command.lots = None;
    assert!(matches!(
        build_execution_command_signing_string(&command, format),
        Err(SigningError::MissingActionField {
            action: ExecutionAction::Buy,
            field: "lots"
        })
    ));

    let mut command = golden_command();
    command.order_type = None;
    assert!(matches!(
        build_execution_command_signing_string(&command, format),
        Err(SigningError::MissingActionField {
            action: ExecutionAction::Buy,
            field: "order_type"
        })
    ));

    let mut command = golden_command();
    command.order_type = Some(OrderType::Limit);
    command.price = None;
    assert!(matches!(
        build_execution_command_signing_string(&command, format),
        Err(SigningError::MissingActionField {
            action: ExecutionAction::Buy,
            field: "price"
        })
    ));

    let mut command = golden_command();
    command.action = ExecutionAction::Modify;
    command.order_type = None;
    command.lots = None;
    command.broker_order_id = None;
    command.position_ticket = None;
    assert!(matches!(
        build_execution_command_signing_string(&command, format),
        Err(SigningError::MissingCommandTarget {
            action: ExecutionAction::Modify
        })
    ));

    command.position_ticket = Some("position_1".into());
    command.price = None;
    command.sl = None;
    command.tp = None;
    command.expiration_time = None;
    assert!(matches!(
        build_execution_command_signing_string(&command, format),
        Err(SigningError::MissingModification)
    ));

    let mut command = golden_command();
    command.action = ExecutionAction::Cancel;
    command.order_type = None;
    command.lots = None;
    command.broker_order_id = None;
    assert!(matches!(
        build_execution_command_signing_string(&command, format),
        Err(SigningError::MissingActionField {
            action: ExecutionAction::Cancel,
            field: "broker_order_id"
        })
    ));
}
