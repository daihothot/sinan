use hmac::{Hmac, Mac};
use serde_json::{Map, Value};
use sha2::Sha256;
use sinan_types::{ExecutionAction, ExecutionCommand, OrderType, SymbolMetadataSnapshot};
use thiserror::Error;

const MAX_DECIMAL_DIGITS: u32 = 18;
const MAX_SAFE_SCALED_INTEGER: f64 = 9_007_199_254_740_991.0;

const SIGNING_FIELDS: [&str; 25] = [
    "command_id",
    "plan_id",
    "leg_id",
    "strategy_id",
    "account_id",
    "terminal_id",
    "client_id",
    "symbol",
    "broker_symbol",
    "action",
    "order_type",
    "lots",
    "price",
    "sl",
    "tp",
    "deviation_points",
    "magic",
    "comment",
    "position_ticket",
    "broker_order_id",
    "filling_policy",
    "time_policy",
    "expiration_time",
    "expires_at",
    "idempotency_key",
];

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CommandSigningFormat {
    pub price_digits: u32,
    pub volume_digits: u32,
    volume_step: Option<VolumeStepConstraint>,
}

impl CommandSigningFormat {
    /// Creates a format without a volume-step alignment constraint.
    pub const fn new(price_digits: u32, volume_digits: u32) -> Self {
        Self {
            price_digits,
            volume_digits,
            volume_step: None,
        }
    }

    pub fn from_symbol_metadata(metadata: &SymbolMetadataSnapshot) -> Result<Self, SigningError> {
        validate_precision(metadata.digits)?;
        let (volume_digits, volume_step_units) = volume_step_units(metadata.volume_step)?;

        Ok(Self {
            price_digits: metadata.digits,
            volume_digits,
            volume_step: Some(VolumeStepConstraint {
                value: metadata.volume_step,
                units: volume_step_units,
            }),
        })
    }

    pub fn volume_step(self) -> Option<f64> {
        self.volume_step.map(|constraint| constraint.value)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct VolumeStepConstraint {
    value: f64,
    units: i64,
}

pub fn build_execution_command_signing_string(
    command: &ExecutionCommand,
    format: CommandSigningFormat,
) -> Result<String, SigningError> {
    validate_execution_command(command, format)?;

    let value = serde_json::to_value(command)?;
    let fields = value
        .as_object()
        .ok_or(SigningError::CommandMustSerializeAsObject)?;

    SIGNING_FIELDS
        .iter()
        .map(|field| {
            let value = signing_field_value(fields, field, format)?;
            Ok(format!("{field}={}", rfc3986_encode(&value)))
        })
        .collect::<Result<Vec<_>, SigningError>>()
        .map(|fields| fields.join("&"))
}

pub fn sign_execution_command(
    command: &ExecutionCommand,
    secret: &[u8],
    format: CommandSigningFormat,
) -> Result<String, SigningError> {
    let signing_string = build_execution_command_signing_string(command, format)?;
    let mut mac =
        <Hmac<Sha256> as Mac>::new_from_slice(secret).map_err(|_| SigningError::InvalidSecret)?;
    mac.update(signing_string.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

pub fn verify_execution_command_hmac(
    command: &ExecutionCommand,
    secret: &[u8],
    format: CommandSigningFormat,
) -> Result<(), SigningError> {
    let signing_string = build_execution_command_signing_string(command, format)?;
    let expected = command.hmac.as_str();

    if expected.len() != 64
        || !expected
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
    {
        return Err(SigningError::InvalidHmacEncoding);
    }

    let expected = hex::decode(expected).map_err(|_| SigningError::InvalidHmacEncoding)?;
    let mut mac =
        <Hmac<Sha256> as Mac>::new_from_slice(secret).map_err(|_| SigningError::InvalidSecret)?;
    mac.update(signing_string.as_bytes());
    mac.verify_slice(&expected)
        .map_err(|_| SigningError::HmacMismatch)
}

fn signing_field_value(
    fields: &Map<String, Value>,
    field: &'static str,
    format: CommandSigningFormat,
) -> Result<String, SigningError> {
    let Some(value) = fields.get(field) else {
        return if is_optional_field(field) {
            Ok(String::new())
        } else {
            Err(SigningError::MissingRequiredField(field))
        };
    };

    match value {
        Value::Null if is_optional_field(field) => Ok(String::new()),
        Value::Null => Err(SigningError::MissingRequiredField(field)),
        Value::String(value) => Ok(value.clone()),
        Value::Number(number) if field == "lots" => {
            fixed_decimal(number, format.volume_digits, field)
        }
        Value::Number(number) if matches!(field, "price" | "sl" | "tp") => {
            fixed_decimal(number, format.price_digits, field)
        }
        Value::Number(number) => Ok(number.to_string()),
        _ => Err(SigningError::InvalidFieldType(field)),
    }
}

fn fixed_decimal(
    number: &serde_json::Number,
    digits: u32,
    field: &'static str,
) -> Result<String, SigningError> {
    let value = number
        .as_f64()
        .ok_or(SigningError::InvalidFieldType(field))?;
    format_fixed_decimal(value, digits)
}

fn validate_execution_command(
    command: &ExecutionCommand,
    format: CommandSigningFormat,
) -> Result<(), SigningError> {
    validate_precision(format.price_digits)?;
    validate_precision(format.volume_digits)?;
    validate_finite_numbers(command)?;
    validate_action_specific_fields(command)?;
    validate_lots_alignment(command.lots, format)
}

fn validate_finite_numbers(command: &ExecutionCommand) -> Result<(), SigningError> {
    for (field, value) in [
        ("lots", command.lots),
        ("price", command.price),
        ("sl", command.sl),
        ("tp", command.tp),
    ] {
        if value.is_some_and(|value| !value.is_finite()) {
            return Err(SigningError::NonFiniteNumber(field));
        }
    }
    Ok(())
}

fn validate_action_specific_fields(command: &ExecutionCommand) -> Result<(), SigningError> {
    match command.action {
        ExecutionAction::Buy | ExecutionAction::Sell => {
            require_action_field(command.action, "lots", command.lots.is_some())?;
            require_action_field(command.action, "order_type", command.order_type.is_some())?;
            if matches!(
                command.order_type,
                Some(OrderType::Limit | OrderType::Stop | OrderType::StopLimit)
            ) {
                require_action_field(command.action, "price", command.price.is_some())?;
            }
        }
        ExecutionAction::Modify => {
            if command.broker_order_id.is_none() && command.position_ticket.is_none() {
                return Err(SigningError::MissingCommandTarget {
                    action: command.action,
                });
            }
            if command.sl.is_none()
                && command.tp.is_none()
                && command.price.is_none()
                && command.expiration_time.is_none()
            {
                return Err(SigningError::MissingModification);
            }
        }
        ExecutionAction::Cancel => {
            require_action_field(
                command.action,
                "broker_order_id",
                command.broker_order_id.is_some(),
            )?;
        }
        ExecutionAction::Close => {}
    }

    Ok(())
}

fn require_action_field(
    action: ExecutionAction,
    field: &'static str,
    present: bool,
) -> Result<(), SigningError> {
    if !present {
        return Err(SigningError::MissingActionField { action, field });
    }
    Ok(())
}

fn validate_lots_alignment(
    lots: Option<f64>,
    format: CommandSigningFormat,
) -> Result<(), SigningError> {
    let (Some(lots), Some(volume_step)) = (lots, format.volume_step) else {
        return Ok(());
    };

    let lots_units = scaled_integer(lots, format.volume_digits, "lots")?;
    if lots_units.rem_euclid(volume_step.units) != 0 {
        return Err(SigningError::LotsNotAlignedToVolumeStep {
            lots,
            volume_step: volume_step.value,
        });
    }
    Ok(())
}

fn is_optional_field(field: &str) -> bool {
    matches!(
        field,
        "plan_id"
            | "leg_id"
            | "terminal_id"
            | "client_id"
            | "broker_symbol"
            | "order_type"
            | "lots"
            | "price"
            | "sl"
            | "tp"
            | "deviation_points"
            | "comment"
            | "position_ticket"
            | "broker_order_id"
            | "filling_policy"
            | "time_policy"
            | "expiration_time"
    )
}

pub fn format_fixed_decimal(value: f64, digits: u32) -> Result<String, SigningError> {
    if !value.is_finite() {
        return Err(SigningError::NonFiniteValue);
    }
    validate_precision(digits)?;
    let digits = digits as usize;
    Ok(format!("{value:.digits$}"))
}

pub fn rfc3986_encode(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";

    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push('%');
            encoded.push(char::from(HEX[(byte >> 4) as usize]));
            encoded.push(char::from(HEX[(byte & 0x0f) as usize]));
        }
    }
    encoded
}

fn volume_step_units(step: f64) -> Result<(u32, i64), SigningError> {
    if !step.is_finite() || step <= 0.0 {
        return Err(SigningError::InvalidVolumeStep);
    }

    for digits in 0..=MAX_DECIMAL_DIGITS {
        match scaled_integer(step, digits, "volume_step") {
            Ok(units) if units > 0 => return Ok((digits, units)),
            Err(SigningError::DecimalPrecisionExceeded { .. }) => {}
            Err(error) => return Err(error),
            Ok(_) => return Err(SigningError::InvalidVolumeStep),
        }
    }

    Err(SigningError::InvalidVolumeStep)
}

fn scaled_integer(value: f64, digits: u32, field: &'static str) -> Result<i64, SigningError> {
    if !value.is_finite() {
        return Err(SigningError::NonFiniteNumber(field));
    }
    let scale = decimal_scale(digits)?;
    let scaled = value * scale as f64;
    if !scaled.is_finite() || scaled.abs() > MAX_SAFE_SCALED_INTEGER {
        return Err(SigningError::ScaledIntegerOverflow(field));
    }

    let nearest = scaled.round();
    let tolerance = f64::EPSILON * scaled.abs().max(1.0) * 8.0;
    if (scaled - nearest).abs() > tolerance {
        return Err(SigningError::DecimalPrecisionExceeded { field, digits });
    }

    Ok(nearest as i64)
}

fn decimal_scale(digits: u32) -> Result<i64, SigningError> {
    validate_precision(digits)?;
    10_i64
        .checked_pow(digits)
        .ok_or(SigningError::PrecisionTooLarge(digits))
}

fn validate_precision(digits: u32) -> Result<(), SigningError> {
    if digits > MAX_DECIMAL_DIGITS {
        return Err(SigningError::PrecisionTooLarge(digits));
    }
    Ok(())
}

#[derive(Debug, Error)]
pub enum SigningError {
    #[error("failed to serialize execution command: {0}")]
    Serialize(#[from] serde_json::Error),

    #[error("ExecutionCommand must serialize as a JSON object")]
    CommandMustSerializeAsObject,

    #[error("execution command is missing required signing field: {0}")]
    MissingRequiredField(&'static str),

    #[error("execution command signing field has an invalid type: {0}")]
    InvalidFieldType(&'static str),

    #[error("execution command signing field is not finite: {0}")]
    NonFiniteNumber(&'static str),

    #[error("{action} execution command requires field {field}")]
    MissingActionField {
        action: ExecutionAction,
        field: &'static str,
    },

    #[error("{action} execution command requires broker_order_id or position_ticket")]
    MissingCommandTarget { action: ExecutionAction },

    #[error("MODIFY execution command requires sl, tp, price, or expiration_time")]
    MissingModification,

    #[error("lots {lots} is not aligned to volume_step {volume_step}")]
    LotsNotAlignedToVolumeStep { lots: f64, volume_step: f64 },

    #[error("{0} cannot be represented safely as a scaled integer")]
    ScaledIntegerOverflow(&'static str),

    #[error("{field} has more than {digits} decimal places")]
    DecimalPrecisionExceeded { field: &'static str, digits: u32 },

    #[error("fixed decimal value must be finite")]
    NonFiniteValue,

    #[error("fixed decimal precision is too large: {0}")]
    PrecisionTooLarge(u32),

    #[error("volume_step must be a positive decimal with at most 18 places")]
    InvalidVolumeStep,

    #[error("HMAC secret is invalid")]
    InvalidSecret,

    #[error("execution command has no HMAC")]
    MissingHmac,

    #[error("execution command HMAC must be 64 lowercase hexadecimal characters")]
    InvalidHmacEncoding,

    #[error("execution command HMAC does not match")]
    HmacMismatch,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_decimal_preserves_trailing_zeroes() {
        assert_eq!(format_fixed_decimal(0.1, 2).unwrap(), "0.10");
        assert_eq!(format_fixed_decimal(2320.5, 2).unwrap(), "2320.50");
    }

    #[test]
    fn rfc3986_encoding_handles_spaces_unicode_and_reserved_bytes() {
        assert_eq!(
            rfc3986_encode("order note/\u{4e2d}\u{6587}&x=1"),
            "order%20note%2F%E4%B8%AD%E6%96%87%26x%3D1"
        );
    }

    #[test]
    fn derives_decimal_places_from_volume_step() {
        assert_eq!(volume_step_units(1.0).unwrap(), (0, 1));
        assert_eq!(volume_step_units(0.1).unwrap(), (1, 1));
        assert_eq!(volume_step_units(0.01).unwrap(), (2, 1));
        assert_eq!(volume_step_units(0.25).unwrap(), (2, 25));
    }
}
