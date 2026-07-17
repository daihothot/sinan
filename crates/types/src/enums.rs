use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{error::Error, fmt, str::FromStr};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParseEnumError {
    enum_name: &'static str,
    value: String,
}

impl ParseEnumError {
    pub fn enum_name(&self) -> &'static str {
        self.enum_name
    }

    pub fn value(&self) -> &str {
        &self.value
    }
}

impl fmt::Display for ParseEnumError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "unknown {} value: {}",
            self.enum_name, self.value
        )
    }
}

impl Error for ParseEnumError {}

macro_rules! string_enum {
    (
        $(#[$meta:meta])*
        pub enum $name:ident {
            $($variant:ident => $value:literal),+ $(,)?
        }
    ) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
        pub enum $name {
            $(#[serde(rename = $value)] $variant),+
        }

        impl $name {
            pub const ALL: &'static [Self] = &[$(Self::$variant),+];

            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $value),+
                }
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }

        impl FromStr for $name {
            type Err = ParseEnumError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                match value {
                    $($value => Ok(Self::$variant)),+,
                    _ => Err(ParseEnumError {
                        enum_name: stringify!($name),
                        value: value.to_owned(),
                    }),
                }
            }
        }
    };
}

string_enum! {
    pub enum ErrorCode {
        AccountSnapshotStale => "ACCOUNT_SNAPSHOT_STALE",
        SymbolMetadataStale => "SYMBOL_METADATA_STALE",
        OrderSnapshotStale => "ORDER_SNAPSHOT_STALE",
        TradeIntentExpired => "TRADE_INTENT_EXPIRED",
        TradeIntentTimeInvalid => "TRADE_INTENT_TIME_INVALID",
        DuplicateTradeIntent => "DUPLICATE_TRADE_INTENT",
        DuplicateCommand => "DUPLICATE_COMMAND",
        DuplicateIdempotencyConflict => "DUPLICATE_IDEMPOTENCY_CONFLICT",
        InvalidHmac => "INVALID_HMAC",
        AuthenticationFailed => "AUTHENTICATION_FAILED",
        SessionIdentityMismatch => "SESSION_IDENTITY_MISMATCH",
        CommandExpired => "COMMAND_EXPIRED",
        CommandDeliveryTimeout => "COMMAND_DELIVERY_TIMEOUT",
        CommandDeliveryUnconfirmed => "COMMAND_DELIVERY_UNCONFIRMED",
        CommandDeliveryFailed => "COMMAND_DELIVERY_FAILED",
        CommandDispatchBackpressure => "COMMAND_DISPATCH_BACKPRESSURE",
        CommandInflightLimitReached => "COMMAND_INFLIGHT_LIMIT_REACHED",
        BrokerRejected => "BROKER_REJECTED",
        BrokerTimeout => "BROKER_TIMEOUT",
        InsufficientMargin => "INSUFFICIENT_MARGIN",
        InvalidVolume => "INVALID_VOLUME",
        InvalidPrice => "INVALID_PRICE",
        InvalidStops => "INVALID_STOPS",
        TradeModeDisabled => "TRADE_MODE_DISABLED",
        ReconciliationFailed => "RECONCILIATION_FAILED",
        ManualReconciliationRequired => "MANUAL_RECONCILIATION_REQUIRED",
        SchemaValidationFailed => "SCHEMA_VALIDATION_FAILED",
        BadRequest => "BAD_REQUEST",
        Unauthorized => "UNAUTHORIZED",
        Forbidden => "FORBIDDEN",
        NotFound => "NOT_FOUND",
        MethodNotAllowed => "METHOD_NOT_ALLOWED",
        Conflict => "CONFLICT",
        IdempotencyKeyConflict => "IDEMPOTENCY_KEY_CONFLICT",
        RateLimited => "RATE_LIMITED",
        InternalError => "INTERNAL_ERROR",
        ServiceUnavailable => "SERVICE_UNAVAILABLE",
        MarketSnapshotStale => "MARKET_SNAPSHOT_STALE",
        RiskInputInvalid => "RISK_INPUT_INVALID",
        RiskLimitExceeded => "RISK_LIMIT_EXCEEDED",
        ExposureLimitExceeded => "EXPOSURE_LIMIT_EXCEEDED",
        PositionLimitExceeded => "POSITION_LIMIT_EXCEEDED",
        RiskReductionNotProvable => "RISK_REDUCTION_NOT_PROVABLE",
        PendingExposureConflict => "PENDING_EXPOSURE_CONFLICT",
        RiskEngineCircuitBreakerTriggered => "RISK_ENGINE_CIRCUIT_BREAKER_TRIGGERED",
        RedisUnavailable => "REDIS_UNAVAILABLE",
        StateStoreUnavailable => "STATE_STORE_UNAVAILABLE",
        ClockSkewDetected => "CLOCK_SKEW_DETECTED",
        TimeSyncUnhealthy => "TIME_SYNC_UNHEALTHY",
        LongTermAuditWriteFailed => "LONG_TERM_AUDIT_WRITE_FAILED",
        SecretRotationFailed => "SECRET_ROTATION_FAILED",
        DeadletterCreated => "DEADLETTER_CREATED",
        UnknownType => "UNKNOWN_TYPE",
        SchemaMajorMismatch => "SCHEMA_MAJOR_MISMATCH",
        MissingRequiredField => "MISSING_REQUIRED_FIELD",
        InvalidFieldType => "INVALID_FIELD_TYPE",
        DecodeFailed => "DECODE_FAILED",
        WireFrameTooLarge => "WIRE_FRAME_TOO_LARGE",
        WireProtocolViolation => "WIRE_PROTOCOL_VIOLATION"
    }
}

string_enum! {
    pub enum TradeIntentAction {
        Buy => "BUY",
        Sell => "SELL",
        Close => "CLOSE",
        Hold => "HOLD"
    }
}

string_enum! {
    pub enum TradeIntentLegAction {
        Buy => "BUY",
        Sell => "SELL",
        Close => "CLOSE"
    }
}

string_enum! {
    pub enum AdjustedRiskLegAction {
        Buy => "BUY",
        Sell => "SELL"
    }
}

string_enum! {
    pub enum PositionSide {
        Buy => "BUY",
        Sell => "SELL"
    }
}

string_enum! {
    pub enum OrderType {
        Market => "MARKET",
        Limit => "LIMIT",
        Stop => "STOP",
        StopLimit => "STOP_LIMIT"
    }
}

string_enum! {
    pub enum OrderSnapshotStatus {
        Placed => "PLACED",
        PartiallyFilled => "PARTIALLY_FILLED",
        Filled => "FILLED",
        Cancelled => "CANCELLED",
        Rejected => "REJECTED",
        Expired => "EXPIRED",
        Unknown => "UNKNOWN"
    }
}

string_enum! {
    pub enum SymbolTradeMode {
        Full => "FULL",
        LongOnly => "LONG_ONLY",
        ShortOnly => "SHORT_ONLY",
        CloseOnly => "CLOSE_ONLY",
        Disabled => "DISABLED"
    }
}

string_enum! {
    pub enum ExecutionAction {
        Buy => "BUY",
        Sell => "SELL",
        Close => "CLOSE",
        Modify => "MODIFY",
        Cancel => "CANCEL"
    }
}

string_enum! {
    pub enum FillingPolicy {
        Fok => "FOK",
        Ioc => "IOC",
        Return => "RETURN"
    }
}

string_enum! {
    pub enum TimePolicy {
        Gtc => "GTC",
        Day => "DAY",
        Specified => "SPECIFIED"
    }
}

string_enum! {
    pub enum ExecutionEventStatus {
        Accepted => "ACCEPTED",
        OrderSent => "ORDER_SENT",
        Rejected => "REJECTED",
        Filled => "FILLED",
        PartiallyFilled => "PARTIALLY_FILLED",
        Failed => "FAILED",
        Expired => "EXPIRED",
        Cancelled => "CANCELLED"
    }
}

string_enum! {
    pub enum ExecutionCommandStatus {
        Created => "CREATED",
        Dispatched => "DISPATCHED",
        DeliveryUnconfirmed => "DELIVERY_UNCONFIRMED",
        DeliveryFailed => "DELIVERY_FAILED",
        Reconciling => "RECONCILING",
        ManualReconciliationRequired => "MANUAL_RECONCILIATION_REQUIRED",
        CommandReceived => "COMMAND_RECEIVED",
        Accepted => "ACCEPTED",
        Rejected => "REJECTED",
        OrderSent => "ORDER_SENT",
        PartiallyFilled => "PARTIALLY_FILLED",
        Filled => "FILLED",
        Failed => "FAILED",
        Expired => "EXPIRED",
        Cancelled => "CANCELLED"
    }
}

string_enum! {
    pub enum SessionStatus {
        Active => "ACTIVE",
        Stale => "STALE",
        Disconnected => "DISCONNECTED",
        Rejected => "REJECTED"
    }
}

string_enum! {
    pub enum ClockSyncStatus {
        Synced => "SYNCED",
        Degraded => "DEGRADED",
        Unsynced => "UNSYNCED"
    }
}

string_enum! {
    pub enum WireInboxStatus {
        Received => "RECEIVED",
        Acked => "ACKED",
        Handled => "HANDLED",
        Duplicate => "DUPLICATE",
        Deadletter => "DEADLETTER",
        Failed => "FAILED"
    }
}

string_enum! {
    pub enum WireOutboxStatus {
        Pending => "PENDING",
        Sent => "SENT",
        Acked => "ACKED",
        Failed => "FAILED",
        Cancelled => "CANCELLED"
    }
}

string_enum! {
    pub enum TradeIntentStatus {
        Accepted => "ACCEPTED",
        RiskBlocked => "RISK_BLOCKED",
        Rejected => "REJECTED",
        Duplicate => "DUPLICATE",
        Expired => "EXPIRED",
        Cancelled => "CANCELLED"
    }
}

string_enum! {
    pub enum ExecutionPlanMode {
        Sequential => "sequential",
        Simultaneous => "simultaneous",
        BestEffortAtomic => "best_effort_atomic"
    }
}

string_enum! {
    pub enum ExecutionFailurePolicy {
        CancelAll => "cancel_all",
        PartialFill => "partial_fill",
        Retry => "retry"
    }
}

string_enum! {
    pub enum ExecutionPlanStatus {
        Pending => "PENDING",
        Reconciling => "RECONCILING",
        ManualReconciliationRequired => "MANUAL_RECONCILIATION_REQUIRED",
        Partial => "PARTIAL",
        Completed => "COMPLETED",
        Failed => "FAILED",
        Expired => "EXPIRED",
        Cancelled => "CANCELLED"
    }
}

string_enum! {
    pub enum ExecutionLegStatus {
        Pending => "PENDING",
        Sent => "SENT",
        DeliveryUnconfirmed => "DELIVERY_UNCONFIRMED",
        Reconciling => "RECONCILING",
        ManualReconciliationRequired => "MANUAL_RECONCILIATION_REQUIRED",
        CommandReceived => "COMMAND_RECEIVED",
        Accepted => "ACCEPTED",
        Rejected => "REJECTED",
        OrderSent => "ORDER_SENT",
        PartiallyFilled => "PARTIALLY_FILLED",
        Filled => "FILLED",
        Failed => "FAILED",
        Expired => "EXPIRED",
        Cancelled => "CANCELLED"
    }
}

string_enum! {
    pub enum CommandDeliveryAttemptStatus {
        Pending => "PENDING",
        Sent => "SENT",
        Acked => "ACKED",
        Backpressure => "BACKPRESSURE",
        NoActiveSession => "NO_ACTIVE_SESSION",
        Failed => "FAILED",
        Timeout => "TIMEOUT",
        Cancelled => "CANCELLED"
    }
}

string_enum! {
    pub enum SystemEventSeverity {
        Info => "INFO",
        Warning => "WARNING",
        Error => "ERROR",
        Critical => "CRITICAL"
    }
}

string_enum! {
    pub enum EventStreamTopic {
        MarketSnapshot => "market.snapshot",
        RiskSummary => "risk.summary",
        ExecutionSummary => "execution.summary",
        SystemEvent => "system.event",
        DeadletterSummary => "deadletter.summary"
    }
}

string_enum! {
    pub enum OutboundSpoolStatus {
        Pending => "PENDING",
        Sent => "SENT",
        Acked => "ACKED",
        Failed => "FAILED",
        Retrying => "RETRYING",
        Deadletter => "DEADLETTER"
    }
}

/// A broker or adapter error may be outside the centrally managed error-code set.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum ErrorCodeOrString {
    Known(ErrorCode),
    Other(String),
}

impl ErrorCodeOrString {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Known(code) => code.as_str(),
            Self::Other(value) => value,
        }
    }
}

impl fmt::Display for ErrorCodeOrString {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl From<ErrorCode> for ErrorCodeOrString {
    fn from(value: ErrorCode) -> Self {
        Self::Known(value)
    }
}

impl From<String> for ErrorCodeOrString {
    fn from(value: String) -> Self {
        value
            .parse::<ErrorCode>()
            .map_or(Self::Other(value), Self::Known)
    }
}

impl From<&str> for ErrorCodeOrString {
    fn from(value: &str) -> Self {
        Self::from(value.to_owned())
    }
}

impl Serialize for ErrorCodeOrString {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for ErrorCodeOrString {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer).map(Self::from)
    }
}
