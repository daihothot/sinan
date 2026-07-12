use serde::{Deserialize, Serialize};
use std::{borrow::Borrow, fmt, ops::Deref};

macro_rules! string_newtype {
    ($($name:ident),+ $(,)?) => {
        $(
            #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
            #[serde(transparent)]
            pub struct $name(String);

            impl $name {
                pub fn new(value: impl Into<String>) -> Self {
                    Self(value.into())
                }

                pub fn as_str(&self) -> &str {
                    &self.0
                }

                pub fn into_inner(self) -> String {
                    self.0
                }

                pub fn is_empty(&self) -> bool {
                    self.0.is_empty()
                }
            }

            impl AsRef<str> for $name {
                fn as_ref(&self) -> &str {
                    self.as_str()
                }
            }

            impl Borrow<str> for $name {
                fn borrow(&self) -> &str {
                    self.as_str()
                }
            }

            impl Deref for $name {
                type Target = str;

                fn deref(&self) -> &Self::Target {
                    self.as_str()
                }
            }

            impl fmt::Display for $name {
                fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                    formatter.write_str(self.as_str())
                }
            }

            impl From<String> for $name {
                fn from(value: String) -> Self {
                    Self(value)
                }
            }

            impl From<&str> for $name {
                fn from(value: &str) -> Self {
                    Self(value.to_owned())
                }
            }

            impl From<$name> for String {
                fn from(value: $name) -> Self {
                    value.into_inner()
                }
            }
        )+
    };
}

string_newtype!(
    AccountId,
    BrokerDealId,
    BrokerOrderId,
    CausationId,
    ClientId,
    CommandId,
    CorrelationId,
    DecisionId,
    ExecutionId,
    IdempotencyKey,
    IntentId,
    LegId,
    MessageId,
    PlanId,
    PositionId,
    PositionTicket,
    RequestId,
    RiskId,
    SessionId,
    StrategyId,
    SymbolCode,
    TerminalId,
    TimeframeCode,
);
