use std::{
    collections::{BTreeSet, HashMap},
    fmt,
    sync::Arc,
};

use axum::http::{header::AUTHORIZATION, HeaderMap};
use sinan_store::AuthorizedAccountScope;
use sinan_types::AccountId;
use thiserror::Error;

/// Scopes understood by the Control Plane HTTP and Event WebSocket surfaces.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ControlPlaneScope {
    WriteIntent,
    ReadState,
    SubscribeEvents,
    DebugRead,
    ExecutionDebugSensitive,
    AdminMaintenance,
}

impl ControlPlaneScope {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::WriteIntent => "control-plane:write-intent",
            Self::ReadState => "control-plane:read-state",
            Self::SubscribeEvents => "event:subscribe",
            Self::DebugRead => "debug:read",
            Self::ExecutionDebugSensitive => "execution:debug-sensitive",
            Self::AdminMaintenance => "admin:maintenance",
        }
    }
}

impl fmt::Display for ControlPlaneScope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// An authenticated Control Plane identity with an explicit account allowlist.
///
/// Empty account scope means no accounts. It never means every account.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlPlanePrincipal {
    subject: String,
    scopes: BTreeSet<ControlPlaneScope>,
    account_scope: AuthorizedAccountScope,
}

impl ControlPlanePrincipal {
    pub fn new(
        subject: impl Into<String>,
        scopes: impl IntoIterator<Item = ControlPlaneScope>,
        account_ids: impl IntoIterator<Item = AccountId>,
    ) -> Self {
        Self::from_account_scope(subject, scopes, AuthorizedAccountScope::new(account_ids))
    }

    pub fn from_account_scope(
        subject: impl Into<String>,
        scopes: impl IntoIterator<Item = ControlPlaneScope>,
        account_scope: AuthorizedAccountScope,
    ) -> Self {
        Self {
            subject: subject.into(),
            scopes: scopes.into_iter().collect(),
            account_scope,
        }
    }

    pub fn subject(&self) -> &str {
        &self.subject
    }

    pub fn has_scope(&self, scope: ControlPlaneScope) -> bool {
        self.scopes.contains(&scope)
    }

    pub fn scopes(&self) -> impl Iterator<Item = ControlPlaneScope> + '_ {
        self.scopes.iter().copied()
    }

    pub fn account_scope(&self) -> &AuthorizedAccountScope {
        &self.account_scope
    }
}

/// A Control Plane token grant used only while constructing a fixed registry.
///
/// Execution Client credentials use a separate registry and cannot be passed
/// to this API without an explicit, visible conversion at the composition root.
pub struct ControlPlaneTokenGrant {
    token: String,
    principal: ControlPlanePrincipal,
}

impl ControlPlaneTokenGrant {
    pub fn new(token: impl Into<String>, principal: ControlPlanePrincipal) -> Self {
        Self {
            token: token.into(),
            principal,
        }
    }
}

/// Immutable Bearer-token registry for Control Plane callers.
#[derive(Clone)]
pub struct FixedBearerTokenRegistry {
    principals_by_token: Arc<HashMap<String, ControlPlanePrincipal>>,
}

impl fmt::Debug for FixedBearerTokenRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FixedBearerTokenRegistry")
            .field("grant_count", &self.principals_by_token.len())
            .finish_non_exhaustive()
    }
}

impl FixedBearerTokenRegistry {
    pub fn new(
        grants: impl IntoIterator<Item = ControlPlaneTokenGrant>,
    ) -> Result<Self, TokenRegistryError> {
        let mut principals_by_token = HashMap::new();
        for grant in grants {
            if grant.token.is_empty() || grant.token.chars().any(char::is_whitespace) {
                return Err(TokenRegistryError::InvalidToken);
            }
            if grant.principal.subject.trim().is_empty() {
                return Err(TokenRegistryError::EmptySubject);
            }
            if principals_by_token
                .insert(grant.token, grant.principal)
                .is_some()
            {
                return Err(TokenRegistryError::DuplicateToken);
            }
        }
        Ok(Self {
            principals_by_token: Arc::new(principals_by_token),
        })
    }

    /// Authenticates one RFC 6750 Bearer header without exposing token values.
    pub fn authenticate(
        &self,
        headers: &HeaderMap,
    ) -> Result<ControlPlanePrincipal, ControlPlaneAuthenticationError> {
        let mut values = headers.get_all(AUTHORIZATION).iter();
        let value = values
            .next()
            .ok_or(ControlPlaneAuthenticationError::MissingAuthorization)?;
        if values.next().is_some() {
            return Err(ControlPlaneAuthenticationError::InvalidAuthorization);
        }
        let value = value
            .to_str()
            .map_err(|_| ControlPlaneAuthenticationError::InvalidAuthorization)?;
        let (scheme, token) = value
            .split_once(' ')
            .ok_or(ControlPlaneAuthenticationError::InvalidAuthorization)?;
        if !scheme.eq_ignore_ascii_case("Bearer")
            || token.is_empty()
            || token.chars().any(char::is_whitespace)
        {
            return Err(ControlPlaneAuthenticationError::InvalidAuthorization);
        }
        self.principals_by_token
            .get(token)
            .cloned()
            .ok_or(ControlPlaneAuthenticationError::InvalidToken)
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum TokenRegistryError {
    #[error("control-plane bearer tokens must be non-empty and contain no whitespace")]
    InvalidToken,
    #[error("control-plane bearer token registry contains a duplicate token")]
    DuplicateToken,
    #[error("control-plane principal subject must not be empty")]
    EmptySubject,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ControlPlaneAuthenticationError {
    #[error("authorization header is required")]
    MissingAuthorization,
    #[error("authorization header must contain exactly one Bearer token")]
    InvalidAuthorization,
    #[error("bearer token is not recognized")]
    InvalidToken,
}
