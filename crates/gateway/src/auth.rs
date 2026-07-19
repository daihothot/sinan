use std::{collections::HashMap, fmt};

use sinan_protocol::{ExecutionClientPlatform, HelloPayload};
use sinan_types::{AccountId, ClientId, TerminalId};
use thiserror::Error;

/// Upper bound used to keep credential comparison work fixed and bounded.
pub const MAX_CLIENT_AUTH_TOKEN_BYTES: usize = 4_096;

/// A borrowed authentication request assembled from the transport handshake.
///
/// The token is deliberately borrowed and omitted from `Debug`, so it does not
/// become part of the authenticated session context or routine diagnostics.
#[derive(Clone, Copy)]
pub struct ClientAuthenticationRequest<'a> {
    client_id: &'a ClientId,
    account_id: &'a AccountId,
    terminal_id: Option<&'a TerminalId>,
    platform: ExecutionClientPlatform,
    token: &'a str,
    remote_addr: Option<&'a str>,
}

impl<'a> ClientAuthenticationRequest<'a> {
    pub const fn new(
        client_id: &'a ClientId,
        account_id: &'a AccountId,
        terminal_id: Option<&'a TerminalId>,
        platform: ExecutionClientPlatform,
        token: &'a str,
        remote_addr: Option<&'a str>,
    ) -> Self {
        Self {
            client_id,
            account_id,
            terminal_id,
            platform,
            token,
            remote_addr,
        }
    }

    pub fn from_hello(hello: &'a HelloPayload, remote_addr: Option<&'a str>) -> Self {
        Self::new(
            &hello.client_id,
            &hello.account_id,
            hello.terminal_id.as_ref(),
            hello.platform,
            &hello.token,
            remote_addr,
        )
    }

    pub const fn client_id(&self) -> &ClientId {
        self.client_id
    }

    pub const fn account_id(&self) -> &AccountId {
        self.account_id
    }

    pub const fn terminal_id(&self) -> Option<&TerminalId> {
        self.terminal_id
    }

    pub const fn platform(&self) -> ExecutionClientPlatform {
        self.platform
    }

    pub const fn token(&self) -> &str {
        self.token
    }

    pub const fn remote_addr(&self) -> Option<&str> {
        self.remote_addr
    }
}

impl fmt::Debug for ClientAuthenticationRequest<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClientAuthenticationRequest")
            .field("client_id", self.client_id)
            .field("account_id", self.account_id)
            .field("terminal_id", &self.terminal_id)
            .field("platform", &self.platform)
            .field("token", &"[REDACTED]")
            .field("remote_addr", &self.remote_addr)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClientSecretEpoch {
    Active,
    Next,
}

impl ClientSecretEpoch {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "ACTIVE",
            Self::Next => "NEXT",
        }
    }
}

/// Identity established by a successful client-auth handshake.
///
/// This type intentionally has no token or secret field.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthenticatedClient {
    pub client_id: ClientId,
    pub account_id: AccountId,
    pub terminal_id: Option<TerminalId>,
    pub platform: ExecutionClientPlatform,
    pub remote_addr: Option<String>,
    pub secret_epoch: ClientSecretEpoch,
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ClientAuthenticationError {
    #[error("client authentication failed")]
    Rejected,
}

/// Synchronous authentication boundary shared by every Execution Client
/// transport. It is object-safe so a connection driver can hold it behind an
/// `Arc<dyn ClientAuthenticator>`.
pub trait ClientAuthenticator: Send + Sync {
    fn authenticate(
        &self,
        request: ClientAuthenticationRequest<'_>,
    ) -> Result<AuthenticatedClient, ClientAuthenticationError>;
}

/// Static client-auth configuration scoped to one client/account pair.
#[derive(Clone)]
pub struct ClientCredential {
    pub client_id: ClientId,
    pub account_id: AccountId,
    pub active_secret: String,
    pub next_secret: Option<String>,
}

impl ClientCredential {
    pub fn new(
        client_id: impl Into<ClientId>,
        account_id: impl Into<AccountId>,
        active_secret: impl Into<String>,
        next_secret: Option<String>,
    ) -> Self {
        Self {
            client_id: client_id.into(),
            account_id: account_id.into(),
            active_secret: active_secret.into(),
            next_secret,
        }
    }
}

impl fmt::Debug for ClientCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClientCredential")
            .field("client_id", &self.client_id)
            .field("account_id", &self.account_id)
            .field("active_secret", &"[REDACTED]")
            .field("has_next_secret", &self.next_secret.is_some())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ClientAuthenticatorConfigError {
    #[error("client credential has an empty identity field: {0}")]
    EmptyIdentity(&'static str),

    #[error("client credential secret for {0} is empty")]
    EmptySecret(ClientSecretEpoch),

    #[error("client credential secret for {0} exceeds the configured maximum")]
    SecretTooLong(ClientSecretEpoch),

    #[error("client credential ACTIVE and NEXT secrets must differ")]
    IdenticalActiveAndNext,

    #[error("duplicate client credential key")]
    DuplicateCredential,
}

impl fmt::Display for ClientSecretEpoch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone)]
struct ClientSecrets {
    active: Vec<u8>,
    next: Option<Vec<u8>>,
}

/// In-memory authenticator for deployments whose credentials come from static
/// configuration or an already-resolved secret provider.
#[derive(Clone, Default)]
pub struct ConfiguredClientAuthenticator {
    credentials: HashMap<(ClientId, AccountId), ClientSecrets>,
}

/// Alternate name emphasizing that this implementation does no dynamic secret
/// lookup after construction.
pub type StaticClientAuthenticator = ConfiguredClientAuthenticator;

impl ConfiguredClientAuthenticator {
    pub fn new(
        credentials: impl IntoIterator<Item = ClientCredential>,
    ) -> Result<Self, ClientAuthenticatorConfigError> {
        let mut configured = HashMap::new();
        for credential in credentials {
            validate_credential(&credential)?;
            let key = (credential.client_id, credential.account_id);
            let secrets = ClientSecrets {
                active: credential.active_secret.into_bytes(),
                next: credential.next_secret.map(String::into_bytes),
            };
            if configured.insert(key, secrets).is_some() {
                return Err(ClientAuthenticatorConfigError::DuplicateCredential);
            }
        }
        Ok(Self {
            credentials: configured,
        })
    }

    pub fn credential_count(&self) -> usize {
        self.credentials.len()
    }
}

impl ClientAuthenticator for ConfiguredClientAuthenticator {
    fn authenticate(
        &self,
        request: ClientAuthenticationRequest<'_>,
    ) -> Result<AuthenticatedClient, ClientAuthenticationError> {
        if request.client_id.as_str().trim().is_empty()
            || request.account_id.as_str().trim().is_empty()
            || request.token.is_empty()
            || request.token.len() > MAX_CLIENT_AUTH_TOKEN_BYTES
        {
            return Err(ClientAuthenticationError::Rejected);
        }

        let secrets = self
            .credentials
            .get(&(request.client_id.clone(), request.account_id.clone()));
        let active = secrets.map_or(&[][..], |secrets| secrets.active.as_slice());
        let next = secrets
            .and_then(|secrets| secrets.next.as_deref())
            .unwrap_or(&[]);

        // Both epochs are always compared. Besides avoiding byte mismatch early
        // exits, this keeps ACTIVE and NEXT validation on the same code path.
        let active_matches = fixed_work_eq(request.token.as_bytes(), active);
        let next_matches = fixed_work_eq(request.token.as_bytes(), next);
        let secret_epoch = if secrets.is_some() && active_matches {
            ClientSecretEpoch::Active
        } else if secrets.is_some()
            && secrets.is_some_and(|secrets| secrets.next.is_some())
            && next_matches
        {
            ClientSecretEpoch::Next
        } else {
            return Err(ClientAuthenticationError::Rejected);
        };

        Ok(AuthenticatedClient {
            client_id: request.client_id.clone(),
            account_id: request.account_id.clone(),
            terminal_id: request.terminal_id.cloned(),
            platform: request.platform,
            remote_addr: request.remote_addr.map(str::to_owned),
            secret_epoch,
        })
    }
}

fn validate_credential(
    credential: &ClientCredential,
) -> Result<(), ClientAuthenticatorConfigError> {
    if credential.client_id.as_str().trim().is_empty() {
        return Err(ClientAuthenticatorConfigError::EmptyIdentity("client_id"));
    }
    if credential.account_id.as_str().trim().is_empty() {
        return Err(ClientAuthenticatorConfigError::EmptyIdentity("account_id"));
    }
    validate_secret(
        credential.active_secret.as_bytes(),
        ClientSecretEpoch::Active,
    )?;
    if let Some(next) = &credential.next_secret {
        validate_secret(next.as_bytes(), ClientSecretEpoch::Next)?;
        if fixed_work_eq(credential.active_secret.as_bytes(), next.as_bytes()) {
            return Err(ClientAuthenticatorConfigError::IdenticalActiveAndNext);
        }
    }
    Ok(())
}

fn validate_secret(
    secret: &[u8],
    epoch: ClientSecretEpoch,
) -> Result<(), ClientAuthenticatorConfigError> {
    if secret.is_empty() {
        Err(ClientAuthenticatorConfigError::EmptySecret(epoch))
    } else if secret.len() > MAX_CLIENT_AUTH_TOKEN_BYTES {
        Err(ClientAuthenticatorConfigError::SecretTooLong(epoch))
    } else {
        Ok(())
    }
}

/// Compares every byte position in the bounded credential space and never
/// exits on the first mismatch. Inputs must not exceed the public limit.
fn fixed_work_eq(left: &[u8], right: &[u8]) -> bool {
    debug_assert!(left.len() <= MAX_CLIENT_AUTH_TOKEN_BYTES);
    debug_assert!(right.len() <= MAX_CLIENT_AUTH_TOKEN_BYTES);

    let mut difference = left.len() ^ right.len();
    for index in 0..MAX_CLIENT_AUTH_TOKEN_BYTES {
        let left_byte = left.get(index).copied().unwrap_or_default();
        let right_byte = right.get(index).copied().unwrap_or_default();
        difference |= usize::from(left_byte ^ right_byte);
    }
    std::hint::black_box(difference) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn credential(
        client_id: &str,
        account_id: &str,
        active_secret: &str,
        next_secret: Option<&str>,
    ) -> ClientCredential {
        ClientCredential::new(
            client_id,
            account_id,
            active_secret,
            next_secret.map(str::to_owned),
        )
    }

    fn authenticate(
        authenticator: &dyn ClientAuthenticator,
        client_id: &ClientId,
        account_id: &AccountId,
        terminal_id: Option<&TerminalId>,
        token: &str,
    ) -> Result<AuthenticatedClient, ClientAuthenticationError> {
        authenticator.authenticate(ClientAuthenticationRequest::new(
            client_id,
            account_id,
            terminal_id,
            ExecutionClientPlatform::Mt5,
            token,
            Some("127.0.0.1:5000"),
        ))
    }

    #[test]
    fn accepts_active_and_next_secrets_and_reports_the_epoch() {
        let authenticator = ConfiguredClientAuthenticator::new([credential(
            "client-1",
            "account-1",
            "active-secret",
            Some("next-secret"),
        )])
        .unwrap();
        let client_id = ClientId::from("client-1");
        let account_id = AccountId::from("account-1");

        let active = authenticate(
            &authenticator,
            &client_id,
            &account_id,
            None,
            "active-secret",
        )
        .unwrap();
        let next =
            authenticate(&authenticator, &client_id, &account_id, None, "next-secret").unwrap();

        assert_eq!(active.secret_epoch, ClientSecretEpoch::Active);
        assert_eq!(next.secret_epoch, ClientSecretEpoch::Next);
    }

    #[test]
    fn rejects_wrong_empty_and_cross_account_tokens() {
        let authenticator = ConfiguredClientAuthenticator::new([credential(
            "client-1",
            "account-1",
            "account-1-secret",
            None,
        )])
        .unwrap();
        let client_id = ClientId::from("client-1");
        let account_1 = AccountId::from("account-1");
        let account_2 = AccountId::from("account-2");

        assert_eq!(
            authenticate(&authenticator, &client_id, &account_1, None, "wrong-secret"),
            Err(ClientAuthenticationError::Rejected)
        );
        assert_eq!(
            authenticate(&authenticator, &client_id, &account_1, None, ""),
            Err(ClientAuthenticationError::Rejected)
        );
        assert_eq!(
            authenticate(
                &authenticator,
                &client_id,
                &account_2,
                None,
                "account-1-secret"
            ),
            Err(ClientAuthenticationError::Rejected)
        );
    }

    #[test]
    fn successful_authentication_preserves_the_complete_transport_identity() {
        let authenticator = ConfiguredClientAuthenticator::new([credential(
            "client-1",
            "account-1",
            "active-secret",
            None,
        )])
        .unwrap();
        let client_id = ClientId::from("client-1");
        let account_id = AccountId::from("account-1");
        let terminal_id = TerminalId::from("Terminal/Desk A");

        let authenticated = authenticate(
            &authenticator,
            &client_id,
            &account_id,
            Some(&terminal_id),
            "active-secret",
        )
        .unwrap();

        assert_eq!(authenticated.client_id, client_id);
        assert_eq!(authenticated.account_id, account_id);
        assert_eq!(authenticated.terminal_id, Some(terminal_id));
        assert_eq!(authenticated.platform, ExecutionClientPlatform::Mt5);
        assert_eq!(authenticated.remote_addr.as_deref(), Some("127.0.0.1:5000"));
    }

    #[test]
    fn invalid_static_configuration_fails_closed() {
        assert_eq!(
            ConfiguredClientAuthenticator::new([credential("client-1", "account-1", "", None)])
                .err(),
            Some(ClientAuthenticatorConfigError::EmptySecret(
                ClientSecretEpoch::Active
            ))
        );
        assert_eq!(
            ConfiguredClientAuthenticator::new([credential(
                "client-1",
                "account-1",
                "same-secret",
                Some("same-secret")
            )])
            .err(),
            Some(ClientAuthenticatorConfigError::IdenticalActiveAndNext)
        );
        assert_eq!(
            ConfiguredClientAuthenticator::new([
                credential("client-1", "account-1", "first", None),
                credential("client-1", "account-1", "second", None),
            ])
            .err(),
            Some(ClientAuthenticatorConfigError::DuplicateCredential)
        );
    }

    #[test]
    fn debug_output_never_contains_request_or_configuration_secrets() {
        let token = "do-not-log-this-token";
        let client_id = ClientId::from("unknown-client");
        let account_id = AccountId::from("unknown-account");
        let request = ClientAuthenticationRequest::new(
            &client_id,
            &account_id,
            None,
            ExecutionClientPlatform::Paper,
            token,
            None,
        );
        let error = ConfiguredClientAuthenticator::default()
            .authenticate(request)
            .unwrap_err();
        let configuration = credential(
            "client-1",
            "account-1",
            token,
            Some("another-private-token"),
        );

        for diagnostic in [
            format!("{request:?}"),
            format!("{error:?}"),
            error.to_string(),
            format!("{configuration:?}"),
        ] {
            assert!(!diagnostic.contains(token));
            assert!(!diagnostic.contains("another-private-token"));
        }
    }
}
