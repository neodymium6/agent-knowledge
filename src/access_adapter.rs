use std::ffi::OsString;
use std::fmt;
use std::io::Write;
use std::path::PathBuf;

use agent_knowledge_access::{
    AccessRegistry, AccessRegistryError, AuthorizedKeysRenderError, write_authorized_keys,
};

#[derive(Debug)]
pub(crate) struct AuthorizedKeysSettings {
    pub(crate) registry_root: PathBuf,
    pub(crate) gateway_config: PathBuf,
    pub(crate) trusted_owner_uid: u32,
    pub(crate) gateway_user: OsString,
    pub(crate) requested_user: OsString,
}

pub(crate) fn parse<I>(mut arguments: I) -> Result<AuthorizedKeysSettings, ()>
where
    I: Iterator<Item = OsString>,
{
    let mut registry_root = None;
    let mut gateway_config = None;
    let mut trusted_owner_uid = None;
    let mut gateway_user = None;
    let mut requested_user = None;
    while let Some(flag) = arguments.next() {
        let value = arguments.next().ok_or(())?;
        match flag.to_str() {
            Some("--registry-root") if registry_root.is_none() => {
                registry_root = Some(PathBuf::from(value));
            }
            Some("--gateway-config") if gateway_config.is_none() => {
                gateway_config = Some(PathBuf::from(value));
            }
            Some("--trusted-owner-uid") if trusted_owner_uid.is_none() => {
                trusted_owner_uid = Some(value.to_str().ok_or(())?.parse().map_err(|_| ())?);
            }
            Some("--gateway-user") if gateway_user.is_none() => {
                gateway_user = Some(value);
            }
            Some("--requested-user") if requested_user.is_none() => {
                requested_user = Some(value);
            }
            _ => return Err(()),
        }
    }
    Ok(AuthorizedKeysSettings {
        registry_root: registry_root.ok_or(())?,
        gateway_config: gateway_config.ok_or(())?,
        trusted_owner_uid: trusted_owner_uid.ok_or(())?,
        gateway_user: gateway_user.ok_or(())?,
        requested_user: requested_user.ok_or(())?,
    })
}

pub(crate) fn execute(
    settings: AuthorizedKeysSettings,
    output: impl Write,
) -> Result<(), AuthorizedKeysAdapterError> {
    if settings.gateway_user != settings.requested_user {
        return Ok(());
    }
    let registry =
        AccessRegistry::open_existing(&settings.registry_root, settings.trusted_owner_uid)
            .map_err(AuthorizedKeysAdapterError::Registry)?;
    let snapshot = registry
        .current()
        .map_err(AuthorizedKeysAdapterError::Registry)?;
    write_authorized_keys(snapshot.as_ref(), &settings.gateway_config, output)
        .map_err(AuthorizedKeysAdapterError::Render)
}

#[derive(Debug)]
pub(crate) enum AuthorizedKeysAdapterError {
    Registry(AccessRegistryError),
    Render(AuthorizedKeysRenderError),
}

impl fmt::Display for AuthorizedKeysAdapterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Registry(error) => write!(formatter, "client access registry failed: {error}"),
            Self::Render(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for AuthorizedKeysAdapterError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Registry(error) => Some(error),
            Self::Render(error) => Some(error),
        }
    }
}
