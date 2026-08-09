use std::fmt;
use std::path::Path;
use std::str::FromStr;

use agent_knowledge_protocol::{ClientId, ClientIdError};

use crate::{KeyValidationError, NormalizedPublicKey};

const MAXIMUM_AUTHORIZED_KEYS_BYTES: usize = 4 * 1024 * 1024;
const FORCED_COMMAND_PREFIX: &str = "restrict,command=\"";
const FORCED_COMMAND_SUFFIX: char = '"';
const FORCED_COMMAND_VERSION: &str = "akg-v1";
const KEY_MARKER: &str = " ssh-ed25519 ";

/// One client recovered from the current static `authorized_keys` format.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ImportedClient {
    /// Client identity embedded in the forced command.
    pub client_id: ClientId,
    /// Validated public key.
    pub public_key: NormalizedPublicKey,
}

/// Parses the exact static format documented by Agent Knowledge.
///
/// Blank lines and comments are ignored. Other OpenSSH options are rejected so
/// migration cannot silently weaken or reinterpret an existing restriction.
///
/// # Errors
///
/// Returns an error when the file is oversized or a non-comment line does not
/// match the documented forced-command form.
pub fn parse_authorized_keys(
    input: &str,
) -> Result<Vec<ImportedClient>, AuthorizedKeysImportError> {
    if input.len() > MAXIMUM_AUTHORIZED_KEYS_BYTES {
        return Err(AuthorizedKeysImportError::TooLarge);
    }
    input
        .lines()
        .enumerate()
        .filter(|(_, line)| {
            let line = line.trim();
            !line.is_empty() && !line.starts_with('#')
        })
        .map(|(index, line)| parse_line(line.trim()).map_err(|error| error.at(index + 1)))
        .collect()
}

fn parse_line(line: &str) -> Result<ImportedClient, AuthorizedKeysImportError> {
    let key_start = line
        .find(KEY_MARKER)
        .ok_or(AuthorizedKeysImportError::InvalidLine { line: None })?;
    let options = &line[..key_start];
    let key = &line[key_start + 1..];
    let command = options
        .strip_prefix(FORCED_COMMAND_PREFIX)
        .and_then(|value| value.strip_suffix(FORCED_COMMAND_SUFFIX))
        .ok_or(AuthorizedKeysImportError::InvalidLine { line: None })?;
    let mut fields = command.split_ascii_whitespace();
    let version = fields.next();
    let config = fields.next();
    let client_id = fields.next();
    if version != Some(FORCED_COMMAND_VERSION)
        || config.is_none_or(|config| !Path::new(config).is_absolute())
        || client_id.is_none()
        || fields.next().is_some()
    {
        return Err(AuthorizedKeysImportError::InvalidLine { line: None });
    }
    Ok(ImportedClient {
        client_id: ClientId::from_str(client_id.unwrap_or_default())
            .map_err(AuthorizedKeysImportError::ClientId)?,
        public_key: NormalizedPublicKey::parse(key).map_err(AuthorizedKeysImportError::Key)?,
    })
}

/// Failure to migrate a static Agent Knowledge `authorized_keys` file.
#[derive(Debug)]
pub enum AuthorizedKeysImportError {
    /// The input exceeded the fixed migration bound.
    TooLarge,
    /// A non-comment line did not match the documented restriction.
    InvalidLine { line: Option<usize> },
    /// A forced command contained an invalid client ID.
    ClientId(ClientIdError),
    /// A key was malformed or unsupported.
    Key(KeyValidationError),
}

impl AuthorizedKeysImportError {
    fn at(self, line: usize) -> Self {
        match self {
            Self::InvalidLine { .. } => Self::InvalidLine { line: Some(line) },
            other => other,
        }
    }
}

impl fmt::Display for AuthorizedKeysImportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooLarge => formatter.write_str("authorized_keys exceeds the import size limit"),
            Self::InvalidLine { line: Some(line) } => write!(
                formatter,
                "authorized_keys line {line} is not a restricted Agent Knowledge key"
            ),
            Self::InvalidLine { line: None } => {
                formatter.write_str("invalid restricted Agent Knowledge key")
            }
            Self::ClientId(error) => write!(formatter, "invalid imported client ID: {error}"),
            Self::Key(error) => write!(formatter, "invalid imported public key: {error}"),
        }
    }
}

impl std::error::Error for AuthorizedKeysImportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ClientId(error) => Some(error),
            Self::Key(error) => Some(error),
            Self::TooLarge | Self::InvalidLine { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AuthorizedKeysImportError, parse_authorized_keys};

    const KEY: &str = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAILM+rvN+ot98qgEN796jTiQfZfG1KaT0PtFDJ/XFSqti fictional@example.invalid";

    #[test]
    fn imports_the_documented_forced_command_format() {
        let input = format!(
            "# fictional deployment\n\nrestrict,command=\"akg-v1 /etc/agent-knowledge/gateway.yaml fictional-node-a\" {KEY}\n"
        );
        let clients = parse_authorized_keys(&input)
            .unwrap_or_else(|error| panic!("fixture must import: {error}"));
        assert_eq!(clients.len(), 1);
        assert_eq!(clients[0].client_id.as_str(), "fictional-node-a");
    }

    #[test]
    fn rejects_unrestricted_and_shell_like_commands() {
        for input in [
            KEY.to_owned(),
            format!(
                "restrict,command=\"akg-v1 /etc/agent-knowledge/gateway.yaml fictional-node-a;id\" {KEY}"
            ),
        ] {
            assert!(matches!(
                parse_authorized_keys(&input),
                Err(AuthorizedKeysImportError::InvalidLine { .. })
                    | Err(AuthorizedKeysImportError::ClientId(_))
            ));
        }
    }
}
