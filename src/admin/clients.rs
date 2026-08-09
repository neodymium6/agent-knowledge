use std::ffi::OsString;
use std::fmt;
use std::io::{self, Write};
use std::path::PathBuf;

use agent_knowledge_access::{
    AccessRegistry, AccessRegistryError, AuthorizedKeysImportError, KeyValidationError,
    MutationOutcome, NormalizedPublicKey, RegistrySnapshot, parse_authorized_keys,
};
use agent_knowledge_core::{BoundedFileError, read_bounded_regular_file};
use agent_knowledge_protocol::ClientId;
use serde::Serialize;

const LOCAL_ADMIN_ACTOR: &str = "local-admin";
const MAXIMUM_PUBLIC_KEY_FILE_BYTES: u64 = 16 * 1024;
const MAXIMUM_AUTHORIZED_KEYS_FILE_BYTES: u64 = 4 * 1024 * 1024;

mod web;

/// One local client-registry administration command.
#[derive(Debug)]
pub(crate) enum ClientAdminCommand {
    List {
        registry_root: PathBuf,
    },
    Add {
        registry_root: PathBuf,
        client_id: ClientId,
        public_key_file: PathBuf,
    },
    Disable {
        registry_root: PathBuf,
        client_id: ClientId,
    },
    Enable {
        registry_root: PathBuf,
        client_id: ClientId,
    },
    RotateKey {
        registry_root: PathBuf,
        client_id: ClientId,
        expected_fingerprint: String,
        public_key_file: PathBuf,
    },
    ImportAuthorizedKeys {
        registry_root: PathBuf,
        authorized_keys_file: PathBuf,
    },
    Serve {
        registry_root: PathBuf,
        socket_path: PathBuf,
    },
}

pub(crate) fn parse<I>(mut arguments: I) -> Result<ClientAdminCommand, ()>
where
    I: Iterator<Item = OsString>,
{
    let action = arguments
        .next()
        .and_then(|value| value.into_string().ok())
        .ok_or(())?;
    let mut registry_root = None;
    let mut client_id = None;
    let mut public_key_file = None;
    let mut expected_fingerprint = None;
    let mut authorized_keys_file = None;
    let mut socket_path = None;
    while let Some(flag) = arguments.next() {
        let value = arguments.next().ok_or(())?;
        match flag.to_str() {
            Some("--registry-root") if registry_root.is_none() => {
                registry_root = Some(PathBuf::from(value));
            }
            Some("--client-id") if client_id.is_none() => {
                let value = value.to_str().ok_or(())?;
                client_id = Some(value.parse().map_err(|_| ())?);
            }
            Some("--public-key-file") if public_key_file.is_none() => {
                public_key_file = Some(PathBuf::from(value));
            }
            Some("--expected-fingerprint") if expected_fingerprint.is_none() => {
                let value = value.into_string().map_err(|_| ())?;
                let encoded = value.strip_prefix("SHA256:").ok_or(())?;
                if encoded.len() != 43
                    || !encoded
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'/'))
                {
                    return Err(());
                }
                expected_fingerprint = Some(value);
            }
            Some("--authorized-keys-file") if authorized_keys_file.is_none() => {
                authorized_keys_file = Some(PathBuf::from(value));
            }
            Some("--socket-path") if socket_path.is_none() => {
                socket_path = Some(PathBuf::from(value));
            }
            _ => return Err(()),
        }
    }
    let registry_root = registry_root.ok_or(())?;
    match action.as_str() {
        "list"
            if client_id.is_none()
                && public_key_file.is_none()
                && expected_fingerprint.is_none()
                && authorized_keys_file.is_none()
                && socket_path.is_none() =>
        {
            Ok(ClientAdminCommand::List { registry_root })
        }
        "add"
            if expected_fingerprint.is_none()
                && authorized_keys_file.is_none()
                && socket_path.is_none() =>
        {
            Ok(ClientAdminCommand::Add {
                registry_root,
                client_id: client_id.ok_or(())?,
                public_key_file: public_key_file.ok_or(())?,
            })
        }
        "disable"
            if public_key_file.is_none()
                && expected_fingerprint.is_none()
                && authorized_keys_file.is_none()
                && socket_path.is_none() =>
        {
            Ok(ClientAdminCommand::Disable {
                registry_root,
                client_id: client_id.ok_or(())?,
            })
        }
        "enable"
            if public_key_file.is_none()
                && expected_fingerprint.is_none()
                && authorized_keys_file.is_none()
                && socket_path.is_none() =>
        {
            Ok(ClientAdminCommand::Enable {
                registry_root,
                client_id: client_id.ok_or(())?,
            })
        }
        "rotate-key" if authorized_keys_file.is_none() && socket_path.is_none() => {
            Ok(ClientAdminCommand::RotateKey {
                registry_root,
                client_id: client_id.ok_or(())?,
                expected_fingerprint: expected_fingerprint.ok_or(())?,
                public_key_file: public_key_file.ok_or(())?,
            })
        }
        "import-authorized-keys"
            if client_id.is_none()
                && public_key_file.is_none()
                && expected_fingerprint.is_none()
                && socket_path.is_none() =>
        {
            Ok(ClientAdminCommand::ImportAuthorizedKeys {
                registry_root,
                authorized_keys_file: authorized_keys_file.ok_or(())?,
            })
        }
        "serve"
            if client_id.is_none()
                && public_key_file.is_none()
                && expected_fingerprint.is_none()
                && authorized_keys_file.is_none() =>
        {
            Ok(ClientAdminCommand::Serve {
                registry_root,
                socket_path: socket_path.ok_or(())?,
            })
        }
        _ => Err(()),
    }
}

pub(crate) fn execute(
    command: ClientAdminCommand,
    mut output: impl Write,
) -> Result<(), ClientAdminError> {
    match command {
        ClientAdminCommand::List { registry_root } => {
            let registry = open_registry(registry_root)?;
            write_list(
                registry.current().map_err(ClientAdminError::Registry)?,
                &mut output,
            )
        }
        ClientAdminCommand::Add {
            registry_root,
            client_id,
            public_key_file,
        } => {
            let key = read_public_key(public_key_file)?;
            let registry = open_registry(registry_root)?;
            let outcome = registry
                .add(client_id, key, LOCAL_ADMIN_ACTOR)
                .map_err(ClientAdminError::Registry)?;
            write_mutation(&outcome, &mut output)
        }
        ClientAdminCommand::Disable {
            registry_root,
            client_id,
        } => {
            let registry = open_registry(registry_root)?;
            let outcome = registry
                .disable(&client_id, LOCAL_ADMIN_ACTOR)
                .map_err(ClientAdminError::Registry)?;
            write_mutation(&outcome, &mut output)
        }
        ClientAdminCommand::Enable {
            registry_root,
            client_id,
        } => {
            let registry = open_registry(registry_root)?;
            let outcome = registry
                .enable(&client_id, LOCAL_ADMIN_ACTOR)
                .map_err(ClientAdminError::Registry)?;
            write_mutation(&outcome, &mut output)
        }
        ClientAdminCommand::RotateKey {
            registry_root,
            client_id,
            expected_fingerprint,
            public_key_file,
        } => {
            let key = read_public_key(public_key_file)?;
            let registry = open_registry(registry_root)?;
            let outcome = registry
                .rotate_key(&client_id, &expected_fingerprint, key, LOCAL_ADMIN_ACTOR)
                .map_err(ClientAdminError::Registry)?;
            write_mutation(&outcome, &mut output)
        }
        ClientAdminCommand::ImportAuthorizedKeys {
            registry_root,
            authorized_keys_file,
        } => {
            let bytes =
                read_bounded_regular_file(authorized_keys_file, MAXIMUM_AUTHORIZED_KEYS_FILE_BYTES)
                    .map_err(ClientAdminError::Input)?;
            let input = std::str::from_utf8(&bytes).map_err(ClientAdminError::Utf8)?;
            let clients = parse_authorized_keys(input).map_err(ClientAdminError::Import)?;
            let registry = open_registry(registry_root)?;
            let outcome = registry
                .import_authorized_keys(clients, LOCAL_ADMIN_ACTOR)
                .map_err(ClientAdminError::Registry)?;
            write_mutation(&outcome, &mut output)
        }
        ClientAdminCommand::Serve {
            registry_root,
            socket_path,
        } => web::run(registry_root, socket_path).map_err(ClientAdminError::Web),
    }
}

fn open_registry(registry_root: PathBuf) -> Result<AccessRegistry, ClientAdminError> {
    AccessRegistry::open_for_effective_user(registry_root).map_err(ClientAdminError::Registry)
}

fn read_public_key(path: PathBuf) -> Result<NormalizedPublicKey, ClientAdminError> {
    let bytes = read_bounded_regular_file(path, MAXIMUM_PUBLIC_KEY_FILE_BYTES)
        .map_err(ClientAdminError::Input)?;
    let input = std::str::from_utf8(&bytes).map_err(ClientAdminError::Utf8)?;
    NormalizedPublicKey::parse(input).map_err(ClientAdminError::Key)
}

#[derive(Serialize)]
struct ListResponse<'a> {
    status: &'static str,
    generation_id: Option<&'a str>,
    clients: &'a [agent_knowledge_access::ClientRecord],
}

fn write_list(
    snapshot: Option<RegistrySnapshot>,
    output: &mut impl Write,
) -> Result<(), ClientAdminError> {
    let response = match &snapshot {
        Some(snapshot) => ListResponse {
            status: "ok",
            generation_id: Some(snapshot.generation_id()),
            clients: snapshot.clients(),
        },
        None => ListResponse {
            status: "ok",
            generation_id: None,
            clients: &[],
        },
    };
    serde_json::to_writer(&mut *output, &response).map_err(ClientAdminError::Json)?;
    writeln!(output).map_err(ClientAdminError::Io)
}

#[derive(Serialize)]
struct MutationResponse<'a> {
    status: &'static str,
    generation_id: &'a str,
}

fn write_mutation(
    outcome: &MutationOutcome,
    output: &mut impl Write,
) -> Result<(), ClientAdminError> {
    let response = MutationResponse {
        status: if outcome.changed() {
            "updated"
        } else {
            "unchanged"
        },
        generation_id: outcome.snapshot().generation_id(),
    };
    serde_json::to_writer(&mut *output, &response).map_err(ClientAdminError::Json)?;
    writeln!(output).map_err(ClientAdminError::Io)
}

/// Failure to execute a local client-registry command.
#[derive(Debug)]
pub(crate) enum ClientAdminError {
    Input(BoundedFileError),
    Utf8(std::str::Utf8Error),
    Key(KeyValidationError),
    Import(AuthorizedKeysImportError),
    Registry(AccessRegistryError),
    Json(serde_json::Error),
    Io(io::Error),
    Web(web::ClientAdminWebError),
}

impl fmt::Display for ClientAdminError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Input(error) => write!(formatter, "could not read client key input: {error}"),
            Self::Utf8(error) => write!(formatter, "client key input is not UTF-8: {error}"),
            Self::Key(error) => error.fmt(formatter),
            Self::Import(error) => error.fmt(formatter),
            Self::Registry(error) => error.fmt(formatter),
            Self::Json(error) => write!(formatter, "could not encode client output: {error}"),
            Self::Io(error) => write!(formatter, "could not write client output: {error}"),
            Self::Web(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for ClientAdminError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Input(error) => Some(error),
            Self::Utf8(error) => Some(error),
            Self::Key(error) => Some(error),
            Self::Import(error) => Some(error),
            Self::Registry(error) => Some(error),
            Self::Json(error) => Some(error),
            Self::Io(error) => Some(error),
            Self::Web(error) => Some(error),
        }
    }
}
