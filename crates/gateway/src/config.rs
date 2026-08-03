use std::collections::BTreeMap;
use std::fmt;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use agent_knowledge_core::{BoundedFileError, read_bounded_regular_file};
use serde::Deserialize;
use serde::de::IgnoredAny;

/// Gateway configuration schema supported by this release.
pub const CURRENT_GATEWAY_CONFIG_VERSION: u16 = 4;
const MAXIMUM_GATEWAY_CONFIG_BYTES: u64 = 64 * 1024;
const MAXIMUM_SUBMIT_TIMEOUT_SECONDS: u64 = 3_600;
const MAXIMUM_READ_RESULTS: usize = 10_000;
const MAXIMUM_SEARCH_QUERY_CHARACTERS: usize = 4_096;
const MAXIMUM_INDEX_ENTRIES: usize = 1_000_000;
const MAXIMUM_INDEX_MARKDOWN_BYTES: u64 = 16 * 1024 * 1024 * 1024;
const MAXIMUM_SEARCH_DOCUMENTS: usize = 1_000_000;
const MAXIMUM_SEARCH_MARKDOWN_BYTES: u64 = 16 * 1024 * 1024 * 1024;
const MAXIMUM_READ_OPERATION_SECONDS: u64 = 300;
const MAXIMUM_RESPONSE_BYTES: u64 = 256 * 1024 * 1024;

/// Validated settings for one forced-command Gateway process.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GatewaySettings {
    gateway_uid: u32,
    queue_socket: PathBuf,
    git_directory: PathBuf,
    content_root: PathBuf,
    official_branch: String,
    maximum_read_results: usize,
    maximum_search_query_characters: usize,
    maximum_index_entries: usize,
    maximum_index_markdown_bytes: u64,
    maximum_search_documents: usize,
    maximum_search_markdown_bytes: u64,
    read_operation_timeout: Duration,
    maximum_response_bytes: u64,
    search_node: bool,
    search_agent: bool,
    search_session: bool,
    search_request_id: bool,
    submit_timeout: Duration,
}

impl GatewaySettings {
    /// Returns the dedicated Unix user ID allowed to run the Gateway.
    #[must_use]
    pub const fn gateway_uid(&self) -> u32 {
        self.gateway_uid
    }

    /// Loads one pinned, bounded, strict YAML configuration file.
    ///
    /// # Errors
    ///
    /// Returns an error for unsafe file input, malformed YAML, an unsupported
    /// schema version, or a non-absolute queue path.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, GatewayConfigError> {
        let bytes = read_bounded_regular_file(path, MAXIMUM_GATEWAY_CONFIG_BYTES)
            .map_err(GatewayConfigError::bounded_file)?;
        let yaml = std::str::from_utf8(&bytes).map_err(|_| GatewayConfigError::InvalidUtf8)?;
        Self::decode(yaml)
    }

    /// Decodes one bounded, strict YAML configuration document.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed or unsupported configuration.
    pub fn decode(yaml: &str) -> Result<Self, GatewayConfigError> {
        if yaml.len() as u64 > MAXIMUM_GATEWAY_CONFIG_BYTES {
            return Err(GatewayConfigError::FileTooLarge {
                maximum: MAXIMUM_GATEWAY_CONFIG_BYTES,
            });
        }
        let budget = yaml.len().max(1);
        let options = serde_saphyr::options! {
            budget: serde_saphyr::budget! {
                max_events: budget,
                max_aliases: 0,
                max_anchors: 0,
                max_depth: 8,
                max_documents: 2,
                max_nodes: budget,
                max_total_scalar_bytes: budget,
                max_total_comment_bytes: budget,
                max_merge_keys: 0,
            },
            merge_keys: serde_saphyr::MergeKeyPolicy::Error,
            strict_booleans: true,
            with_snippet: false,
        };
        let envelope: WireSchemaEnvelope =
            serde_saphyr::from_str_with_options(yaml, options.clone())
                .map_err(|error| GatewayConfigError::InvalidYaml(Box::new(error)))?;
        if envelope.schema_version != CURRENT_GATEWAY_CONFIG_VERSION {
            return Err(GatewayConfigError::UnsupportedSchemaVersion {
                found: envelope.schema_version,
            });
        }
        let wire: WireGatewayConfig = serde_saphyr::from_str_with_options(yaml, options)
            .map_err(|error| GatewayConfigError::InvalidYaml(Box::new(error)))?;
        Self::try_from(wire)
    }

    /// Returns the local queue-ingress Unix socket.
    #[must_use]
    pub fn queue_socket(&self) -> &Path {
        &self.queue_socket
    }

    /// Returns the bare repository containing the official branch.
    #[must_use]
    pub fn git_directory(&self) -> &Path {
        &self.git_directory
    }

    /// Returns the canonical committed content worktree.
    #[must_use]
    pub fn content_root(&self) -> &Path {
        &self.content_root
    }

    /// Returns the official branch name without the `refs/heads/` prefix.
    #[must_use]
    pub fn official_branch(&self) -> &str {
        &self.official_branch
    }

    /// Returns the maximum documents one read operation may return.
    #[must_use]
    pub const fn maximum_read_results(&self) -> usize {
        self.maximum_read_results
    }

    /// Returns the maximum Unicode scalar values in one search query.
    #[must_use]
    pub const fn maximum_search_query_characters(&self) -> usize {
        self.maximum_search_query_characters
    }

    #[must_use]
    pub const fn maximum_index_entries(&self) -> usize {
        self.maximum_index_entries
    }

    #[must_use]
    pub const fn maximum_index_markdown_bytes(&self) -> u64 {
        self.maximum_index_markdown_bytes
    }

    #[must_use]
    pub const fn maximum_search_documents(&self) -> usize {
        self.maximum_search_documents
    }

    #[must_use]
    pub const fn maximum_search_markdown_bytes(&self) -> u64 {
        self.maximum_search_markdown_bytes
    }

    #[must_use]
    pub const fn read_operation_timeout(&self) -> Duration {
        self.read_operation_timeout
    }

    #[must_use]
    pub const fn maximum_response_bytes(&self) -> u64 {
        self.maximum_response_bytes
    }

    /// Returns the allowlist for optional metadata included in search.
    #[must_use]
    pub const fn search_metadata_fields(&self) -> [bool; 4] {
        [
            self.search_node,
            self.search_agent,
            self.search_session,
            self.search_request_id,
        ]
    }

    /// Returns the absolute wall-clock deadline for one submit stream.
    #[must_use]
    pub const fn submit_timeout(&self) -> Duration {
        self.submit_timeout
    }
}

impl TryFrom<WireGatewayConfig> for GatewaySettings {
    type Error = GatewayConfigError;

    fn try_from(wire: WireGatewayConfig) -> Result<Self, Self::Error> {
        if wire.schema_version != CURRENT_GATEWAY_CONFIG_VERSION {
            return Err(GatewayConfigError::UnsupportedSchemaVersion {
                found: wire.schema_version,
            });
        }
        if wire.identity.gateway_uid == 0 {
            return Err(GatewayConfigError::InvalidGatewayUid);
        }
        let queue_socket = validate_storage_path(wire.storage.queue_socket, "queue_socket")?;
        let git_directory = validate_storage_path(wire.storage.git_directory, "git_directory")?;
        let content_root = validate_storage_path(wire.storage.content_root, "content_root")?;
        if !valid_official_branch(&wire.repository.official_branch) {
            return Err(GatewayConfigError::InvalidOfficialBranch);
        }
        if wire.reads.maximum_results == 0 || wire.reads.maximum_results > MAXIMUM_READ_RESULTS {
            return Err(GatewayConfigError::InvalidReadResultLimit);
        }
        if wire.reads.maximum_query_characters == 0
            || wire.reads.maximum_query_characters > MAXIMUM_SEARCH_QUERY_CHARACTERS
        {
            return Err(GatewayConfigError::InvalidSearchQueryLimit);
        }
        validate_read_budget(
            "maximum_index_entries",
            wire.reads.maximum_index_entries as u64,
            MAXIMUM_INDEX_ENTRIES as u64,
        )?;
        validate_read_budget(
            "maximum_index_markdown_bytes",
            wire.reads.maximum_index_markdown_bytes,
            MAXIMUM_INDEX_MARKDOWN_BYTES,
        )?;
        validate_read_budget(
            "maximum_search_documents",
            wire.reads.maximum_search_documents as u64,
            MAXIMUM_SEARCH_DOCUMENTS as u64,
        )?;
        validate_read_budget(
            "maximum_search_markdown_bytes",
            wire.reads.maximum_search_markdown_bytes,
            MAXIMUM_SEARCH_MARKDOWN_BYTES,
        )?;
        validate_read_budget(
            "operation_timeout_seconds",
            wire.reads.operation_timeout_seconds,
            MAXIMUM_READ_OPERATION_SECONDS,
        )?;
        validate_read_budget(
            "maximum_response_bytes",
            wire.reads.maximum_response_bytes,
            MAXIMUM_RESPONSE_BYTES,
        )?;
        if wire.transport.submit_timeout_seconds == 0
            || wire.transport.submit_timeout_seconds > MAXIMUM_SUBMIT_TIMEOUT_SECONDS
        {
            return Err(GatewayConfigError::InvalidSubmitTimeout);
        }
        Ok(Self {
            gateway_uid: wire.identity.gateway_uid,
            queue_socket,
            git_directory,
            content_root,
            official_branch: wire.repository.official_branch,
            maximum_read_results: wire.reads.maximum_results,
            maximum_search_query_characters: wire.reads.maximum_query_characters,
            maximum_index_entries: wire.reads.maximum_index_entries,
            maximum_index_markdown_bytes: wire.reads.maximum_index_markdown_bytes,
            maximum_search_documents: wire.reads.maximum_search_documents,
            maximum_search_markdown_bytes: wire.reads.maximum_search_markdown_bytes,
            read_operation_timeout: Duration::from_secs(wire.reads.operation_timeout_seconds),
            maximum_response_bytes: wire.reads.maximum_response_bytes,
            search_node: wire.reads.search_metadata.node,
            search_agent: wire.reads.search_metadata.agent,
            search_session: wire.reads.search_metadata.session,
            search_request_id: wire.reads.search_metadata.request_id,
            submit_timeout: Duration::from_secs(wire.transport.submit_timeout_seconds),
        })
    }
}

fn validate_read_budget(
    field: &'static str,
    value: u64,
    maximum: u64,
) -> Result<(), GatewayConfigError> {
    if value == 0 || value > maximum {
        return Err(GatewayConfigError::InvalidReadBudget { field, maximum });
    }
    Ok(())
}

fn valid_official_branch(branch: &str) -> bool {
    !branch.is_empty()
        && branch != "@"
        && !branch.starts_with(['.', '/'])
        && !branch.ends_with(['.', '/'])
        && !branch.contains("..")
        && !branch.contains("@{")
        && !branch.split('/').any(|component| {
            component.is_empty() || component.ends_with(".lock") || component.starts_with('.')
        })
        && !branch.chars().any(|character| {
            character.is_control()
                || character.is_ascii_whitespace()
                || matches!(character, '~' | '^' | ':' | '?' | '*' | '[' | '\\')
        })
}

fn validate_storage_path(
    path: PathBuf,
    field: &'static str,
) -> Result<PathBuf, GatewayConfigError> {
    if !path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
        || path.parent().is_none()
    {
        return Err(GatewayConfigError::InvalidStoragePath { field });
    }
    Ok(path)
}

#[derive(Debug, Deserialize)]
struct WireSchemaEnvelope {
    schema_version: u16,
    #[serde(flatten)]
    _remaining: BTreeMap<String, IgnoredAny>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireGatewayConfig {
    schema_version: u16,
    identity: WireIdentity,
    storage: WireStorage,
    repository: WireRepository,
    reads: WireReads,
    transport: WireTransport,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireIdentity {
    gateway_uid: u32,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireStorage {
    queue_socket: PathBuf,
    git_directory: PathBuf,
    content_root: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireRepository {
    official_branch: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireReads {
    maximum_results: usize,
    maximum_query_characters: usize,
    maximum_index_entries: usize,
    maximum_index_markdown_bytes: u64,
    maximum_search_documents: usize,
    maximum_search_markdown_bytes: u64,
    operation_timeout_seconds: u64,
    maximum_response_bytes: u64,
    search_metadata: WireSearchMetadata,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireSearchMetadata {
    node: bool,
    agent: bool,
    session: bool,
    request_id: bool,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireTransport {
    submit_timeout_seconds: u64,
}

/// Invalid Gateway configuration input.
#[derive(Debug)]
pub enum GatewayConfigError {
    /// The configuration file could not be read.
    Io(io::Error),
    /// The configured path did not name a regular file.
    InvalidFileType,
    /// The configuration exceeded the fixed parser input bound.
    FileTooLarge {
        /// Maximum accepted configuration bytes.
        maximum: u64,
    },
    /// The configuration was not UTF-8.
    InvalidUtf8,
    /// YAML decoding or strict schema validation failed.
    InvalidYaml(Box<serde_saphyr::Error>),
    /// The configuration schema version is unsupported.
    UnsupportedSchemaVersion {
        /// Version found in the file.
        found: u16,
    },
    /// The configured Gateway user ID was root.
    InvalidGatewayUid,
    /// A storage path was relative, non-normalized, or a filesystem root.
    InvalidStoragePath {
        /// Invalid configuration field.
        field: &'static str,
    },
    /// The official branch name was empty or contained control characters.
    InvalidOfficialBranch,
    /// The maximum read result count was zero or exceeded the implementation bound.
    InvalidReadResultLimit,
    /// The maximum search query length was zero or exceeded the implementation bound.
    InvalidSearchQueryLimit,
    /// A read work or response budget was zero or exceeded its hard bound.
    InvalidReadBudget {
        /// Invalid configuration field.
        field: &'static str,
        /// Implementation maximum.
        maximum: u64,
    },
    /// The submit deadline was zero or exceeded the operational bound.
    InvalidSubmitTimeout,
}

impl GatewayConfigError {
    fn bounded_file(error: BoundedFileError) -> Self {
        match error {
            BoundedFileError::Io(error) => Self::Io(error),
            BoundedFileError::InvalidFileType => Self::InvalidFileType,
            BoundedFileError::FileTooLarge { maximum } => Self::FileTooLarge { maximum },
        }
    }
}

impl fmt::Display for GatewayConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "could not read Gateway configuration: {error}"),
            Self::InvalidFileType => {
                formatter.write_str("Gateway configuration must be a regular file")
            }
            Self::FileTooLarge { maximum } => {
                write!(formatter, "Gateway configuration exceeds {maximum} bytes")
            }
            Self::InvalidUtf8 => formatter.write_str("Gateway configuration must be UTF-8"),
            Self::InvalidYaml(error) => write!(formatter, "invalid Gateway configuration: {error}"),
            Self::UnsupportedSchemaVersion { found } => write!(
                formatter,
                "unsupported Gateway configuration schema version {found}"
            ),
            Self::InvalidGatewayUid => {
                formatter.write_str("Gateway user ID must be a non-root numeric ID")
            }
            Self::InvalidStoragePath { field } => write!(
                formatter,
                "Gateway `{field}` must be an absolute normalized non-root path"
            ),
            Self::InvalidOfficialBranch => {
                formatter.write_str("Gateway official branch must be nonempty visible text")
            }
            Self::InvalidReadResultLimit => write!(
                formatter,
                "Gateway maximum read results must be between 1 and {MAXIMUM_READ_RESULTS}"
            ),
            Self::InvalidSearchQueryLimit => write!(
                formatter,
                "Gateway maximum search query length must be between 1 and {MAXIMUM_SEARCH_QUERY_CHARACTERS}"
            ),
            Self::InvalidReadBudget { field, maximum } => write!(
                formatter,
                "Gateway `{field}` must be between 1 and {maximum}"
            ),
            Self::InvalidSubmitTimeout => write!(
                formatter,
                "Gateway submit timeout must be between 1 and {MAXIMUM_SUBMIT_TIMEOUT_SECONDS} seconds"
            ),
        }
    }
}

impl std::error::Error for GatewayConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::InvalidYaml(error) => Some(error.as_ref()),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{GatewayConfigError, GatewaySettings};

    const VALID_CONFIG: &str = "schema_version: 4\nidentity:\n  gateway_uid: 61001\nstorage:\n  queue_socket: /run/agent-knowledge/queue-ingress.sock\n  git_directory: /srv/fictional-knowledge/repository\n  content_root: /srv/fictional-knowledge/content\nrepository:\n  official_branch: main\nreads:\n  maximum_results: 100\n  maximum_query_characters: 512\n  maximum_index_entries: 100000\n  maximum_index_markdown_bytes: 536870912\n  maximum_search_documents: 10000\n  maximum_search_markdown_bytes: 536870912\n  operation_timeout_seconds: 30\n  maximum_response_bytes: 268435456\n  search_metadata:\n    node: true\n    agent: true\n    session: true\n    request_id: true\ntransport:\n  submit_timeout_seconds: 300\n";

    #[test]
    fn decodes_only_the_strict_versioned_gateway_shape() {
        let settings = GatewaySettings::decode(VALID_CONFIG)
            .unwrap_or_else(|error| panic!("Gateway fixture must decode: {error}"));
        assert_eq!(
            settings.queue_socket(),
            std::path::Path::new("/run/agent-knowledge/queue-ingress.sock")
        );
        assert_eq!(settings.submit_timeout(), Duration::from_secs(300));
        assert_eq!(settings.gateway_uid(), 61_001);
        assert_eq!(settings.official_branch(), "main");
        assert_eq!(settings.maximum_read_results(), 100);
        assert_eq!(settings.maximum_search_query_characters(), 512);
        assert_eq!(settings.maximum_index_entries(), 100_000);
        assert_eq!(settings.maximum_index_markdown_bytes(), 536_870_912);
        assert_eq!(settings.maximum_search_documents(), 10_000);
        assert_eq!(settings.maximum_search_markdown_bytes(), 536_870_912);
        assert_eq!(settings.read_operation_timeout(), Duration::from_secs(30));
        assert_eq!(settings.maximum_response_bytes(), 268_435_456);
        assert_eq!(settings.search_metadata_fields(), [true; 4]);

        let version_three = VALID_CONFIG.replacen(
            "schema_version: 4\nidentity:\n  gateway_uid: 61001\n",
            "schema_version: 3\n",
            1,
        );
        assert!(matches!(
            GatewaySettings::decode(&version_three),
            Err(GatewayConfigError::UnsupportedSchemaVersion { found: 3 })
        ));
        let version_two = version_three
            .replacen("schema_version: 3", "schema_version: 2", 1)
            .replacen("queue_socket:", "queue_root:", 1);
        assert!(matches!(
            GatewaySettings::decode(&version_two),
            Err(GatewayConfigError::UnsupportedSchemaVersion { found: 2 })
        ));
        assert!(matches!(
            GatewaySettings::decode(&VALID_CONFIG.replace("identity:\n  gateway_uid: 61001\n", "")),
            Err(GatewayConfigError::InvalidYaml(_))
        ));
        assert!(
            GatewaySettings::decode(
                &VALID_CONFIG.replace("/run/agent-knowledge/queue-ingress.sock", "relative/socket")
            )
            .is_err()
        );
        assert!(
            GatewaySettings::decode(
                &VALID_CONFIG.replace("official_branch: main", "official_branch: ''")
            )
            .is_err()
        );
        for branch in ["foo..bar", "main.lock", "../main", "feature/@{upstream}"] {
            assert!(
                GatewaySettings::decode(&VALID_CONFIG.replace(
                    "official_branch: main",
                    &format!("official_branch: {branch}")
                ))
                .is_err()
            );
        }
        assert!(
            GatewaySettings::decode(
                &VALID_CONFIG.replace("maximum_results: 100", "maximum_results: 0")
            )
            .is_err()
        );
        assert!(
            GatewaySettings::decode(&VALID_CONFIG.replace(
                "maximum_query_characters: 512",
                "maximum_query_characters: 0"
            ))
            .is_err()
        );
        assert!(
            GatewaySettings::decode(&VALID_CONFIG.replace(
                "maximum_response_bytes: 268435456",
                "maximum_response_bytes: 0"
            ))
            .is_err()
        );
        assert!(GatewaySettings::decode(&format!("{VALID_CONFIG}extra: true\n")).is_err());
        assert!(
            GatewaySettings::decode(&VALID_CONFIG.replace("gateway_uid: 61001", "gateway_uid: 0"))
                .is_err()
        );
        let future_version = VALID_CONFIG
            .replacen("schema_version: 4", "schema_version: 5", 1)
            .replacen(
                "identity:\n  gateway_uid: 61001\n",
                "future_identity: true\n",
                1,
            )
            .replacen("queue_socket:", "future_queue_endpoint:", 1);
        assert!(matches!(
            GatewaySettings::decode(&future_version),
            Err(GatewayConfigError::UnsupportedSchemaVersion { found: 5 })
        ));
    }
}
