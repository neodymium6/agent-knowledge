use std::fmt;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::time::Duration;

use agent_knowledge_core::{BoundedFileError, read_bounded_regular_file};
use serde::Deserialize;

/// Gateway configuration schema supported by this release.
pub const CURRENT_GATEWAY_CONFIG_VERSION: u16 = 1;
const MAXIMUM_GATEWAY_CONFIG_BYTES: u64 = 64 * 1024;
const MAXIMUM_SUBMIT_TIMEOUT_SECONDS: u64 = 3_600;

/// Validated settings for one forced-command Gateway process.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GatewaySettings {
    queue_root: PathBuf,
    submit_timeout: Duration,
}

impl GatewaySettings {
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
        let wire: WireGatewayConfig = serde_saphyr::from_str_with_options(yaml, options)
            .map_err(|error| GatewayConfigError::InvalidYaml(Box::new(error)))?;
        Self::try_from(wire)
    }

    /// Returns the durable request queue root.
    #[must_use]
    pub fn queue_root(&self) -> &Path {
        &self.queue_root
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
        let queue_root = wire.storage.queue_root;
        if !queue_root.is_absolute()
            || queue_root
                .components()
                .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
            || queue_root.parent().is_none()
        {
            return Err(GatewayConfigError::InvalidQueuePath);
        }
        if wire.transport.submit_timeout_seconds == 0
            || wire.transport.submit_timeout_seconds > MAXIMUM_SUBMIT_TIMEOUT_SECONDS
        {
            return Err(GatewayConfigError::InvalidSubmitTimeout);
        }
        Ok(Self {
            queue_root,
            submit_timeout: Duration::from_secs(wire.transport.submit_timeout_seconds),
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireGatewayConfig {
    schema_version: u16,
    storage: WireStorage,
    transport: WireTransport,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireStorage {
    queue_root: PathBuf,
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
    /// The queue root was relative, non-normalized, or a filesystem root.
    InvalidQueuePath,
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
            Self::InvalidQueuePath => formatter
                .write_str("Gateway queue root must be an absolute normalized non-root path"),
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

    #[test]
    fn decodes_only_the_strict_versioned_gateway_shape() {
        let settings = GatewaySettings::decode(
            "schema_version: 1\nstorage:\n  queue_root: /srv/fictional-knowledge/queue\ntransport:\n  submit_timeout_seconds: 300\n",
        )
        .unwrap_or_else(|error| panic!("Gateway fixture must decode: {error}"));
        assert_eq!(
            settings.queue_root(),
            std::path::Path::new("/srv/fictional-knowledge/queue")
        );
        assert_eq!(settings.submit_timeout(), Duration::from_secs(300));

        for invalid in [
            "schema_version: 2\nstorage:\n  queue_root: /srv/fictional-knowledge/queue\ntransport:\n  submit_timeout_seconds: 300\n",
            "schema_version: 1\nstorage:\n  queue_root: relative/queue\ntransport:\n  submit_timeout_seconds: 300\n",
            "schema_version: 1\nstorage:\n  queue_root: /srv/../queue\ntransport:\n  submit_timeout_seconds: 300\n",
            "schema_version: 1\nstorage:\n  queue_root: /\ntransport:\n  submit_timeout_seconds: 300\n",
            "schema_version: 1\nstorage:\n  queue_root: /srv/queue\ntransport:\n  submit_timeout_seconds: 300\nextra: true\n",
            "schema_version: 1\nstorage: &storage\n  queue_root: /srv/queue\ntransport:\n  submit_timeout_seconds: 300\ncopy: *storage\n",
            "schema_version: 1\nstorage:\n  queue_root: /srv/queue\ntransport:\n  submit_timeout_seconds: 0\n",
            "schema_version: 1\nstorage:\n  queue_root: /srv/queue\ntransport:\n  submit_timeout_seconds: 3601\n",
        ] {
            assert!(GatewaySettings::decode(invalid).is_err());
        }
        assert!(matches!(
            GatewaySettings::decode(
                "schema_version: 2\nstorage:\n  queue_root: /srv/fictional-knowledge/queue\ntransport:\n  submit_timeout_seconds: 300\n"
            ),
            Err(GatewayConfigError::UnsupportedSchemaVersion { found: 2 })
        ));
    }
}
