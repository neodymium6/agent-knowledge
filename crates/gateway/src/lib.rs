//! Restricted forced-command Gateway for authenticated coding agents.

mod config;
mod submit;

use std::fmt;
use std::io::Read;

use agent_knowledge_core::ErrorCode;
use agent_knowledge_protocol::{ClientId, SubmitResponse};
use agent_knowledge_queue::{FileQueue, PackagePolicy, QueueError};

pub use config::{CURRENT_GATEWAY_CONFIG_VERSION, GatewayConfigError, GatewaySettings};
pub use submit::ArchiveError;

/// Opened Gateway dependencies for one forced-command process.
#[derive(Debug)]
pub struct Gateway {
    queue: FileQueue,
}

impl Gateway {
    /// Opens the durable queue using validated deployment settings.
    ///
    /// # Errors
    ///
    /// Returns an error when the queue cannot be initialized or pinned.
    pub fn open(settings: &GatewaySettings) -> Result<Self, GatewayError> {
        let queue = FileQueue::initialize(settings.queue_root(), PackagePolicy::default())
            .map_err(|error| GatewayError::Queue(Box::new(error)))?;
        Ok(Self { queue })
    }

    /// Streams and durably accepts one authenticated tar submission.
    ///
    /// # Errors
    ///
    /// Returns an error for malformed archives, invalid packages, limits, or
    /// durable queue failures. No unchecked archive entry is extracted.
    pub fn submit(
        &self,
        client_id: ClientId,
        input: impl Read,
    ) -> Result<SubmitResponse, GatewayError> {
        submit::submit(&self.queue, client_id, input)
    }
}

/// Failure while opening or serving one Gateway operation.
#[derive(Debug)]
pub enum GatewayError {
    /// Durable queue initialization or acceptance failed.
    Queue(Box<QueueError>),
    /// The untrusted tar stream violated the wire protocol.
    Archive(ArchiveError),
}

impl GatewayError {
    /// Returns the stable protocol classification for this failure.
    #[must_use]
    pub fn error_code(&self) -> ErrorCode {
        match self {
            Self::Queue(error) => error.error_code(),
            Self::Archive(error) => error.error_code(),
        }
    }
}

impl fmt::Display for GatewayError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Queue(error) => write!(formatter, "Gateway queue operation failed: {error}"),
            Self::Archive(error) => write!(formatter, "invalid submit archive: {error}"),
        }
    }
}

impl std::error::Error for GatewayError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Queue(error) => Some(error.as_ref()),
            Self::Archive(error) => Some(error),
        }
    }
}

#[cfg(test)]
mod tests;
