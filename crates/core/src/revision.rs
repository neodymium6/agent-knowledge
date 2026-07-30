use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

const PREFIX: &str = "sha256:";
const DIGEST_BYTES: usize = 32;
const DIGEST_HEX_LENGTH: usize = DIGEST_BYTES * 2;

/// The SHA-256 revision of an exact file byte sequence.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(try_from = "String", into = "String")]
pub struct Revision([u8; DIGEST_BYTES]);

impl Revision {
    /// Creates a revision from raw SHA-256 digest bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; DIGEST_BYTES]) -> Self {
        Self(bytes)
    }

    /// Returns the raw SHA-256 digest bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; DIGEST_BYTES] {
        &self.0
    }
}

impl fmt::Display for Revision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(PREFIX)?;
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl FromStr for Revision {
    type Err = RevisionParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let Some(digest) = value.strip_prefix(PREFIX) else {
            return Err(RevisionParseError::MissingPrefix);
        };

        if digest.len() != DIGEST_HEX_LENGTH {
            return Err(RevisionParseError::InvalidLength {
                actual: digest.len(),
            });
        }

        if !digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(RevisionParseError::InvalidHex);
        }

        let mut bytes = [0_u8; DIGEST_BYTES];
        for (index, pair) in digest.as_bytes().chunks_exact(2).enumerate() {
            let Ok(pair) = str::from_utf8(pair) else {
                return Err(RevisionParseError::InvalidHex);
            };
            let Ok(byte) = u8::from_str_radix(pair, 16) else {
                return Err(RevisionParseError::InvalidHex);
            };
            bytes[index] = byte;
        }

        Ok(Self(bytes))
    }
}

impl TryFrom<String> for Revision {
    type Error = RevisionParseError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<Revision> for String {
    fn from(value: Revision) -> Self {
        value.to_string()
    }
}

/// A failure to parse a canonical SHA-256 revision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RevisionParseError {
    /// The `sha256:` prefix was missing.
    MissingPrefix,
    /// The hexadecimal digest did not contain exactly 64 characters.
    InvalidLength {
        /// The observed hexadecimal string length.
        actual: usize,
    },
    /// The digest contained a non-lowercase-hexadecimal character.
    InvalidHex,
}

impl fmt::Display for RevisionParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingPrefix => formatter.write_str("revision must start with `sha256:`"),
            Self::InvalidLength { actual } => write!(
                formatter,
                "revision digest must contain {DIGEST_HEX_LENGTH} hexadecimal characters, got {actual}"
            ),
            Self::InvalidHex => {
                formatter.write_str("revision digest must use lowercase hexadecimal characters")
            }
        }
    }
}

impl std::error::Error for RevisionParseError {}

#[cfg(test)]
mod tests {
    use super::{DIGEST_BYTES, Revision, RevisionParseError};

    const REVISION_TEXT: &str =
        "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    #[test]
    fn revision_round_trips_canonically() {
        let revision = REVISION_TEXT.parse::<Revision>();
        assert_eq!(
            revision.map(|value| value.to_string()),
            Ok(REVISION_TEXT.into())
        );
    }

    #[test]
    fn revision_rejects_noncanonical_values() {
        assert_eq!(
            "0123".parse::<Revision>(),
            Err(RevisionParseError::MissingPrefix)
        );
        assert!(matches!(
            "sha256:0123".parse::<Revision>(),
            Err(RevisionParseError::InvalidLength { actual: 4 })
        ));
        assert_eq!(
            "sha256:ABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCDEFABCD"
                .parse::<Revision>(),
            Err(RevisionParseError::InvalidHex)
        );
    }

    #[test]
    fn revision_serializes_as_a_string() {
        let revision = Revision::from_bytes([0; DIGEST_BYTES]);
        let expected = format!("\"sha256:{}\"", "00".repeat(DIGEST_BYTES));
        let serialized = match serde_json::to_string(&revision) {
            Ok(serialized) => serialized,
            Err(error) => panic!("revision must serialize: {error}"),
        };
        assert_eq!(serialized, expected);
    }
}
