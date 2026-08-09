use std::fmt;

use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD};
use sha2::{Digest, Sha256};

const MAXIMUM_PUBLIC_KEY_BYTES: usize = 16 * 1024;
const ED25519_ALGORITHM: &str = "ssh-ed25519";
const ED25519_KEY_BYTES: usize = 32;

/// One validated Ed25519 public key in canonical OpenSSH form.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NormalizedPublicKey {
    openssh: String,
    fingerprint: String,
}

impl NormalizedPublicKey {
    /// Parses and canonicalizes an OpenSSH public key.
    ///
    /// Comments are deliberately discarded because registry identity is the
    /// client ID and fingerprint, not an unvalidated key comment.
    ///
    /// # Errors
    ///
    /// Returns an error for oversized, multiline, malformed, or non-Ed25519
    /// input.
    pub fn parse(input: &str) -> Result<Self, KeyValidationError> {
        if input.len() > MAXIMUM_PUBLIC_KEY_BYTES {
            return Err(KeyValidationError::TooLarge);
        }
        let input = input.trim();
        if input.is_empty() || input.contains(['\n', '\r']) {
            return Err(KeyValidationError::InvalidFormat);
        }
        let mut fields = input.split_ascii_whitespace();
        let algorithm = fields.next().ok_or(KeyValidationError::InvalidFormat)?;
        let encoded = fields.next().ok_or(KeyValidationError::InvalidFormat)?;
        if algorithm != ED25519_ALGORITHM {
            return Err(KeyValidationError::UnsupportedAlgorithm(
                algorithm.to_owned(),
            ));
        }
        let wire = STANDARD
            .decode(encoded)
            .map_err(|_| KeyValidationError::InvalidEncoding)?;
        validate_ed25519_wire(&wire)?;
        let encoded = STANDARD.encode(&wire);
        let openssh = format!("{ED25519_ALGORITHM} {encoded}");
        let fingerprint = format!("SHA256:{}", STANDARD_NO_PAD.encode(Sha256::digest(&wire)));
        Ok(Self {
            openssh,
            fingerprint,
        })
    }

    /// Returns the canonical OpenSSH public-key line without a comment.
    #[must_use]
    pub fn openssh(&self) -> &str {
        &self.openssh
    }

    /// Returns the OpenSSH SHA-256 fingerprint.
    #[must_use]
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }
}

fn validate_ed25519_wire(wire: &[u8]) -> Result<(), KeyValidationError> {
    let (algorithm, remainder) = read_ssh_string(wire)?;
    let (public_key, remainder) = read_ssh_string(remainder)?;
    if algorithm != ED25519_ALGORITHM.as_bytes()
        || public_key.len() != ED25519_KEY_BYTES
        || !remainder.is_empty()
    {
        return Err(KeyValidationError::InvalidEncoding);
    }
    Ok(())
}

fn read_ssh_string(input: &[u8]) -> Result<(&[u8], &[u8]), KeyValidationError> {
    let length_bytes: [u8; 4] = input
        .get(..4)
        .and_then(|bytes| bytes.try_into().ok())
        .ok_or(KeyValidationError::InvalidEncoding)?;
    let length = usize::try_from(u32::from_be_bytes(length_bytes))
        .map_err(|_| KeyValidationError::InvalidEncoding)?;
    let end = 4usize
        .checked_add(length)
        .ok_or(KeyValidationError::InvalidEncoding)?;
    let value = input
        .get(4..end)
        .ok_or(KeyValidationError::InvalidEncoding)?;
    let remainder = input
        .get(end..)
        .ok_or(KeyValidationError::InvalidEncoding)?;
    Ok((value, remainder))
}

/// Failure to accept a client public key.
#[derive(Debug)]
pub enum KeyValidationError {
    /// The input exceeded the fixed public-key bound.
    TooLarge,
    /// The input was empty or contained multiple lines.
    InvalidFormat,
    /// The Base64 payload or SSH wire structure was invalid.
    InvalidEncoding,
    /// The key algorithm is not allowed by the initial registry policy.
    UnsupportedAlgorithm(String),
}

impl fmt::Display for KeyValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooLarge => formatter.write_str("SSH public key exceeds the size limit"),
            Self::InvalidFormat => {
                formatter.write_str("SSH public key must be one non-empty OpenSSH line")
            }
            Self::InvalidEncoding => formatter.write_str("invalid Ed25519 OpenSSH public key"),
            Self::UnsupportedAlgorithm(algorithm) => {
                write!(
                    formatter,
                    "unsupported SSH public-key algorithm: {algorithm}"
                )
            }
        }
    }
}

impl std::error::Error for KeyValidationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::{KeyValidationError, NormalizedPublicKey};

    const ED25519_KEY: &str = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAILM+rvN+ot98qgEN796jTiQfZfG1KaT0PtFDJ/XFSqti fictional-node@example.invalid";

    #[test]
    fn canonicalizes_ed25519_keys_and_discards_comments() {
        let key = NormalizedPublicKey::parse(ED25519_KEY)
            .unwrap_or_else(|error| panic!("fixture key must be accepted: {error}"));
        assert_eq!(
            key.openssh(),
            "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAILM+rvN+ot98qgEN796jTiQfZfG1KaT0PtFDJ/XFSqti"
        );
        assert_eq!(
            key.fingerprint(),
            "SHA256:UCUiLr7Pjs9wFFJMDByLgc3NrtdU344OgUM45wZPcIQ"
        );
    }

    #[test]
    fn rejects_multiline_and_non_ed25519_input() {
        assert!(matches!(
            NormalizedPublicKey::parse(&format!("{ED25519_KEY}\n{ED25519_KEY}")),
            Err(KeyValidationError::InvalidFormat)
        ));
        assert!(NormalizedPublicKey::parse("ssh-rsa invalid").is_err());
    }
}
