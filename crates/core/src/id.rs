use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use ulid::{DecodeError, Ulid};

macro_rules! define_id {
    ($name:ident, $description:literal) => {
        #[doc = $description]
        #[derive(
            Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
        )]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(Ulid);

        impl $name {
            /// Generates a new identifier.
            #[must_use]
            pub fn generate() -> Self {
                Self(Ulid::generate())
            }

            /// Returns the underlying ULID.
            #[must_use]
            pub const fn as_ulid(&self) -> &Ulid {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }

        impl FromStr for $name {
            type Err = DecodeError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                let parsed = value.parse::<Ulid>()?;
                if parsed.to_string() != value {
                    return Err(DecodeError::InvalidChar);
                }
                Ok(Self(parsed))
            }
        }

        impl TryFrom<String> for $name {
            type Error = DecodeError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                value.parse()
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.to_string()
            }
        }

        impl From<Ulid> for $name {
            fn from(value: Ulid) -> Self {
                Self(value)
            }
        }
    };
}

define_id!(RequestId, "A permanent identifier for one change request.");
define_id!(
    DocumentId,
    "A permanent identifier for one Markdown document."
);
define_id!(
    SessionId,
    "An identifier for one coding-agent work session."
);

#[cfg(test)]
mod tests {
    use super::{DocumentId, RequestId, SessionId};

    const ULID_TEXT: &str = "01K00000000000000000000000";

    #[test]
    fn typed_ids_parse_and_display_canonically() {
        let request_id = ULID_TEXT.parse::<RequestId>();
        let document_id = ULID_TEXT.parse::<DocumentId>();
        let session_id = ULID_TEXT.parse::<SessionId>();

        assert_eq!(request_id.map(|id| id.to_string()), Ok(ULID_TEXT.into()));
        assert_eq!(document_id.map(|id| id.to_string()), Ok(ULID_TEXT.into()));
        assert_eq!(session_id.map(|id| id.to_string()), Ok(ULID_TEXT.into()));
    }

    #[test]
    fn typed_ids_reject_invalid_text() {
        assert!("not-a-ulid".parse::<RequestId>().is_err());
        assert!("not-a-ulid".parse::<DocumentId>().is_err());
        assert!("not-a-ulid".parse::<SessionId>().is_err());
        assert!("01k00000000000000000000000".parse::<RequestId>().is_err());
        assert!("81K00000000000000000000000".parse::<RequestId>().is_err());
    }

    #[test]
    fn typed_ids_serialize_as_strings() {
        let Ok(request_id) = ULID_TEXT.parse::<RequestId>() else {
            panic!("fixture ULID must be valid");
        };

        let serialized = match serde_json::to_string(&request_id) {
            Ok(serialized) => serialized,
            Err(error) => panic!("request ID must serialize: {error}"),
        };
        assert_eq!(serialized, format!("\"{ULID_TEXT}\""));
        assert!(serde_json::from_str::<RequestId>("\"01k00000000000000000000000\"").is_err());
    }
}
