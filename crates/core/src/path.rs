use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

const MAX_PATH_LENGTH: usize = 4_096;
const MAX_COMPONENT_LENGTH: usize = 255;
const MAX_PROJECT_LENGTH: usize = 63;

/// A configured project identifier.
#[derive(Clone, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(try_from = "String", into = "String")]
pub struct ProjectId(String);

impl ProjectId {
    /// Returns the identifier as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ProjectId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for ProjectId {
    type Err = PathValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        validate_project_id(value)?;
        Ok(Self(value.into()))
    }
}

impl TryFrom<String> for ProjectId {
    type Error = PathValidationError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate_project_id(&value)?;
        Ok(Self(value))
    }
}

impl From<ProjectId> for String {
    fn from(value: ProjectId) -> Self {
        value.0
    }
}

/// A normalized relative path inside a request's `payload/` directory.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(try_from = "String", into = "String")]
pub struct PayloadPath(String);

impl PayloadPath {
    /// Returns the normalized slash-separated path.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for PayloadPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for PayloadPath {
    type Err = PathValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        validate_relative_path(value)?;
        Ok(Self(value.into()))
    }
}

impl TryFrom<String> for PayloadPath {
    type Error = PathValidationError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        validate_relative_path(&value)?;
        Ok(Self(value))
    }
}

impl From<PayloadPath> for String {
    fn from(value: PayloadPath) -> Self {
        value.0
    }
}

/// A single safe attachment file name.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(try_from = "String", into = "String")]
pub struct AttachmentName(String);

impl AttachmentName {
    /// Returns the file name as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AttachmentName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl FromStr for AttachmentName {
    type Err = PathValidationError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.contains(['/', '\\']) {
            return Err(PathValidationError::Separator);
        }
        validate_component(value)?;
        if value.starts_with('.') {
            return Err(PathValidationError::HiddenComponent);
        }
        Ok(Self(value.into()))
    }
}

impl TryFrom<String> for AttachmentName {
    type Error = PathValidationError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<AttachmentName> for String {
    fn from(value: AttachmentName) -> Self {
        value.0
    }
}

fn validate_project_id(value: &str) -> Result<(), PathValidationError> {
    if value.is_empty() {
        return Err(PathValidationError::Empty);
    }
    if value.len() > MAX_PROJECT_LENGTH {
        return Err(PathValidationError::TooLong {
            maximum: MAX_PROJECT_LENGTH,
            actual: value.len(),
        });
    }
    if value.starts_with('-') || value.ends_with('-') {
        return Err(PathValidationError::InvalidProjectId);
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        return Err(PathValidationError::InvalidProjectId);
    }
    Ok(())
}

fn validate_relative_path(value: &str) -> Result<(), PathValidationError> {
    if value.is_empty() {
        return Err(PathValidationError::Empty);
    }
    if value.len() > MAX_PATH_LENGTH {
        return Err(PathValidationError::TooLong {
            maximum: MAX_PATH_LENGTH,
            actual: value.len(),
        });
    }
    if value.starts_with('/') {
        return Err(PathValidationError::Absolute);
    }
    if value.contains('\\') {
        return Err(PathValidationError::Backslash);
    }

    for component in value.split('/') {
        validate_component(component)?;
    }
    Ok(())
}

fn validate_component(value: &str) -> Result<(), PathValidationError> {
    if value.is_empty() {
        return Err(PathValidationError::EmptyComponent);
    }
    if value == "." || value == ".." {
        return Err(PathValidationError::Traversal);
    }
    if value.len() > MAX_COMPONENT_LENGTH {
        return Err(PathValidationError::TooLong {
            maximum: MAX_COMPONENT_LENGTH,
            actual: value.len(),
        });
    }
    if value.chars().any(char::is_control) {
        return Err(PathValidationError::ControlCharacter);
    }
    Ok(())
}

/// A failure to validate a project identifier, payload path, or attachment name.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PathValidationError {
    /// The complete value was empty.
    Empty,
    /// A path component was empty.
    EmptyComponent,
    /// An absolute path was provided.
    Absolute,
    /// A `.` or `..` traversal component was present.
    Traversal,
    /// A backslash made the path platform-dependent.
    Backslash,
    /// A file name contained a path separator.
    Separator,
    /// A control character was present.
    ControlCharacter,
    /// A hidden attachment name was provided.
    HiddenComponent,
    /// A project identifier was not a lowercase ASCII slug.
    InvalidProjectId,
    /// A path or component exceeded its hard protocol limit.
    TooLong {
        /// The applicable maximum byte length.
        maximum: usize,
        /// The observed byte length.
        actual: usize,
    },
}

impl fmt::Display for PathValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("value must not be empty"),
            Self::EmptyComponent => formatter.write_str("path components must not be empty"),
            Self::Absolute => formatter.write_str("path must be relative"),
            Self::Traversal => formatter.write_str("path must not contain `.` or `..` components"),
            Self::Backslash => formatter.write_str("path must use `/` separators"),
            Self::Separator => formatter.write_str("file name must not contain path separators"),
            Self::ControlCharacter => {
                formatter.write_str("value must not contain control characters")
            }
            Self::HiddenComponent => formatter.write_str("attachment name must not be hidden"),
            Self::InvalidProjectId => {
                formatter.write_str("project ID must be a lowercase ASCII slug")
            }
            Self::TooLong { maximum, actual } => {
                write!(formatter, "value is {actual} bytes; maximum is {maximum}")
            }
        }
    }
}

impl std::error::Error for PathValidationError {}

#[cfg(test)]
mod tests {
    use super::{AttachmentName, PathValidationError, PayloadPath, ProjectId};

    #[test]
    fn project_ids_accept_conservative_slugs() {
        let project = "cuda-solver".parse::<ProjectId>();
        assert_eq!(
            project.map(|value| value.to_string()),
            Ok("cuda-solver".into())
        );
    }

    #[test]
    fn project_ids_reject_unsafe_values() {
        assert_eq!(
            "CUDA".parse::<ProjectId>(),
            Err(PathValidationError::InvalidProjectId)
        );
        assert_eq!(
            "-cuda".parse::<ProjectId>(),
            Err(PathValidationError::InvalidProjectId)
        );
    }

    #[test]
    fn payload_paths_accept_normalized_relative_paths() {
        let path = "experiment/results.csv".parse::<PayloadPath>();
        assert_eq!(
            path.map(|value| value.to_string()),
            Ok("experiment/results.csv".into())
        );
    }

    #[test]
    fn payload_paths_reject_escaping_and_platform_dependent_paths() {
        assert_eq!(
            "../secret".parse::<PayloadPath>(),
            Err(PathValidationError::Traversal)
        );
        assert_eq!(
            "/absolute".parse::<PayloadPath>(),
            Err(PathValidationError::Absolute)
        );
        assert_eq!(
            "directory\\file".parse::<PayloadPath>(),
            Err(PathValidationError::Backslash)
        );
        assert_eq!(
            "directory//file".parse::<PayloadPath>(),
            Err(PathValidationError::EmptyComponent)
        );
    }

    #[test]
    fn attachment_names_are_single_visible_components() {
        assert!("results.csv".parse::<AttachmentName>().is_ok());
        assert_eq!(
            ".hidden".parse::<AttachmentName>(),
            Err(PathValidationError::HiddenComponent)
        );
        assert_eq!(
            "directory/file".parse::<AttachmentName>(),
            Err(PathValidationError::Separator)
        );
    }
}
