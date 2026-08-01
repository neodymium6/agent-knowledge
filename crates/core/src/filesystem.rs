use std::fmt;
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};

/// Captured identity and ancestry for one pinned filesystem object.
#[derive(Clone, Debug)]
pub struct PathAttestation {
    canonical_path: PathBuf,
    object: Option<FilesystemObjectId>,
    ancestors: Vec<FilesystemObjectId>,
}

impl PathAttestation {
    /// Captures a canonical live path after proving that it names `pinned`.
    ///
    /// # Errors
    ///
    /// Returns an error when the path cannot be resolved, its ancestry cannot
    /// be inspected, or its live object differs from the pinned handle.
    pub fn capture(path: &Path, pinned: &File) -> Result<Self, PathAttestationError> {
        let canonical_path = fs::canonicalize(path).map_err(PathAttestationError::Io)?;
        let live = fs::metadata(&canonical_path).map_err(PathAttestationError::Io)?;
        let pinned = pinned.metadata().map_err(PathAttestationError::Io)?;
        if !same_object(&live, &pinned) {
            return Err(PathAttestationError::BindingMismatch);
        }
        #[cfg(unix)]
        let object = Some(FilesystemObjectId::from_metadata(&pinned));
        #[cfg(not(unix))]
        let object = None;
        #[cfg(unix)]
        let ancestors = canonical_path
            .ancestors()
            .skip(1)
            .map(|ancestor| {
                fs::metadata(ancestor)
                    .map(|metadata| FilesystemObjectId::from_metadata(&metadata))
                    .map_err(PathAttestationError::Io)
            })
            .collect::<Result<Vec<_>, _>>()?;
        #[cfg(not(unix))]
        let ancestors = Vec::new();
        Ok(Self {
            canonical_path,
            object,
            ancestors,
        })
    }

    /// Returns the canonical path captured for diagnostics and lexical checks.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.canonical_path
    }

    /// Returns whether this object is equal to or nested below `other`.
    #[must_use]
    pub fn is_within(&self, other: &Self) -> bool {
        self.canonical_path.starts_with(&other.canonical_path)
            || other.object.is_some_and(|object| {
                self.object == Some(object) || self.ancestors.contains(&object)
            })
    }
}

#[cfg(unix)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FilesystemObjectId {
    device: u64,
    inode: u64,
}

#[cfg(not(unix))]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FilesystemObjectId;

#[cfg(unix)]
impl FilesystemObjectId {
    fn from_metadata(metadata: &fs::Metadata) -> Self {
        use std::os::unix::fs::MetadataExt;

        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
        }
    }
}

#[cfg(unix)]
fn same_object(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    FilesystemObjectId::from_metadata(left) == FilesystemObjectId::from_metadata(right)
}

#[cfg(not(unix))]
fn same_object(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    left.file_type() == right.file_type()
        && left.len() == right.len()
        && left.modified().ok() == right.modified().ok()
}

/// Failure while attesting a pinned filesystem object.
#[derive(Debug)]
pub enum PathAttestationError {
    /// The configured live path no longer names the pinned object.
    BindingMismatch,
    /// Filesystem inspection failed.
    Io(io::Error),
}

impl fmt::Display for PathAttestationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BindingMismatch => {
                formatter.write_str("live filesystem path differs from its pinned object")
            }
            Self::Io(error) => write!(formatter, "could not attest filesystem path: {error}"),
        }
    }
}

impl std::error::Error for PathAttestationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::BindingMismatch => None,
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::path::PathBuf;

    #[cfg(unix)]
    use super::{FilesystemObjectId, PathAttestation};

    #[cfg(unix)]
    #[test]
    fn identity_ancestry_detects_a_nonlexical_alias() {
        let parent_identity = FilesystemObjectId {
            device: 7,
            inode: 11,
        };
        let parent = PathAttestation {
            canonical_path: PathBuf::from("/fictional/first"),
            object: Some(parent_identity),
            ancestors: Vec::new(),
        };
        let child = PathAttestation {
            canonical_path: PathBuf::from("/fictional/second"),
            object: Some(FilesystemObjectId {
                device: 7,
                inode: 12,
            }),
            ancestors: vec![parent_identity],
        };

        assert!(child.is_within(&parent));
        assert!(!parent.is_within(&child));
    }
}
