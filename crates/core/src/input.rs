use std::fmt;
use std::fs::{File, Metadata, OpenOptions};
use std::io::{self, Read};
use std::path::{Path, PathBuf};

#[cfg(target_os = "linux")]
fn open_regular_file(path: &Path) -> Result<(File, Metadata, File), BoundedFileError> {
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

    let anchor = OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_PATH | nix::libc::O_CLOEXEC)
        .open(path)
        .map_err(BoundedFileError::Io)?;
    let metadata = anchor.metadata().map_err(BoundedFileError::Io)?;
    if !metadata.file_type().is_file() {
        return Err(BoundedFileError::InvalidFileType);
    }
    let stable_path = PathBuf::from(format!("/proc/self/fd/{}", anchor.as_raw_fd()));
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_CLOEXEC)
        .open(stable_path)
        .map_err(BoundedFileError::Io)?;
    let read_metadata = file.metadata().map_err(BoundedFileError::Io)?;
    if metadata.dev() != read_metadata.dev() || metadata.ino() != read_metadata.ino() {
        return Err(BoundedFileError::InvalidFileType);
    }
    Ok((file, metadata, anchor))
}

#[cfg(not(target_os = "linux"))]
fn open_regular_file(path: &Path) -> Result<(File, Metadata), BoundedFileError> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.custom_flags(nix::libc::O_CLOEXEC | nix::libc::O_NONBLOCK);
    }
    let file = options.open(path).map_err(BoundedFileError::Io)?;
    let metadata = file.metadata().map_err(BoundedFileError::Io)?;
    Ok((file, metadata))
}

/// Reads one regular file while enforcing a byte bound before deserialization.
///
/// Symbolic links to regular files are supported for projected configuration.
/// On Linux, the selected target remains pinned while it is validated and read.
///
/// # Errors
///
/// Returns an error for I/O failures, non-regular targets, or oversized input.
pub fn read_bounded_regular_file(
    path: impl AsRef<Path>,
    maximum_bytes: u64,
) -> Result<Vec<u8>, BoundedFileError> {
    let path = path.as_ref();
    #[cfg(target_os = "linux")]
    let (file, metadata, _anchor) = open_regular_file(path)?;
    #[cfg(not(target_os = "linux"))]
    let (file, metadata) = open_regular_file(path)?;
    if !metadata.file_type().is_file() {
        return Err(BoundedFileError::InvalidFileType);
    }
    if metadata.len() > maximum_bytes {
        return Err(BoundedFileError::FileTooLarge {
            maximum: maximum_bytes,
        });
    }
    let capacity = usize::try_from(metadata.len()).unwrap_or(usize::MAX);
    let maximum_capacity = usize::try_from(maximum_bytes).unwrap_or(usize::MAX);
    let mut bytes = Vec::with_capacity(capacity.min(maximum_capacity));
    file.take(maximum_bytes.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(BoundedFileError::Io)?;
    if bytes.len() as u64 > maximum_bytes {
        return Err(BoundedFileError::FileTooLarge {
            maximum: maximum_bytes,
        });
    }
    Ok(bytes)
}

/// Failure while reading one bounded regular input file.
#[derive(Debug)]
pub enum BoundedFileError {
    /// The configured file could not be opened or read.
    Io(io::Error),
    /// The selected target was not a regular file.
    InvalidFileType,
    /// The input exceeded its fixed parser bound.
    FileTooLarge {
        /// Maximum accepted bytes.
        maximum: u64,
    },
}

impl fmt::Display for BoundedFileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "could not read bounded file: {error}"),
            Self::InvalidFileType => formatter.write_str("input must be a regular file"),
            Self::FileTooLarge { maximum } => {
                write!(formatter, "input exceeds {maximum} bytes")
            }
        }
    }
}

impl std::error::Error for BoundedFileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::InvalidFileType | Self::FileTooLarge { .. } => None,
        }
    }
}
