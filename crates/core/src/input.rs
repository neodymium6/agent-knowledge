use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{self, Read};
use std::path::{Path, PathBuf};

#[cfg(target_os = "linux")]
fn open_regular_file(
    path: &Path,
    follow_symbolic_links: bool,
) -> Result<PinnedRegularFile, BoundedFileError> {
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

    let no_follow = if follow_symbolic_links {
        0
    } else {
        nix::libc::O_NOFOLLOW
    };
    let anchor = OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_PATH | nix::libc::O_CLOEXEC | no_follow)
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
    Ok(PinnedRegularFile {
        file,
        length: metadata.len(),
        _anchor: anchor,
    })
}

#[cfg(not(target_os = "linux"))]
fn open_regular_file(
    path: &Path,
    follow_symbolic_links: bool,
) -> Result<PinnedRegularFile, BoundedFileError> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        let no_follow = if follow_symbolic_links {
            0
        } else {
            nix::libc::O_NOFOLLOW
        };
        options.custom_flags(nix::libc::O_CLOEXEC | nix::libc::O_NONBLOCK | no_follow);
    }
    #[cfg(not(unix))]
    if !follow_symbolic_links
        && std::fs::symlink_metadata(path)
            .map_err(BoundedFileError::Io)?
            .file_type()
            .is_symlink()
    {
        return Err(BoundedFileError::InvalidFileType);
    }
    let file = options.open(path).map_err(BoundedFileError::Io)?;
    let metadata = file.metadata().map_err(BoundedFileError::Io)?;
    if !metadata.file_type().is_file() {
        return Err(BoundedFileError::InvalidFileType);
    }
    Ok(PinnedRegularFile {
        file,
        length: metadata.len(),
    })
}

/// A regular file pinned to the object selected when it was opened.
#[derive(Debug)]
pub struct PinnedRegularFile {
    file: File,
    length: u64,
    #[cfg(target_os = "linux")]
    _anchor: File,
}

impl PinnedRegularFile {
    /// Opens a regular file without blocking on a replaced FIFO or following a
    /// replacement after the selected object has been pinned.
    ///
    /// # Errors
    ///
    /// Returns an error for I/O failures or a non-regular selected target.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, BoundedFileError> {
        open_regular_file(path.as_ref(), true)
    }

    /// Opens a regular file while rejecting a symbolic link at the selected
    /// path.
    ///
    /// # Errors
    ///
    /// Returns an error for I/O failures, symbolic links, or non-regular
    /// targets.
    pub fn open_no_follow(path: impl AsRef<Path>) -> Result<Self, BoundedFileError> {
        open_regular_file(path.as_ref(), false)
    }

    /// Returns the byte length observed from the pinned file descriptor.
    #[must_use]
    pub const fn byte_length(&self) -> u64 {
        self.length
    }
}

impl Read for PinnedRegularFile {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.file.read(buffer)
    }
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
    let mut file = PinnedRegularFile::open(path)?;
    if file.byte_length() > maximum_bytes {
        return Err(BoundedFileError::FileTooLarge {
            maximum: maximum_bytes,
        });
    }
    let capacity = usize::try_from(file.byte_length()).unwrap_or(usize::MAX);
    let maximum_capacity = usize::try_from(maximum_bytes).unwrap_or(usize::MAX);
    let mut bytes = Vec::with_capacity(capacity.min(maximum_capacity));
    file.by_ref()
        .take(maximum_bytes.saturating_add(1))
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

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Read;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::PinnedRegularFile;

    static NEXT_FILE: AtomicU64 = AtomicU64::new(0);

    fn test_path() -> PathBuf {
        let sequence = NEXT_FILE.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "agent-knowledge-pinned-file-test-{}-{sequence}",
            std::process::id()
        ))
    }

    #[test]
    fn reads_the_regular_file_selected_when_opened() {
        let path = test_path();
        fs::write(&path, b"original")
            .unwrap_or_else(|error| panic!("fixture must be written: {error}"));
        let mut pinned = PinnedRegularFile::open(&path)
            .unwrap_or_else(|error| panic!("regular file must be pinned: {error}"));
        fs::remove_file(&path)
            .unwrap_or_else(|error| panic!("fixture path must be removed: {error}"));
        fs::write(&path, b"replacement")
            .unwrap_or_else(|error| panic!("replacement fixture must be written: {error}"));

        let mut contents = String::new();
        pinned
            .read_to_string(&mut contents)
            .unwrap_or_else(|error| panic!("pinned file must be readable: {error}"));
        assert_eq!(contents, "original");
        assert_eq!(pinned.byte_length(), 8);

        fs::remove_file(path)
            .unwrap_or_else(|error| panic!("replacement fixture must be removed: {error}"));
    }

    #[cfg(unix)]
    #[test]
    fn no_follow_open_rejects_a_symbolic_link() {
        use std::os::unix::fs::symlink;

        let target = test_path();
        let link = test_path();
        fs::write(&target, b"fictional")
            .unwrap_or_else(|error| panic!("symlink target must be written: {error}"));
        symlink(&target, &link)
            .unwrap_or_else(|error| panic!("symlink fixture must be created: {error}"));
        assert!(PinnedRegularFile::open_no_follow(&link).is_err());
        fs::remove_file(link)
            .unwrap_or_else(|error| panic!("symlink fixture must be removed: {error}"));
        fs::remove_file(target)
            .unwrap_or_else(|error| panic!("symlink target must be removed: {error}"));
    }
}
