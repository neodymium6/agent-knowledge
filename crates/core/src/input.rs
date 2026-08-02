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
        _anchor: Some(anchor),
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
    _anchor: Option<File>,
}

/// A directory descriptor used as a non-escaping path-resolution root.
#[derive(Debug)]
pub struct PinnedDirectory {
    #[cfg(target_os = "linux")]
    file: File,
}

impl PinnedDirectory {
    /// Clones an already-open directory descriptor into a path-resolution root.
    ///
    /// # Errors
    ///
    /// Returns an error when the descriptor cannot be cloned or does not name
    /// a directory. This capability is available only on Linux.
    pub fn try_clone_from(file: &File) -> Result<Self, PinnedPathError> {
        #[cfg(target_os = "linux")]
        {
            let file = file.try_clone().map_err(PinnedPathError::Io)?;
            if !file.metadata().map_err(PinnedPathError::Io)?.is_dir() {
                return Err(PinnedPathError::ExpectedDirectory);
            }
            Ok(Self { file })
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = file;
            Err(PinnedPathError::UnsupportedPlatform)
        }
    }

    /// Clones the pinned directory descriptor for identity attestation.
    ///
    /// # Errors
    ///
    /// Returns an error when the descriptor cannot be cloned. This capability
    /// is available only on Linux.
    pub fn try_clone_file(&self) -> Result<File, PinnedPathError> {
        #[cfg(target_os = "linux")]
        {
            self.file.try_clone().map_err(PinnedPathError::Io)
        }
        #[cfg(not(target_os = "linux"))]
        {
            Err(PinnedPathError::UnsupportedPlatform)
        }
    }

    /// Opens a real directory without following a final symbolic link.
    ///
    /// # Errors
    ///
    /// Returns an error for I/O failures or a non-directory target. This
    /// capability is available only on the initial Linux deployment target.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, PinnedPathError> {
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::OpenOptionsExt;

            let file = OpenOptions::new()
                .read(true)
                .custom_flags(
                    nix::libc::O_PATH
                        | nix::libc::O_DIRECTORY
                        | nix::libc::O_NOFOLLOW
                        | nix::libc::O_CLOEXEC,
                )
                .open(path)
                .map_err(|error| {
                    if error.raw_os_error() == Some(nix::libc::ENOTDIR) {
                        PinnedPathError::ExpectedDirectory
                    } else {
                        PinnedPathError::Io(error)
                    }
                })?;
            if !file.metadata().map_err(PinnedPathError::Io)?.is_dir() {
                return Err(PinnedPathError::ExpectedDirectory);
            }
            Ok(Self { file })
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = path;
            Err(PinnedPathError::UnsupportedPlatform)
        }
    }

    /// Opens a regular file strictly beneath this directory without resolving
    /// any symbolic-link component.
    ///
    /// # Errors
    ///
    /// Returns an error when the path escapes, contains a symbolic link, does
    /// not name a regular file, or cannot be opened.
    pub fn open_regular_beneath(
        &self,
        path: impl AsRef<Path>,
    ) -> Result<PinnedRegularFile, PinnedPathError> {
        #[cfg(target_os = "linux")]
        {
            use nix::errno::Errno;
            use nix::fcntl::{OFlag, OpenHow, ResolveFlag, openat2};

            let path = path.as_ref();
            validate_beneath_path(path)?;
            let how = OpenHow::new()
                .flags(OFlag::O_RDONLY | OFlag::O_CLOEXEC | OFlag::O_NONBLOCK)
                .resolve(ResolveFlag::RESOLVE_BENEATH | ResolveFlag::RESOLVE_NO_SYMLINKS);
            match openat2(&self.file, path, how) {
                Ok(file) => pinned_regular_file(File::from(file)),
                Err(Errno::ENOSYS | Errno::EPERM) => self.open_regular_beneath_with_openat(path),
                Err(error) => Err(PinnedPathError::Io(error.into())),
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = path;
            Err(PinnedPathError::UnsupportedPlatform)
        }
    }

    #[cfg(target_os = "linux")]
    fn open_regular_beneath_with_openat(
        &self,
        path: &Path,
    ) -> Result<PinnedRegularFile, PinnedPathError> {
        use nix::fcntl::{OFlag, openat};
        use nix::sys::stat::Mode;

        validate_beneath_path(path)?;
        let mut directory =
            nix::unistd::dup(&self.file).map_err(|error| PinnedPathError::Io(error.into()))?;
        let mut components = path.components().peekable();
        while let Some(component) = components.next() {
            let std::path::Component::Normal(name) = component else {
                return Err(PinnedPathError::InvalidRelativePath);
            };
            if components.peek().is_some() {
                directory = openat(
                    &directory,
                    name,
                    OFlag::O_PATH | OFlag::O_DIRECTORY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC,
                    Mode::empty(),
                )
                .map_err(|error| PinnedPathError::Io(error.into()))?;
            } else {
                let file = openat(
                    &directory,
                    name,
                    OFlag::O_RDONLY | OFlag::O_NOFOLLOW | OFlag::O_CLOEXEC | OFlag::O_NONBLOCK,
                    Mode::empty(),
                )
                .map_err(|error| PinnedPathError::Io(error.into()))?;
                return pinned_regular_file(File::from(file));
            }
        }
        Err(PinnedPathError::InvalidRelativePath)
    }
}

#[cfg(target_os = "linux")]
fn validate_beneath_path(path: &Path) -> Result<(), PinnedPathError> {
    if path.is_absolute() {
        return Err(PinnedPathError::InvalidRelativePath);
    }
    let mut normalized = PathBuf::new();
    for component in path.components() {
        let std::path::Component::Normal(name) = component else {
            return Err(PinnedPathError::InvalidRelativePath);
        };
        normalized.push(name);
    }
    if normalized.as_os_str().is_empty() || normalized.as_os_str() != path.as_os_str() {
        return Err(PinnedPathError::InvalidRelativePath);
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn pinned_regular_file(file: File) -> Result<PinnedRegularFile, PinnedPathError> {
    let metadata = file.metadata().map_err(PinnedPathError::Io)?;
    if !metadata.is_file() {
        return Err(PinnedPathError::ExpectedRegularFile);
    }
    Ok(PinnedRegularFile {
        file,
        length: metadata.len(),
        _anchor: None,
    })
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

    /// Returns current metadata for the pinned file descriptor.
    ///
    /// This permits callers with stricter content policies to revalidate link
    /// count and mode without resolving the pathname again.
    ///
    /// # Errors
    ///
    /// Returns an error when descriptor metadata cannot be read.
    pub fn metadata(&self) -> io::Result<std::fs::Metadata> {
        self.file.metadata()
    }

    /// Clones the pinned readable descriptor without resolving its path again.
    ///
    /// # Errors
    ///
    /// Returns an error when the descriptor cannot be cloned.
    pub fn try_clone_file(&self) -> io::Result<File> {
        self.file.try_clone()
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

/// Failure while pinning a directory or resolving a file beneath it.
#[derive(Debug)]
pub enum PinnedPathError {
    /// A directory or descendant could not be opened or inspected.
    Io(io::Error),
    /// The root target was not a directory.
    ExpectedDirectory,
    /// The descendant target was not a regular file.
    ExpectedRegularFile,
    /// The descendant path was not a normalized, nonempty relative path.
    InvalidRelativePath,
    /// Descriptor-relative safe path resolution is unavailable on this target.
    UnsupportedPlatform,
}

impl fmt::Display for PinnedPathError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "could not resolve pinned path: {error}"),
            Self::ExpectedDirectory => formatter.write_str("input must be a directory"),
            Self::ExpectedRegularFile => formatter.write_str("input must be a regular file"),
            Self::InvalidRelativePath => {
                formatter.write_str("path must be a normalized, nonempty relative path")
            }
            Self::UnsupportedPlatform => {
                formatter.write_str("pinned directory resolution is unsupported on this platform")
            }
        }
    }
}

impl std::error::Error for PinnedPathError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::ExpectedDirectory
            | Self::ExpectedRegularFile
            | Self::InvalidRelativePath
            | Self::UnsupportedPlatform => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Read;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::{PinnedDirectory, PinnedRegularFile};

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

    #[cfg(target_os = "linux")]
    #[test]
    fn beneath_open_rejects_a_symbolic_link_in_a_parent_component() {
        use std::os::unix::fs::symlink;

        let root = test_path();
        let outside = test_path();
        fs::create_dir(&root)
            .unwrap_or_else(|error| panic!("pinned root fixture must be created: {error}"));
        fs::create_dir(&outside)
            .unwrap_or_else(|error| panic!("outside fixture must be created: {error}"));
        fs::write(outside.join("document.md"), b"fictional")
            .unwrap_or_else(|error| panic!("outside file must be written: {error}"));
        symlink(&outside, root.join("linked"))
            .unwrap_or_else(|error| panic!("parent symlink must be created: {error}"));
        let pinned = PinnedDirectory::open(&root)
            .unwrap_or_else(|error| panic!("root must be pinned: {error}"));
        assert!(pinned.open_regular_beneath("linked/document.md").is_err());
        fs::remove_dir_all(root)
            .unwrap_or_else(|error| panic!("pinned root fixture must be removed: {error}"));
        fs::remove_dir_all(outside)
            .unwrap_or_else(|error| panic!("outside fixture must be removed: {error}"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn openat_fallback_is_contained_and_reports_directory_types() {
        use std::os::unix::fs::symlink;

        let root = test_path();
        let outside = test_path();
        let regular = test_path();
        fs::create_dir_all(root.join("real"))
            .unwrap_or_else(|error| panic!("fallback root fixture must be created: {error}"));
        fs::create_dir(&outside)
            .unwrap_or_else(|error| panic!("outside fixture must be created: {error}"));
        fs::write(root.join("real/document.md"), b"fictional")
            .unwrap_or_else(|error| panic!("fallback file must be written: {error}"));
        fs::write(outside.join("document.md"), b"fictional")
            .unwrap_or_else(|error| panic!("outside file must be written: {error}"));
        fs::write(&regular, b"not a directory")
            .unwrap_or_else(|error| panic!("regular fixture must be written: {error}"));
        symlink(&outside, root.join("linked"))
            .unwrap_or_else(|error| panic!("parent symlink must be created: {error}"));
        let pinned = PinnedDirectory::open(&root)
            .unwrap_or_else(|error| panic!("root must be pinned: {error}"));

        assert!(
            pinned
                .open_regular_beneath_with_openat(Path::new("real/document.md"))
                .is_ok()
        );
        assert!(
            pinned
                .open_regular_beneath_with_openat(Path::new("linked/document.md"))
                .is_err()
        );
        assert!(matches!(
            PinnedDirectory::open(&regular),
            Err(super::PinnedPathError::ExpectedDirectory)
        ));

        fs::remove_dir_all(root)
            .unwrap_or_else(|error| panic!("fallback root fixture must be removed: {error}"));
        fs::remove_dir_all(outside)
            .unwrap_or_else(|error| panic!("outside fixture must be removed: {error}"));
        fs::remove_file(regular)
            .unwrap_or_else(|error| panic!("regular fixture must be removed: {error}"));
    }
}
