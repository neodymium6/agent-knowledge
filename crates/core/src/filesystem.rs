use std::fmt;
use std::fs::File;
#[cfg(target_os = "linux")]
use std::fs::OpenOptions;
use std::io;
use std::path::{Path, PathBuf};
#[cfg(target_os = "linux")]
use std::sync::Arc;

#[cfg(target_os = "linux")]
use std::ffi::OsString;
#[cfg(target_os = "linux")]
use std::fs;
#[cfg(target_os = "linux")]
use std::io::Read;
#[cfg(target_os = "linux")]
use std::os::unix::ffi::OsStringExt;
#[cfg(target_os = "linux")]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;

#[cfg(target_os = "linux")]
const MAXIMUM_MOUNT_INFO_BYTES: u64 = 1024 * 1024;
#[cfg(target_os = "linux")]
const MAXIMUM_FD_INFO_BYTES: u64 = 16 * 1024;

/// Captured identity and backing location for one pinned filesystem object.
#[derive(Clone, Debug)]
pub struct PathAttestation {
    canonical_path: PathBuf,
    #[cfg(target_os = "linux")]
    stable_path: PathBuf,
    #[cfg(target_os = "linux")]
    _anchor: Arc<File>,
    #[cfg(target_os = "linux")]
    object: Option<FilesystemObjectId>,
    #[cfg(target_os = "linux")]
    ancestors: Vec<FilesystemObjectId>,
    #[cfg(target_os = "linux")]
    backing: LinuxBackingLocation,
}

impl PathAttestation {
    /// Captures a canonical live path after proving that it names `pinned`.
    ///
    /// # Errors
    ///
    /// Returns an error when the platform cannot attest Linux mount topology,
    /// the path differs from the pinned object, or inspection fails.
    pub fn capture(path: &Path, pinned: &File) -> Result<Self, PathAttestationError> {
        #[cfg(target_os = "linux")]
        {
            Self::capture_linux(path, pinned)
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (path, pinned);
            Err(PathAttestationError::UnsupportedPlatform)
        }
    }

    /// Resolves and attests a configured destination without creating it.
    ///
    /// The nearest existing ancestor is pinned, and any missing normalized
    /// suffix is projected into both its namespace and backing locations.
    ///
    /// # Errors
    ///
    /// Returns an error when no safe existing ancestor can be pinned or Linux
    /// mount topology cannot be inspected.
    pub fn resolve_destination(path: &Path) -> Result<Self, PathAttestationError> {
        #[cfg(target_os = "linux")]
        {
            let mut existing = path;
            let mut missing = Vec::<OsString>::new();
            loop {
                match fs::canonicalize(existing) {
                    Ok(resolved) => {
                        let pinned =
                            open_path_handle(&resolved).map_err(PathAttestationError::Io)?;
                        let metadata = pinned.metadata().map_err(PathAttestationError::Io)?;
                        if !metadata.is_dir() && !metadata.is_file() {
                            return Err(PathAttestationError::BindingMismatch);
                        }
                        let mut attestation = Self::capture_linux(&resolved, &pinned)?;
                        if missing.is_empty() {
                            return Ok(attestation);
                        }
                        if !metadata.is_dir() {
                            return Err(PathAttestationError::BindingMismatch);
                        }
                        if let Some(object) = attestation.object.take() {
                            attestation.ancestors.insert(0, object);
                        }
                        for component in missing.iter().rev() {
                            attestation.canonical_path.push(component);
                            attestation.stable_path.push(component);
                            attestation.backing.path.push(component);
                        }
                        return Ok(attestation);
                    }
                    Err(source) if source.kind() == io::ErrorKind::NotFound => {
                        let Some(component) = existing.file_name() else {
                            return Err(PathAttestationError::Io(source));
                        };
                        missing.push(component.to_os_string());
                        let Some(parent) = existing.parent() else {
                            return Err(PathAttestationError::Io(source));
                        };
                        existing = parent;
                    }
                    Err(source) => return Err(PathAttestationError::Io(source)),
                }
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = path;
            Err(PathAttestationError::UnsupportedPlatform)
        }
    }

    /// Returns the canonical path captured for diagnostics and lexical checks.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.canonical_path
    }

    /// Returns a descriptor-backed path that retains the resolved destination.
    ///
    /// The path is valid only in this process and while this attestation remains
    /// alive. Callers must not persist it, pass it to a child process, or use a
    /// copied path after dropping the attestation.
    #[must_use]
    pub fn stable_path(&self) -> &Path {
        #[cfg(target_os = "linux")]
        {
            &self.stable_path
        }
        #[cfg(not(target_os = "linux"))]
        {
            &self.canonical_path
        }
    }

    /// Returns whether this object or destination is equal to or below `other`.
    #[must_use]
    pub fn is_within(&self, other: &Self) -> bool {
        #[cfg(target_os = "linux")]
        {
            self.canonical_path.starts_with(&other.canonical_path)
                || other.object.is_some_and(|object| {
                    self.object == Some(object) || self.ancestors.contains(&object)
                })
                || (self.backing.device == other.backing.device
                    && self.backing.path.starts_with(&other.backing.path))
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = other;
            false
        }
    }

    /// Returns whether two attestations name the same pinned object or the
    /// same not-yet-created destination below an identical backing path.
    #[must_use]
    pub fn matches_destination(&self, other: &Self) -> bool {
        #[cfg(target_os = "linux")]
        {
            self.canonical_path == other.canonical_path
                && self.backing.device == other.backing.device
                && self.backing.path == other.backing.path
                && self
                    .object
                    .is_none_or(|object| other.object == Some(object))
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = other;
            false
        }
    }

    #[cfg(target_os = "linux")]
    fn capture_linux(path: &Path, pinned: &File) -> Result<Self, PathAttestationError> {
        let canonical_path = fs::canonicalize(path).map_err(PathAttestationError::Io)?;
        let live_handle = open_path_handle(&canonical_path).map_err(PathAttestationError::Io)?;
        let (object, pinned_mount_id, is_directory) = validate_linux_binding(&live_handle, pinned)?;
        let object = Some(object);
        let stable_path = linux_stable_path(&live_handle, is_directory);
        let ancestors = canonical_path
            .ancestors()
            .skip(1)
            .map(|ancestor| {
                fs::metadata(ancestor)
                    .map(|metadata| FilesystemObjectId::from_metadata(&metadata))
                    .map_err(PathAttestationError::Io)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let backing = linux_backing_location(
            &canonical_path,
            object.map_or(0, |id| id.device),
            pinned_mount_id,
        )?;
        Ok(Self {
            canonical_path,
            stable_path,
            _anchor: Arc::new(live_handle),
            object,
            ancestors,
            backing,
        })
    }
}

#[cfg(target_os = "linux")]
fn validate_linux_binding(
    live: &File,
    pinned: &File,
) -> Result<(FilesystemObjectId, u64, bool), PathAttestationError> {
    let live_metadata = live.metadata().map_err(PathAttestationError::Io)?;
    let pinned_metadata = pinned.metadata().map_err(PathAttestationError::Io)?;
    let live_object = FilesystemObjectId::from_metadata(&live_metadata);
    let pinned_object = FilesystemObjectId::from_metadata(&pinned_metadata);
    let live_mount_id = linux_mount_id(live)?;
    let pinned_mount_id = linux_mount_id(pinned)?;
    if live_object != pinned_object || live_mount_id != pinned_mount_id {
        return Err(PathAttestationError::BindingMismatch);
    }
    Ok((pinned_object, pinned_mount_id, pinned_metadata.is_dir()))
}

#[cfg(target_os = "linux")]
fn linux_stable_path(file: &File, is_directory: bool) -> PathBuf {
    let path = PathBuf::from(format!("/proc/self/fd/{}", file.as_raw_fd()));
    if is_directory { path.join(".") } else { path }
}

#[cfg(target_os = "linux")]
fn open_path_handle(path: &Path) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_PATH | nix::libc::O_CLOEXEC)
        .open(path)
}

#[cfg(target_os = "linux")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FilesystemObjectId {
    device: u64,
    inode: u64,
}

#[cfg(target_os = "linux")]
impl FilesystemObjectId {
    fn from_metadata(metadata: &fs::Metadata) -> Self {
        use std::os::unix::fs::MetadataExt;

        Self {
            device: metadata.dev(),
            inode: metadata.ino(),
        }
    }
}

#[cfg(target_os = "linux")]
#[derive(Clone, Debug)]
struct LinuxBackingLocation {
    device: u64,
    path: PathBuf,
}

#[cfg(target_os = "linux")]
fn linux_backing_location(
    canonical_path: &Path,
    device: u64,
    mount_id: u64,
) -> Result<LinuxBackingLocation, PathAttestationError> {
    let mut bytes = Vec::with_capacity(MAXIMUM_MOUNT_INFO_BYTES as usize);
    File::open("/proc/self/mountinfo")
        .and_then(|file| {
            file.take(MAXIMUM_MOUNT_INFO_BYTES + 1)
                .read_to_end(&mut bytes)
        })
        .map_err(PathAttestationError::Io)?;
    if bytes.len() as u64 > MAXIMUM_MOUNT_INFO_BYTES {
        return Err(PathAttestationError::InvalidMountInformation);
    }
    linux_backing_location_from(&bytes, canonical_path, device, mount_id)
}

#[cfg(target_os = "linux")]
fn linux_mount_id(file: &File) -> Result<u64, PathAttestationError> {
    let mut bytes = Vec::with_capacity(MAXIMUM_FD_INFO_BYTES as usize);
    File::open(format!("/proc/self/fdinfo/{}", file.as_raw_fd()))
        .and_then(|file| file.take(MAXIMUM_FD_INFO_BYTES + 1).read_to_end(&mut bytes))
        .map_err(PathAttestationError::Io)?;
    if bytes.len() as u64 > MAXIMUM_FD_INFO_BYTES {
        return Err(PathAttestationError::InvalidMountInformation);
    }
    let contents =
        std::str::from_utf8(&bytes).map_err(|_| PathAttestationError::InvalidMountInformation)?;
    contents
        .lines()
        .find_map(|line| line.strip_prefix("mnt_id:").map(str::trim))
        .ok_or(PathAttestationError::InvalidMountInformation)?
        .parse()
        .map_err(|_| PathAttestationError::InvalidMountInformation)
}

#[cfg(target_os = "linux")]
fn linux_backing_location_from(
    mount_info: &[u8],
    canonical_path: &Path,
    device: u64,
    mount_id: u64,
) -> Result<LinuxBackingLocation, PathAttestationError> {
    for line in mount_info.split(|byte| *byte == b'\n') {
        let fields = line
            .split(|byte| *byte == b' ')
            .filter(|field| !field.is_empty())
            .collect::<Vec<_>>();
        if fields.len() < 6 {
            continue;
        }
        let Some(entry_mount_id) = std::str::from_utf8(fields[0])
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
        else {
            continue;
        };
        if entry_mount_id != mount_id {
            continue;
        }
        let Some(entry_device) = parse_linux_device(fields[2]) else {
            continue;
        };
        if entry_device != device {
            continue;
        }
        let root = decode_mount_path(fields[3])?;
        let mount_point = decode_mount_path(fields[4])?;
        let Ok(relative) = canonical_path.strip_prefix(&mount_point) else {
            continue;
        };
        return Ok(LinuxBackingLocation {
            device,
            path: root.join(relative),
        });
    }
    Err(PathAttestationError::InvalidMountInformation)
}

#[cfg(target_os = "linux")]
fn parse_linux_device(value: &[u8]) -> Option<u64> {
    let separator = value.iter().position(|byte| *byte == b':')?;
    let major = std::str::from_utf8(&value[..separator])
        .ok()?
        .parse::<u64>()
        .ok()?;
    let minor = std::str::from_utf8(&value[separator + 1..])
        .ok()?
        .parse::<u64>()
        .ok()?;
    Some(
        ((major & 0x0000_0fff) << 8)
            | (minor & 0x0000_00ff)
            | ((major & !0x0000_0fff) << 32)
            | ((minor & !0x0000_00ff) << 12),
    )
}

#[cfg(target_os = "linux")]
fn decode_mount_path(value: &[u8]) -> Result<PathBuf, PathAttestationError> {
    let mut decoded = Vec::with_capacity(value.len());
    let mut index = 0;
    while index < value.len() {
        if value[index] == b'\\' && index + 3 < value.len() {
            let digits = &value[index + 1..index + 4];
            if digits.iter().all(|digit| (b'0'..=b'7').contains(digit)) {
                let decoded_byte = u16::from(digits[0] - b'0') * 64
                    + u16::from(digits[1] - b'0') * 8
                    + u16::from(digits[2] - b'0');
                decoded.push(
                    u8::try_from(decoded_byte)
                        .map_err(|_| PathAttestationError::InvalidMountInformation)?,
                );
                index += 4;
                continue;
            }
        }
        decoded.push(value[index]);
        index += 1;
    }
    let path = PathBuf::from(OsString::from_vec(decoded));
    if !path.is_absolute() {
        return Err(PathAttestationError::InvalidMountInformation);
    }
    Ok(path)
}

/// Failure while attesting a pinned filesystem object.
#[derive(Debug)]
pub enum PathAttestationError {
    /// The current platform cannot attest the required filesystem topology.
    UnsupportedPlatform,
    /// The configured live path no longer names the pinned object.
    BindingMismatch,
    /// Linux mount topology was missing, malformed, or exceeded its bound.
    InvalidMountInformation,
    /// Filesystem inspection failed.
    Io(io::Error),
}

impl fmt::Display for PathAttestationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedPlatform => {
                formatter.write_str("filesystem path attestation requires Linux")
            }
            Self::BindingMismatch => {
                formatter.write_str("live filesystem path differs from its pinned object")
            }
            Self::InvalidMountInformation => {
                formatter.write_str("Linux mount topology could not be attested")
            }
            Self::Io(error) => write!(formatter, "could not attest filesystem path: {error}"),
        }
    }
}

impl std::error::Error for PathAttestationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::UnsupportedPlatform | Self::BindingMismatch | Self::InvalidMountInformation => {
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_os = "linux")]
    use std::fs::File;
    #[cfg(target_os = "linux")]
    use std::path::Path;

    #[cfg(target_os = "linux")]
    use nix::sys::stat::Mode;
    #[cfg(target_os = "linux")]
    use nix::unistd::mkfifo;

    #[cfg(target_os = "linux")]
    use super::{
        PathAttestation, PathAttestationError, linux_backing_location_from, parse_linux_device,
        validate_linux_binding,
    };

    #[cfg(target_os = "linux")]
    #[test]
    fn destination_rejects_a_fifo_without_blocking() {
        let root = std::env::temp_dir().join(format!(
            "agent-knowledge-core-attestation-test-{}",
            ulid::Ulid::generate()
        ));
        std::fs::create_dir(&root)
            .unwrap_or_else(|error| panic!("test root must be created: {error}"));
        let fifo = root.join("fictional-fifo");
        mkfifo(&fifo, Mode::S_IRUSR | Mode::S_IWUSR)
            .unwrap_or_else(|error| panic!("FIFO fixture must be created: {error}"));

        assert!(matches!(
            PathAttestation::resolve_destination(&fifo),
            Err(PathAttestationError::BindingMismatch)
        ));

        std::fs::remove_dir_all(&root)
            .unwrap_or_else(|error| panic!("test root must be removed: {error}"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn binding_rejects_different_handles_on_the_same_mount() {
        let live = File::open("/proc/self/status")
            .unwrap_or_else(|error| panic!("live fixture must open: {error}"));
        let pinned = File::open("/proc/self/mountinfo")
            .unwrap_or_else(|error| panic!("pinned fixture must open: {error}"));

        assert!(matches!(
            validate_linux_binding(&live, &pinned),
            Err(PathAttestationError::BindingMismatch)
        ));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn mount_roots_reveal_a_nested_bind_alias() {
        let device =
            parse_linux_device(b"0:42").unwrap_or_else(|| panic!("fixture device must parse"));
        let mount_info = b"30 20 0:42 / / rw - ext4 /dev/fictional rw\n\
31 30 0:42 /data/content/queue /mnt/queue rw - none /data/content/queue rw\n";
        let content =
            linux_backing_location_from(mount_info, Path::new("/data/content"), device, 30)
                .unwrap_or_else(|error| panic!("content backing path must resolve: {error}"));
        let queue = linux_backing_location_from(mount_info, Path::new("/mnt/queue"), device, 31)
            .unwrap_or_else(|error| panic!("queue backing path must resolve: {error}"));

        assert!(queue.path.starts_with(content.path));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn mount_paths_decode_kernel_octal_escapes() {
        let device =
            parse_linux_device(b"7:11").unwrap_or_else(|| panic!("fixture device must parse"));
        let mount_info =
            b"31 30 7:11 /fictional\\040root /mnt/fictional\\040root rw - none none rw\n";
        let backing = linux_backing_location_from(
            mount_info,
            Path::new("/mnt/fictional root/child"),
            device,
            31,
        )
        .unwrap_or_else(|error| panic!("escaped backing path must resolve: {error}"));

        assert_eq!(backing.path, Path::new("/fictional root/child"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn mount_id_selects_the_pinned_stacked_mount() {
        let device =
            parse_linux_device(b"7:11").unwrap_or_else(|| panic!("fixture device must parse"));
        let mount_info = b"31 30 7:11 /wrong/root /mnt/stacked rw - none none rw\n\
32 30 7:11 /data/content/queue /mnt/stacked rw - none none rw\n";
        let backing =
            linux_backing_location_from(mount_info, Path::new("/mnt/stacked"), device, 32)
                .unwrap_or_else(|error| panic!("pinned stacked mount must resolve: {error}"));

        assert_eq!(backing.path, Path::new("/data/content/queue"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn oversized_octal_mount_escape_fails_closed() {
        let device =
            parse_linux_device(b"7:11").unwrap_or_else(|| panic!("fixture device must parse"));
        let mount_info = b"31 30 7:11 /fictional\\400root /mnt/fictional rw - none none rw\n";

        assert!(
            linux_backing_location_from(mount_info, Path::new("/mnt/fictional"), device, 31,)
                .is_err()
        );
    }
}
