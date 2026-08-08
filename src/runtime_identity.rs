use std::collections::BTreeSet;
use std::fmt;
use std::fs;
use std::io;
use std::os::fd::AsRawFd;
#[cfg(target_os = "linux")]
use std::os::unix::fs::{FileTypeExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

use agent_knowledge_gateway::GatewaySettings;
use agent_knowledge_worker::WorkerSettings;

#[cfg(target_os = "linux")]
use nix::sys::socket::{UnixAddr, getsockname};
#[cfg(target_os = "linux")]
use nix::unistd::{Gid, Uid, getgroups};
#[cfg(target_os = "linux")]
use xattr::FileExt as _;

const WORKER_ROLE: &str = "Worker";
const QUEUE_INGRESS_ROLE: &str = "Queue Ingress";
const GATEWAY_ROLE: &str = "Gateway";
#[cfg(target_os = "linux")]
const ACTIVATED_SOCKET_MODE: u32 = 0o660;
const QUEUE_DIRECTORY_MODE: u32 = 0o2770;
const READER_DIRECTORY_MODE: u32 = 0o2750;
const WORKER_PRIVATE_DIRECTORY_MODE: u32 = 0o750;
const RUNTIME_DIRECTORY_MODE: u32 = 0o2750;
#[cfg(target_os = "linux")]
const POSIX_ACL_XATTRS: [&str; 2] = ["system.posix_acl_access", "system.posix_acl_default"];

#[derive(Clone, Debug, Eq, PartialEq)]
struct ProcessIdentity {
    uid: u32,
    primary_gid: u32,
    groups: BTreeSet<u32>,
}

impl ProcessIdentity {
    #[cfg(target_os = "linux")]
    fn current() -> Result<Self, RuntimeIdentityError> {
        let primary_gid = Gid::effective().as_raw();
        let mut groups = getgroups()
            .map_err(|error| {
                RuntimeIdentityError::GroupInspection(io::Error::from_raw_os_error(error as i32))
            })?
            .into_iter()
            .map(Gid::as_raw)
            .collect::<BTreeSet<_>>();
        groups.insert(primary_gid);
        Ok(Self {
            uid: Uid::effective().as_raw(),
            primary_gid,
            groups,
        })
    }

    #[cfg(not(target_os = "linux"))]
    fn current() -> Result<Self, RuntimeIdentityError> {
        Err(RuntimeIdentityError::UnsupportedPlatform)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DirectoryIdentity {
    path: PathBuf,
    uid: u32,
    gid: u32,
    mode: u32,
}

impl DirectoryIdentity {
    #[cfg(target_os = "linux")]
    fn inspect(path: &Path) -> Result<Self, RuntimeIdentityError> {
        let directory = fs::OpenOptions::new()
            .read(true)
            .custom_flags(nix::libc::O_DIRECTORY | nix::libc::O_NOFOLLOW | nix::libc::O_CLOEXEC)
            .open(path)
            .map_err(|source| RuntimeIdentityError::BoundaryInspection {
                path: path.to_path_buf(),
                source,
            })?;
        let metadata =
            directory
                .metadata()
                .map_err(|source| RuntimeIdentityError::BoundaryInspection {
                    path: path.to_path_buf(),
                    source,
                })?;
        if !metadata.file_type().is_dir() {
            return Err(RuntimeIdentityError::InvalidBoundary(path.to_path_buf()));
        }
        for name in POSIX_ACL_XATTRS {
            if directory
                .get_xattr(name)
                .map_err(|source| RuntimeIdentityError::BoundaryInspection {
                    path: path.to_path_buf(),
                    source,
                })?
                .is_some()
            {
                return Err(RuntimeIdentityError::PosixAclBoundary(path.to_path_buf()));
            }
        }
        Ok(Self {
            path: path.to_path_buf(),
            uid: metadata.uid(),
            gid: metadata.gid(),
            mode: metadata.mode() & 0o7777,
        })
    }

    #[cfg(not(target_os = "linux"))]
    fn inspect(_path: &Path) -> Result<Self, RuntimeIdentityError> {
        Err(RuntimeIdentityError::UnsupportedPlatform)
    }

    #[cfg(test)]
    fn fictional(path: &str, uid: u32, gid: u32, mode: u32) -> Self {
        Self {
            path: PathBuf::from(path),
            uid,
            gid,
            mode,
        }
    }
}

pub(crate) fn validate_worker(settings: &WorkerSettings) -> Result<(), RuntimeIdentityError> {
    let boundaries = WorkerBoundaries {
        queue: DirectoryIdentity::inspect(settings.queue_root())?,
        repository: DirectoryIdentity::inspect(settings.repository_root())?,
        content: DirectoryIdentity::inspect(settings.content_root())?,
        work: DirectoryIdentity::inspect(settings.work_root())?,
        releases: DirectoryIdentity::inspect(settings.release_root())?,
        search_indexes: settings
            .search_index_root()
            .map(DirectoryIdentity::inspect)
            .transpose()?,
    };
    validate_worker_identity(&ProcessIdentity::current()?, &boundaries)
}

pub(crate) fn validate_queue_ingress(
    queue_root: &Path,
    socket_path: &Path,
) -> Result<(), RuntimeIdentityError> {
    let queue = DirectoryIdentity::inspect(queue_root)?;
    let runtime = socket_path
        .parent()
        .ok_or_else(|| RuntimeIdentityError::InvalidBoundary(socket_path.to_path_buf()))
        .and_then(DirectoryIdentity::inspect)?;
    validate_queue_ingress_identity(&ProcessIdentity::current()?, &queue, &runtime)
}

pub(crate) fn validate_activated_queue_ingress(
    queue_root: &Path,
    socket_path: &Path,
    input: &impl AsRawFd,
) -> Result<(), RuntimeIdentityError> {
    validate_queue_ingress(queue_root, socket_path)?;
    validate_activated_socket(input.as_raw_fd(), socket_path)
}

#[cfg(target_os = "linux")]
fn validate_activated_socket(
    input_fd: std::os::fd::RawFd,
    expected_path: &Path,
) -> Result<(), RuntimeIdentityError> {
    let address = getsockname::<UnixAddr>(input_fd).map_err(|error| {
        RuntimeIdentityError::SocketInspection(io::Error::from_raw_os_error(error as i32))
    })?;
    let actual_path = address
        .path()
        .ok_or(RuntimeIdentityError::UnnamedActivatedSocket)?;
    if actual_path != expected_path {
        return Err(RuntimeIdentityError::ActivatedSocketPathMismatch {
            expected: expected_path.to_path_buf(),
            actual: actual_path.to_path_buf(),
        });
    }
    let metadata = inspect_socket_boundary(expected_path)?;
    if !metadata.file_type().is_socket() {
        return Err(RuntimeIdentityError::InvalidSocketBoundary(
            expected_path.to_path_buf(),
        ));
    }
    let parent = expected_path
        .parent()
        .ok_or_else(|| RuntimeIdentityError::InvalidBoundary(expected_path.to_path_buf()))?;
    let runtime = DirectoryIdentity::inspect(parent)?;
    require_socket_identity(expected_path, &metadata, &runtime)?;
    let device = metadata.dev();
    let inode = metadata.ino();
    for name in POSIX_ACL_XATTRS {
        if xattr::get(expected_path, name)
            .map_err(|source| RuntimeIdentityError::SocketBoundaryInspection {
                path: expected_path.to_path_buf(),
                source,
            })?
            .is_some()
        {
            return Err(RuntimeIdentityError::PosixAclSocketBoundary(
                expected_path.to_path_buf(),
            ));
        }
    }
    let observed = inspect_socket_boundary(expected_path)?;
    if !observed.file_type().is_socket() || observed.dev() != device || observed.ino() != inode {
        return Err(RuntimeIdentityError::SocketBoundaryChanged(
            expected_path.to_path_buf(),
        ));
    }
    require_socket_identity(expected_path, &observed, &runtime)
}

#[cfg(target_os = "linux")]
fn inspect_socket_boundary(path: &Path) -> Result<fs::Metadata, RuntimeIdentityError> {
    fs::symlink_metadata(path).map_err(|source| RuntimeIdentityError::SocketBoundaryInspection {
        path: path.to_path_buf(),
        source,
    })
}

#[cfg(target_os = "linux")]
fn require_socket_identity(
    path: &Path,
    metadata: &fs::Metadata,
    runtime: &DirectoryIdentity,
) -> Result<(), RuntimeIdentityError> {
    let mode = metadata.mode() & 0o7777;
    if metadata.uid() != runtime.uid
        || metadata.gid() != runtime.gid
        || mode != ACTIVATED_SOCKET_MODE
    {
        return Err(RuntimeIdentityError::InvalidSocketIdentity {
            path: path.to_path_buf(),
            expected_uid: runtime.uid,
            actual_uid: metadata.uid(),
            expected_gid: runtime.gid,
            actual_gid: metadata.gid(),
            expected_mode: ACTIVATED_SOCKET_MODE,
            actual_mode: mode,
        });
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn validate_activated_socket(
    _input_fd: std::os::fd::RawFd,
    _expected_path: &Path,
) -> Result<(), RuntimeIdentityError> {
    Err(RuntimeIdentityError::UnsupportedPlatform)
}

pub(crate) fn validate_gateway(settings: &GatewaySettings) -> Result<(), RuntimeIdentityError> {
    let socket_parent = settings
        .queue_socket()
        .parent()
        .ok_or_else(|| RuntimeIdentityError::InvalidBoundary(settings.queue_socket().into()))?;
    let boundaries = GatewayBoundaries {
        repository: DirectoryIdentity::inspect(settings.git_directory())?,
        content: DirectoryIdentity::inspect(settings.content_root())?,
        runtime: DirectoryIdentity::inspect(socket_parent)?,
    };
    validate_gateway_identity(
        &ProcessIdentity::current()?,
        settings.gateway_uid(),
        &boundaries,
    )
}

struct WorkerBoundaries {
    queue: DirectoryIdentity,
    repository: DirectoryIdentity,
    content: DirectoryIdentity,
    work: DirectoryIdentity,
    releases: DirectoryIdentity,
    search_indexes: Option<DirectoryIdentity>,
}

fn validate_worker_identity(
    process: &ProcessIdentity,
    boundaries: &WorkerBoundaries,
) -> Result<(), RuntimeIdentityError> {
    require_boundary_mode(WORKER_ROLE, &boundaries.queue, QUEUE_DIRECTORY_MODE)?;
    require_boundary_mode(WORKER_ROLE, &boundaries.repository, READER_DIRECTORY_MODE)?;
    require_boundary_mode(WORKER_ROLE, &boundaries.content, READER_DIRECTORY_MODE)?;
    require_boundary_mode(WORKER_ROLE, &boundaries.work, WORKER_PRIVATE_DIRECTORY_MODE)?;
    require_boundary_mode(
        WORKER_ROLE,
        &boundaries.releases,
        WORKER_PRIVATE_DIRECTORY_MODE,
    )?;
    if let Some(search_indexes) = boundaries.search_indexes.as_ref() {
        require_boundary_mode(WORKER_ROLE, search_indexes, READER_DIRECTORY_MODE)?;
    }
    let mut worker_owned = vec![&boundaries.content, &boundaries.work, &boundaries.releases];
    if let Some(search_indexes) = boundaries.search_indexes.as_ref() {
        worker_owned.push(search_indexes);
    }
    require_matching_owner(WORKER_ROLE, &boundaries.repository, &worker_owned)?;
    let mut gateway_readable = vec![&boundaries.content];
    if let Some(search_indexes) = boundaries.search_indexes.as_ref() {
        gateway_readable.push(search_indexes);
    }
    require_matching_group(WORKER_ROLE, &boundaries.repository, &gateway_readable)?;
    require_matching_group(WORKER_ROLE, &boundaries.work, &[&boundaries.releases])?;
    require_distinct(
        WORKER_ROLE,
        "Worker and Queue Ingress owners",
        boundaries.work.uid,
        boundaries.queue.uid,
    )?;
    require_distinct_groups(
        WORKER_ROLE,
        &[
            boundaries.work.gid,
            boundaries.queue.gid,
            boundaries.repository.gid,
        ],
    )?;
    require_process_identity(
        WORKER_ROLE,
        process,
        boundaries.work.uid,
        boundaries.work.gid,
        [boundaries.work.gid, boundaries.queue.gid],
    )
}

fn validate_queue_ingress_identity(
    process: &ProcessIdentity,
    queue: &DirectoryIdentity,
    runtime: &DirectoryIdentity,
) -> Result<(), RuntimeIdentityError> {
    require_boundary_mode(QUEUE_INGRESS_ROLE, queue, QUEUE_DIRECTORY_MODE)?;
    require_boundary_mode(QUEUE_INGRESS_ROLE, runtime, RUNTIME_DIRECTORY_MODE)?;
    require_matching_owner(QUEUE_INGRESS_ROLE, queue, &[runtime])?;
    require_distinct(
        QUEUE_INGRESS_ROLE,
        "queue-owner and ingress-client groups",
        queue.gid,
        runtime.gid,
    )?;
    require_process_identity(
        QUEUE_INGRESS_ROLE,
        process,
        queue.uid,
        queue.gid,
        [queue.gid],
    )
}

struct GatewayBoundaries {
    repository: DirectoryIdentity,
    content: DirectoryIdentity,
    runtime: DirectoryIdentity,
}

fn validate_gateway_identity(
    process: &ProcessIdentity,
    expected_uid: u32,
    boundaries: &GatewayBoundaries,
) -> Result<(), RuntimeIdentityError> {
    require_boundary_mode(GATEWAY_ROLE, &boundaries.repository, READER_DIRECTORY_MODE)?;
    require_boundary_mode(GATEWAY_ROLE, &boundaries.content, READER_DIRECTORY_MODE)?;
    require_boundary_mode(GATEWAY_ROLE, &boundaries.runtime, RUNTIME_DIRECTORY_MODE)?;
    require_matching_owner(GATEWAY_ROLE, &boundaries.repository, &[&boundaries.content])?;
    require_matching_group(GATEWAY_ROLE, &boundaries.repository, &[&boundaries.content])?;
    require_distinct(
        GATEWAY_ROLE,
        "Repository Worker and Queue Ingress owners",
        boundaries.repository.uid,
        boundaries.runtime.uid,
    )?;
    require_distinct(
        GATEWAY_ROLE,
        "Gateway-reader and ingress-client groups",
        boundaries.repository.gid,
        boundaries.runtime.gid,
    )?;
    if process.uid == boundaries.repository.uid || process.uid == boundaries.runtime.uid {
        return Err(RuntimeIdentityError::UnsafeProcessUser {
            role: GATEWAY_ROLE,
            actual: process.uid,
        });
    }
    require_process_identity(
        GATEWAY_ROLE,
        process,
        expected_uid,
        boundaries.repository.gid,
        [boundaries.repository.gid, boundaries.runtime.gid],
    )
}

fn require_boundary_mode(
    role: &'static str,
    boundary: &DirectoryIdentity,
    expected: u32,
) -> Result<(), RuntimeIdentityError> {
    if boundary.mode != expected {
        return Err(RuntimeIdentityError::BoundaryModeMismatch {
            role,
            path: boundary.path.clone(),
            expected,
            actual: boundary.mode,
        });
    }
    Ok(())
}

fn require_matching_owner(
    role: &'static str,
    expected: &DirectoryIdentity,
    observed: &[&DirectoryIdentity],
) -> Result<(), RuntimeIdentityError> {
    if expected.uid == 0 {
        return Err(RuntimeIdentityError::UnsafeBoundaryIdentity {
            role,
            path: expected.path.clone(),
        });
    }
    for boundary in observed {
        if boundary.uid != expected.uid {
            return Err(RuntimeIdentityError::InconsistentBoundaryOwner {
                role,
                expected: expected.path.clone(),
                observed: boundary.path.clone(),
            });
        }
    }
    Ok(())
}

fn require_matching_group(
    role: &'static str,
    expected: &DirectoryIdentity,
    observed: &[&DirectoryIdentity],
) -> Result<(), RuntimeIdentityError> {
    if expected.gid == 0 {
        return Err(RuntimeIdentityError::UnsafeBoundaryIdentity {
            role,
            path: expected.path.clone(),
        });
    }
    for boundary in observed {
        if boundary.gid != expected.gid {
            return Err(RuntimeIdentityError::InconsistentBoundaryGroup {
                role,
                expected: expected.path.clone(),
                observed: boundary.path.clone(),
            });
        }
    }
    Ok(())
}

fn require_distinct(
    role: &'static str,
    boundary: &'static str,
    first: u32,
    second: u32,
) -> Result<(), RuntimeIdentityError> {
    if first == 0 || second == 0 || first == second {
        Err(RuntimeIdentityError::CollapsedBoundary { role, boundary })
    } else {
        Ok(())
    }
}

fn require_distinct_groups(role: &'static str, groups: &[u32]) -> Result<(), RuntimeIdentityError> {
    if groups.contains(&0)
        || groups
            .iter()
            .enumerate()
            .any(|(index, group)| groups[..index].contains(group))
    {
        Err(RuntimeIdentityError::CollapsedBoundary {
            role,
            boundary: "service role groups",
        })
    } else {
        Ok(())
    }
}

fn require_process_identity<const N: usize>(
    role: &'static str,
    process: &ProcessIdentity,
    expected_uid: u32,
    expected_primary_gid: u32,
    expected_groups: [u32; N],
) -> Result<(), RuntimeIdentityError> {
    if expected_uid == 0 || expected_primary_gid == 0 || expected_groups.contains(&0) {
        return Err(RuntimeIdentityError::CollapsedBoundary {
            role,
            boundary: "service process identities",
        });
    }
    if process.uid == 0 {
        return Err(RuntimeIdentityError::RootProcess { role });
    }
    if process.uid != expected_uid {
        return Err(RuntimeIdentityError::ProcessUserMismatch {
            role,
            expected: expected_uid,
            actual: process.uid,
        });
    }
    if process.primary_gid != expected_primary_gid {
        return Err(RuntimeIdentityError::PrimaryGroupMismatch {
            role,
            expected: expected_primary_gid,
            actual: process.primary_gid,
        });
    }
    let expected = expected_groups.into_iter().collect::<BTreeSet<_>>();
    if process.groups != expected {
        return Err(RuntimeIdentityError::GroupSetMismatch {
            role,
            expected: expected.into_iter().collect(),
            actual: process.groups.iter().copied().collect(),
        });
    }
    Ok(())
}

#[derive(Debug)]
pub(crate) enum RuntimeIdentityError {
    #[cfg(not(target_os = "linux"))]
    UnsupportedPlatform,
    GroupInspection(io::Error),
    SocketInspection(io::Error),
    BoundaryInspection {
        path: PathBuf,
        source: io::Error,
    },
    InvalidBoundary(PathBuf),
    PosixAclBoundary(PathBuf),
    BoundaryModeMismatch {
        role: &'static str,
        path: PathBuf,
        expected: u32,
        actual: u32,
    },
    UnnamedActivatedSocket,
    ActivatedSocketPathMismatch {
        expected: PathBuf,
        actual: PathBuf,
    },
    SocketBoundaryInspection {
        path: PathBuf,
        source: io::Error,
    },
    InvalidSocketBoundary(PathBuf),
    PosixAclSocketBoundary(PathBuf),
    SocketBoundaryChanged(PathBuf),
    InvalidSocketIdentity {
        path: PathBuf,
        expected_uid: u32,
        actual_uid: u32,
        expected_gid: u32,
        actual_gid: u32,
        expected_mode: u32,
        actual_mode: u32,
    },
    UnsafeBoundaryIdentity {
        role: &'static str,
        path: PathBuf,
    },
    InconsistentBoundaryOwner {
        role: &'static str,
        expected: PathBuf,
        observed: PathBuf,
    },
    InconsistentBoundaryGroup {
        role: &'static str,
        expected: PathBuf,
        observed: PathBuf,
    },
    CollapsedBoundary {
        role: &'static str,
        boundary: &'static str,
    },
    RootProcess {
        role: &'static str,
    },
    UnsafeProcessUser {
        role: &'static str,
        actual: u32,
    },
    ProcessUserMismatch {
        role: &'static str,
        expected: u32,
        actual: u32,
    },
    PrimaryGroupMismatch {
        role: &'static str,
        expected: u32,
        actual: u32,
    },
    GroupSetMismatch {
        role: &'static str,
        expected: Vec<u32>,
        actual: Vec<u32>,
    },
}

impl fmt::Display for RuntimeIdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            #[cfg(not(target_os = "linux"))]
            Self::UnsupportedPlatform => {
                formatter.write_str("runtime identity validation requires Linux")
            }
            Self::GroupInspection(error) => {
                write!(formatter, "could not inspect supplementary groups: {error}")
            }
            Self::SocketInspection(error) => {
                write!(
                    formatter,
                    "could not inspect activated Unix socket: {error}"
                )
            }
            Self::BoundaryInspection { path, source } => {
                write!(formatter, "could not inspect {}: {source}", path.display())
            }
            Self::InvalidBoundary(path) => {
                write!(
                    formatter,
                    "runtime identity boundary {} is not a directory",
                    path.display()
                )
            }
            Self::PosixAclBoundary(path) => write!(
                formatter,
                "runtime identity boundary {} has a POSIX ACL",
                path.display()
            ),
            Self::BoundaryModeMismatch {
                role,
                path,
                expected,
                actual,
            } => write!(
                formatter,
                "{role} boundary {} mode {actual:04o} does not match required {expected:04o}",
                path.display()
            ),
            Self::UnnamedActivatedSocket => {
                formatter.write_str("activated Unix socket has no filesystem path")
            }
            Self::ActivatedSocketPathMismatch { expected, actual } => write!(
                formatter,
                "activated Unix socket {} does not match configured path {}",
                actual.display(),
                expected.display()
            ),
            Self::SocketBoundaryInspection { path, source } => write!(
                formatter,
                "could not inspect activated Unix socket {}: {source}",
                path.display()
            ),
            Self::InvalidSocketBoundary(path) => write!(
                formatter,
                "activated Unix socket path {} is not a socket",
                path.display()
            ),
            Self::PosixAclSocketBoundary(path) => write!(
                formatter,
                "activated Unix socket {} has a POSIX ACL",
                path.display()
            ),
            Self::SocketBoundaryChanged(path) => write!(
                formatter,
                "activated Unix socket {} changed during identity validation",
                path.display()
            ),
            Self::InvalidSocketIdentity {
                path,
                expected_uid,
                actual_uid,
                expected_gid,
                actual_gid,
                expected_mode,
                actual_mode,
            } => write!(
                formatter,
                "activated Unix socket {} identity {actual_uid}:{actual_gid}:{actual_mode:04o} does not match required {expected_uid}:{expected_gid}:{expected_mode:04o}",
                path.display()
            ),
            Self::UnsafeBoundaryIdentity { role, path } => write!(
                formatter,
                "{role} boundary {} uses a root identity",
                path.display()
            ),
            Self::InconsistentBoundaryOwner {
                role,
                expected,
                observed,
            } => write!(
                formatter,
                "{role} boundaries {} and {} have different owners",
                expected.display(),
                observed.display()
            ),
            Self::InconsistentBoundaryGroup {
                role,
                expected,
                observed,
            } => write!(
                formatter,
                "{role} boundaries {} and {} have different groups",
                expected.display(),
                observed.display()
            ),
            Self::CollapsedBoundary { role, boundary } => {
                write!(
                    formatter,
                    "{role} {boundary} are not distinct non-root identities"
                )
            }
            Self::RootProcess { role } => write!(formatter, "{role} must not run as root"),
            Self::UnsafeProcessUser { role, actual } => write!(
                formatter,
                "{role} process user {actual} owns another service boundary"
            ),
            Self::ProcessUserMismatch {
                role,
                expected,
                actual,
            } => write!(
                formatter,
                "{role} process user {actual} does not match boundary owner {expected}"
            ),
            Self::PrimaryGroupMismatch {
                role,
                expected,
                actual,
            } => write!(
                formatter,
                "{role} primary group {actual} does not match required group {expected}"
            ),
            Self::GroupSetMismatch {
                role,
                expected,
                actual,
            } => write!(
                formatter,
                "{role} process groups {actual:?} do not exactly match required groups {expected:?}"
            ),
        }
    }
}

impl std::error::Error for RuntimeIdentityError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::GroupInspection(error) | Self::SocketInspection(error) => Some(error),
            Self::BoundaryInspection { source, .. }
            | Self::SocketBoundaryInspection { source, .. } => Some(source),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::{self, File};
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::net::{UnixListener, UnixStream};

    use nix::unistd::Uid;
    use ulid::Ulid;
    use xattr::FileExt as _;

    use super::{
        ACTIVATED_SOCKET_MODE, DirectoryIdentity, GatewayBoundaries, ProcessIdentity,
        RuntimeIdentityError, WorkerBoundaries, validate_activated_socket,
        validate_gateway_identity, validate_queue_ingress_identity, validate_worker_identity,
    };
    use std::collections::BTreeSet;

    fn process(uid: u32, primary_gid: u32, groups: &[u32]) -> ProcessIdentity {
        ProcessIdentity {
            uid,
            primary_gid,
            groups: groups.iter().copied().collect::<BTreeSet<_>>(),
        }
    }

    fn extended_posix_acl(named_uid: u32) -> Vec<u8> {
        let mut value = 2_u32.to_le_bytes().to_vec();
        for (tag, permissions, id) in [
            (0x01_u16, 0o7_u16, u32::MAX),
            (0x02_u16, 0o7_u16, named_uid),
            (0x04_u16, 0o5_u16, u32::MAX),
            (0x10_u16, 0o7_u16, u32::MAX),
            (0x20_u16, 0o5_u16, u32::MAX),
        ] {
            value.extend_from_slice(&tag.to_le_bytes());
            value.extend_from_slice(&permissions.to_le_bytes());
            value.extend_from_slice(&id.to_le_bytes());
        }
        value
    }

    #[test]
    fn activated_socket_must_match_the_configured_path() {
        let directory = std::env::temp_dir().join(format!(
            "agent-knowledge-activated-socket-test-{}",
            Ulid::generate()
        ));
        fs::create_dir(&directory)
            .unwrap_or_else(|error| panic!("socket test directory must be created: {error}"));
        let actual_path = directory.join("actual.sock");
        let configured_path = directory.join("configured.sock");
        let listener = UnixListener::bind(&actual_path)
            .unwrap_or_else(|error| panic!("socket test listener must bind: {error}"));
        fs::set_permissions(
            &actual_path,
            fs::Permissions::from_mode(ACTIVATED_SOCKET_MODE),
        )
        .unwrap_or_else(|error| panic!("socket test mode must be set: {error}"));
        let client = UnixStream::connect(&actual_path)
            .unwrap_or_else(|error| panic!("socket test client must connect: {error}"));
        let (accepted, _) = listener
            .accept()
            .unwrap_or_else(|error| panic!("socket test connection must be accepted: {error}"));

        assert!(validate_activated_socket(accepted.as_raw_fd(), &actual_path).is_ok());
        assert!(matches!(
            validate_activated_socket(accepted.as_raw_fd(), &configured_path),
            Err(RuntimeIdentityError::ActivatedSocketPathMismatch { .. })
        ));
        xattr::set(
            &actual_path,
            "system.posix_acl_access",
            &extended_posix_acl(Uid::effective().as_raw()),
        )
        .unwrap_or_else(|error| panic!("socket ACL fixture must be written: {error}"));
        fs::set_permissions(
            &actual_path,
            fs::Permissions::from_mode(ACTIVATED_SOCKET_MODE),
        )
        .unwrap_or_else(|error| panic!("socket test mode must be restored: {error}"));
        assert!(matches!(
            validate_activated_socket(accepted.as_raw_fd(), &actual_path),
            Err(RuntimeIdentityError::PosixAclSocketBoundary(path)) if path == actual_path
        ));

        drop(accepted);
        drop(client);
        drop(listener);
        fs::remove_file(&actual_path)
            .unwrap_or_else(|error| panic!("socket test path must be removed: {error}"));
        fs::remove_dir(&directory)
            .unwrap_or_else(|error| panic!("socket test directory must be removed: {error}"));
    }

    #[test]
    fn directory_inspection_rejects_posix_acls() {
        let directory = std::env::temp_dir().join(format!(
            "agent-knowledge-runtime-acl-test-{}",
            Ulid::generate()
        ));
        fs::create_dir(&directory)
            .unwrap_or_else(|error| panic!("ACL test directory must be created: {error}"));
        let file = File::open(&directory)
            .unwrap_or_else(|error| panic!("ACL test directory must open: {error}"));
        file.set_xattr(
            "system.posix_acl_access",
            &extended_posix_acl(Uid::effective().as_raw()),
        )
        .unwrap_or_else(|error| panic!("ACL fixture must be written: {error}"));

        assert!(matches!(
            DirectoryIdentity::inspect(&directory),
            Err(RuntimeIdentityError::PosixAclBoundary(path)) if path == directory
        ));

        drop(file);
        fs::remove_dir(&directory)
            .unwrap_or_else(|error| panic!("ACL test directory must be removed: {error}"));
    }

    fn worker_boundaries() -> WorkerBoundaries {
        WorkerBoundaries {
            queue: DirectoryIdentity::fictional("/srv/fictional/queue", 61_002, 62_002, 0o2770),
            repository: DirectoryIdentity::fictional(
                "/srv/fictional/repository",
                61_003,
                62_001,
                0o2750,
            ),
            content: DirectoryIdentity::fictional("/srv/fictional/content", 61_003, 62_001, 0o2750),
            work: DirectoryIdentity::fictional("/srv/fictional/work", 61_003, 62_003, 0o750),
            releases: DirectoryIdentity::fictional(
                "/srv/fictional/releases",
                61_003,
                62_003,
                0o750,
            ),
            search_indexes: Some(DirectoryIdentity::fictional(
                "/srv/fictional/search-indexes",
                61_003,
                62_001,
                0o2750,
            )),
        }
    }

    #[test]
    fn accepts_exact_worker_identity() {
        assert!(
            validate_worker_identity(
                &process(61_003, 62_003, &[62_002, 62_003]),
                &worker_boundaries(),
            )
            .is_ok()
        );
    }

    #[test]
    fn accepts_worker_identity_when_search_indexes_are_disabled() {
        let mut boundaries = worker_boundaries();
        boundaries.search_indexes = None;
        assert!(
            validate_worker_identity(&process(61_003, 62_003, &[62_002, 62_003]), &boundaries,)
                .is_ok()
        );
    }

    #[test]
    fn rejects_worker_with_an_unrelated_group() {
        assert!(matches!(
            validate_worker_identity(
                &process(61_003, 62_003, &[62_002, 62_003, 62_099]),
                &worker_boundaries(),
            ),
            Err(RuntimeIdentityError::GroupSetMismatch { .. })
        ));
    }

    #[test]
    fn rejects_worker_when_storage_roles_collapse() {
        let mut boundaries = worker_boundaries();
        boundaries.repository.gid = boundaries.queue.gid;
        boundaries.content.gid = boundaries.queue.gid;
        boundaries
            .search_indexes
            .as_mut()
            .unwrap_or_else(|| panic!("search index fixture must exist"))
            .gid = boundaries.queue.gid;
        assert!(matches!(
            validate_worker_identity(&process(61_003, 62_003, &[62_002, 62_003]), &boundaries,),
            Err(RuntimeIdentityError::CollapsedBoundary { .. })
        ));
    }

    #[test]
    fn rejects_worker_with_writable_reader_storage() {
        let mut boundaries = worker_boundaries();
        boundaries.repository.mode = 0o2770;
        assert!(matches!(
            validate_worker_identity(&process(61_003, 62_003, &[62_002, 62_003]), &boundaries,),
            Err(RuntimeIdentityError::BoundaryModeMismatch { .. })
        ));
    }

    #[test]
    fn accepts_exact_queue_ingress_identity() {
        let queue = DirectoryIdentity::fictional("/srv/fictional/queue", 61_002, 62_002, 0o2770);
        let runtime = DirectoryIdentity::fictional("/run/fictional", 61_002, 62_004, 0o2750);
        assert!(
            validate_queue_ingress_identity(&process(61_002, 62_002, &[62_002]), &queue, &runtime,)
                .is_ok()
        );
    }

    #[test]
    fn rejects_queue_ingress_in_the_socket_client_group() {
        let queue = DirectoryIdentity::fictional("/srv/fictional/queue", 61_002, 62_002, 0o2770);
        let runtime = DirectoryIdentity::fictional("/run/fictional", 61_002, 62_004, 0o2750);
        assert!(matches!(
            validate_queue_ingress_identity(
                &process(61_002, 62_002, &[62_002, 62_004]),
                &queue,
                &runtime,
            ),
            Err(RuntimeIdentityError::GroupSetMismatch { .. })
        ));
    }

    #[test]
    fn rejects_queue_ingress_with_a_root_owned_queue() {
        let queue = DirectoryIdentity::fictional("/srv/fictional/queue", 61_002, 0, 0o2770);
        let runtime = DirectoryIdentity::fictional("/run/fictional", 61_002, 62_004, 0o2750);
        assert!(matches!(
            validate_queue_ingress_identity(&process(61_002, 0, &[0]), &queue, &runtime),
            Err(RuntimeIdentityError::CollapsedBoundary { .. })
        ));
    }

    #[test]
    fn rejects_queue_ingress_with_a_writable_socket_namespace() {
        let queue = DirectoryIdentity::fictional("/srv/fictional/queue", 61_002, 62_002, 0o2770);
        let runtime = DirectoryIdentity::fictional("/run/fictional", 61_002, 62_004, 0o2770);
        assert!(matches!(
            validate_queue_ingress_identity(&process(61_002, 62_002, &[62_002]), &queue, &runtime,),
            Err(RuntimeIdentityError::BoundaryModeMismatch { .. })
        ));
    }

    #[test]
    fn rejects_queue_ingress_with_world_writable_storage() {
        let queue = DirectoryIdentity::fictional("/srv/fictional/queue", 61_002, 62_002, 0o2777);
        let runtime = DirectoryIdentity::fictional("/run/fictional", 61_002, 62_004, 0o2750);
        assert!(matches!(
            validate_queue_ingress_identity(&process(61_002, 62_002, &[62_002]), &queue, &runtime,),
            Err(RuntimeIdentityError::BoundaryModeMismatch { .. })
        ));
    }

    fn gateway_boundaries() -> GatewayBoundaries {
        GatewayBoundaries {
            repository: DirectoryIdentity::fictional(
                "/srv/fictional/repository",
                61_003,
                62_001,
                0o2750,
            ),
            content: DirectoryIdentity::fictional("/srv/fictional/content", 61_003, 62_001, 0o2750),
            runtime: DirectoryIdentity::fictional("/run/fictional", 61_002, 62_004, 0o2750),
        }
    }

    #[test]
    fn accepts_exact_gateway_identity() {
        assert!(
            validate_gateway_identity(
                &process(61_001, 62_001, &[62_001, 62_004]),
                61_001,
                &gateway_boundaries(),
            )
            .is_ok()
        );
    }

    #[test]
    fn rejects_gateway_running_as_a_storage_owner() {
        assert!(matches!(
            validate_gateway_identity(
                &process(61_003, 62_001, &[62_001, 62_004]),
                61_001,
                &gateway_boundaries(),
            ),
            Err(RuntimeIdentityError::UnsafeProcessUser { .. })
        ));
    }

    #[test]
    fn rejects_gateway_with_writable_reader_storage() {
        let mut boundaries = gateway_boundaries();
        boundaries.content.mode = 0o2770;
        assert!(matches!(
            validate_gateway_identity(
                &process(61_001, 62_001, &[62_001, 62_004]),
                61_001,
                &boundaries,
            ),
            Err(RuntimeIdentityError::BoundaryModeMismatch { .. })
        ));
    }

    #[test]
    fn rejects_gateway_without_the_ingress_group() {
        assert!(matches!(
            validate_gateway_identity(
                &process(61_001, 62_001, &[62_001]),
                61_001,
                &gateway_boundaries(),
            ),
            Err(RuntimeIdentityError::GroupSetMismatch { .. })
        ));
    }

    #[test]
    fn rejects_gateway_running_as_an_unconfigured_user() {
        assert!(matches!(
            validate_gateway_identity(
                &process(61_099, 62_001, &[62_001, 62_004]),
                61_001,
                &gateway_boundaries(),
            ),
            Err(RuntimeIdentityError::ProcessUserMismatch { .. })
        ));
    }
}
