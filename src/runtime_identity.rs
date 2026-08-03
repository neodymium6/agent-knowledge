use std::collections::BTreeSet;
use std::fmt;
use std::fs;
use std::io;
use std::os::fd::AsRawFd;
#[cfg(target_os = "linux")]
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::path::{Path, PathBuf};

use agent_knowledge_gateway::GatewaySettings;
use agent_knowledge_worker::WorkerSettings;

#[cfg(target_os = "linux")]
use nix::sys::socket::{UnixAddr, getsockname};
#[cfg(target_os = "linux")]
use nix::unistd::{Gid, Uid, getgroups};

const WORKER_ROLE: &str = "Worker";
const QUEUE_INGRESS_ROLE: &str = "Queue Ingress";
const GATEWAY_ROLE: &str = "Gateway";
#[cfg(target_os = "linux")]
const ACTIVATED_SOCKET_MODE: u32 = 0o660;

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
}

impl DirectoryIdentity {
    #[cfg(target_os = "linux")]
    fn inspect(path: &Path) -> Result<Self, RuntimeIdentityError> {
        let metadata = fs::symlink_metadata(path).map_err(|source| {
            RuntimeIdentityError::BoundaryInspection {
                path: path.to_path_buf(),
                source,
            }
        })?;
        if !metadata.file_type().is_dir() {
            return Err(RuntimeIdentityError::InvalidBoundary(path.to_path_buf()));
        }
        Ok(Self {
            path: path.to_path_buf(),
            uid: metadata.uid(),
            gid: metadata.gid(),
        })
    }

    #[cfg(not(target_os = "linux"))]
    fn inspect(_path: &Path) -> Result<Self, RuntimeIdentityError> {
        Err(RuntimeIdentityError::UnsupportedPlatform)
    }

    #[cfg(test)]
    fn fictional(path: &str, uid: u32, gid: u32) -> Self {
        Self {
            path: PathBuf::from(path),
            uid,
            gid,
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
    let metadata = fs::symlink_metadata(expected_path).map_err(|source| {
        RuntimeIdentityError::SocketBoundaryInspection {
            path: expected_path.to_path_buf(),
            source,
        }
    })?;
    if !metadata.file_type().is_socket() {
        return Err(RuntimeIdentityError::InvalidSocketBoundary(
            expected_path.to_path_buf(),
        ));
    }
    let parent = expected_path
        .parent()
        .ok_or_else(|| RuntimeIdentityError::InvalidBoundary(expected_path.to_path_buf()))?;
    let runtime = DirectoryIdentity::inspect(parent)?;
    let mode = metadata.mode() & 0o7777;
    if metadata.uid() != runtime.uid
        || metadata.gid() != runtime.gid
        || mode != ACTIVATED_SOCKET_MODE
    {
        return Err(RuntimeIdentityError::InvalidSocketIdentity {
            path: expected_path.to_path_buf(),
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
}

fn validate_worker_identity(
    process: &ProcessIdentity,
    boundaries: &WorkerBoundaries,
) -> Result<(), RuntimeIdentityError> {
    require_matching_owner(
        WORKER_ROLE,
        &boundaries.repository,
        &[&boundaries.content, &boundaries.work, &boundaries.releases],
    )?;
    require_matching_group(WORKER_ROLE, &boundaries.repository, &[&boundaries.content])?;
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
    use std::fs;
    use std::os::fd::AsRawFd;
    use std::os::unix::fs::PermissionsExt;
    use std::os::unix::net::{UnixListener, UnixStream};

    use ulid::Ulid;

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

        drop(accepted);
        drop(client);
        drop(listener);
        fs::remove_file(&actual_path)
            .unwrap_or_else(|error| panic!("socket test path must be removed: {error}"));
        fs::remove_dir(&directory)
            .unwrap_or_else(|error| panic!("socket test directory must be removed: {error}"));
    }

    fn worker_boundaries() -> WorkerBoundaries {
        WorkerBoundaries {
            queue: DirectoryIdentity::fictional("/srv/fictional/queue", 61_002, 62_002),
            repository: DirectoryIdentity::fictional("/srv/fictional/repository", 61_003, 62_001),
            content: DirectoryIdentity::fictional("/srv/fictional/content", 61_003, 62_001),
            work: DirectoryIdentity::fictional("/srv/fictional/work", 61_003, 62_003),
            releases: DirectoryIdentity::fictional("/srv/fictional/releases", 61_003, 62_003),
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
        assert!(matches!(
            validate_worker_identity(&process(61_003, 62_003, &[62_002, 62_003]), &boundaries,),
            Err(RuntimeIdentityError::CollapsedBoundary { .. })
        ));
    }

    #[test]
    fn accepts_exact_queue_ingress_identity() {
        let queue = DirectoryIdentity::fictional("/srv/fictional/queue", 61_002, 62_002);
        let runtime = DirectoryIdentity::fictional("/run/fictional", 61_002, 62_004);
        assert!(
            validate_queue_ingress_identity(&process(61_002, 62_002, &[62_002]), &queue, &runtime,)
                .is_ok()
        );
    }

    #[test]
    fn rejects_queue_ingress_in_the_socket_client_group() {
        let queue = DirectoryIdentity::fictional("/srv/fictional/queue", 61_002, 62_002);
        let runtime = DirectoryIdentity::fictional("/run/fictional", 61_002, 62_004);
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
        let queue = DirectoryIdentity::fictional("/srv/fictional/queue", 61_002, 0);
        let runtime = DirectoryIdentity::fictional("/run/fictional", 61_002, 62_004);
        assert!(matches!(
            validate_queue_ingress_identity(&process(61_002, 0, &[0]), &queue, &runtime),
            Err(RuntimeIdentityError::CollapsedBoundary { .. })
        ));
    }

    fn gateway_boundaries() -> GatewayBoundaries {
        GatewayBoundaries {
            repository: DirectoryIdentity::fictional("/srv/fictional/repository", 61_003, 62_001),
            content: DirectoryIdentity::fictional("/srv/fictional/content", 61_003, 62_001),
            runtime: DirectoryIdentity::fictional("/run/fictional", 61_002, 62_004),
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
