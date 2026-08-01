use std::ffi::OsStr;
use std::fmt;
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

use agent_knowledge_core::{PathAttestation, PathAttestationError};

use crate::store::{BuildDirectory, BuiltDirectory, MAXIMUM_RELEASE_TREE_DEPTH, ReleasePolicy};

#[cfg(unix)]
use nix::errno::Errno;
#[cfg(unix)]
use nix::sys::signal::{Signal, killpg};
#[cfg(target_os = "linux")]
use nix::sys::wait::{WaitPidFlag, WaitStatus, waitpid};
#[cfg(unix)]
use nix::unistd::Pid;
#[cfg(unix)]
use std::fs::OpenOptions;
#[cfg(unix)]
use std::os::unix::process::CommandExt;

const POLL_INTERVAL: Duration = Duration::from_millis(10);
#[cfg(unix)]
const TERMINATION_GRACE: Duration = Duration::from_secs(1);
static BUILD_PROCESS_LEASE: Mutex<()> = Mutex::new(());

/// A bounded invocation of the configured Quartz CLI.
#[derive(Clone, Debug)]
pub struct QuartzBuilder {
    configured_program: PathBuf,
    program_handle: Arc<File>,
    configured_integration_directory: PathBuf,
    integration_directory: PathBuf,
    integration_handle: Arc<File>,
    timeout: Duration,
    output_policy: ReleasePolicy,
}

impl QuartzBuilder {
    /// Validates operator-controlled, trusted Quartz command configuration.
    ///
    /// The builder invokes the configured launcher as `program build -d
    /// <content> -o <output>` without a shell. The program and integration
    /// tree must be deployed immutably for the lifetime of this builder.
    pub fn new(
        program: impl AsRef<Path>,
        integration_directory: impl AsRef<Path>,
        timeout: Duration,
    ) -> Result<Self, QuartzBuildError> {
        Self::new_with_policy(
            program,
            integration_directory,
            timeout,
            ReleasePolicy::default(),
        )
    }

    /// Creates a builder with explicit live output limits.
    pub fn new_with_policy(
        program: impl AsRef<Path>,
        integration_directory: impl AsRef<Path>,
        timeout: Duration,
        output_policy: ReleasePolicy,
    ) -> Result<Self, QuartzBuildError> {
        if timeout.is_zero() {
            return Err(QuartzBuildError::InvalidTimeout);
        }
        Instant::now()
            .checked_add(timeout)
            .ok_or(QuartzBuildError::InvalidTimeout)?;
        let output_policy = output_policy
            .validate()
            .map_err(|_| QuartzBuildError::InvalidOutputLimits)?;
        let configured_program = canonical_regular_file(program.as_ref())?;
        let program_handle =
            Arc::new(File::open(&configured_program).map_err(QuartzBuildError::Io)?);
        let configured_integration_directory = canonical_directory(integration_directory.as_ref())?;
        let integration_handle =
            Arc::new(File::open(&configured_integration_directory).map_err(QuartzBuildError::Io)?);
        let integration_directory =
            stable_file_path(&integration_handle, &configured_integration_directory)?;
        Ok(Self {
            configured_program,
            program_handle,
            configured_integration_directory,
            integration_directory,
            integration_handle,
            timeout,
            output_policy,
        })
    }

    /// Attests the launcher and integration root selected and pinned at open.
    ///
    /// # Errors
    ///
    /// Returns an error when a configured path no longer names its pinned
    /// object or its ancestry cannot be inspected.
    pub fn trusted_attestations(&self) -> Result<[PathAttestation; 2], PathAttestationError> {
        Ok([
            PathAttestation::capture(&self.configured_program, &self.program_handle)?,
            PathAttestation::capture(
                &self.configured_integration_directory,
                &self.integration_handle,
            )?,
        ])
    }

    /// Consumes a staging directory and returns it only after a successful,
    /// fully validated Quartz build.
    pub fn build(
        &self,
        content: &Path,
        build: BuildDirectory,
    ) -> Result<BuiltDirectory, QuartzBuildError> {
        self.build_path(content, build.path())?;
        Ok(BuiltDirectory::new(build))
    }

    fn build_path(&self, content: &Path, output: &Path) -> Result<(), QuartzBuildError> {
        if !content.is_absolute() || !output.is_absolute() {
            return Err(QuartzBuildError::BuildPathsMustBeAbsolute);
        }
        ensure_directory_target(content)?;
        ensure_empty_directory(output)?;
        let canonical_content = fs::canonicalize(content).map_err(QuartzBuildError::Io)?;
        let canonical_output = fs::canonicalize(output).map_err(QuartzBuildError::Io)?;
        if canonical_content.starts_with(&canonical_output)
            || canonical_output.starts_with(&canonical_content)
        {
            return Err(QuartzBuildError::OverlappingPaths);
        }
        self.validate_live_command()?;
        let _process_lease = BuildProcessLease::acquire()?;
        let deadline = Instant::now()
            .checked_add(self.timeout)
            .ok_or(QuartzBuildError::InvalidTimeout)?;

        let mut command = Command::new(&self.configured_program);
        command
            .current_dir(&self.integration_directory)
            .arg(OsStr::new("build"))
            .arg(OsStr::new("-d"))
            .arg(content)
            .arg(OsStr::new("-o"))
            .arg(output)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        #[cfg(unix)]
        command.process_group(0);
        let child = spawn_quartz(&mut command, deadline, self.timeout)?;
        let mut child = ChildGuard::new(child)?;
        let status = loop {
            if let Some(status) = child.try_wait()? {
                child.terminate_group_and_wait(deadline, self.timeout)?;
                enforce_output_limits(output, self.output_policy, deadline, self.timeout)?;
                break status;
            }
            enforce_output_limits(output, self.output_policy, deadline, self.timeout)?;
            if Instant::now() >= deadline {
                child.terminate();
                return Err(QuartzBuildError::TimedOut {
                    timeout: self.timeout,
                });
            }
            thread::sleep(POLL_INTERVAL.min(self.timeout));
        };
        if !status.success() {
            return Err(QuartzBuildError::CommandFailed { status });
        }
        ensure_nonempty_directory(output)?;
        self.validate_live_command()
    }

    fn validate_live_command(&self) -> Result<(), QuartzBuildError> {
        validate_pinned_file(&self.configured_program, &self.program_handle)?;
        validate_pinned_directory(
            &self.configured_integration_directory,
            &self.integration_handle,
        )
    }
}

fn spawn_quartz(
    command: &mut Command,
    deadline: Instant,
    timeout: Duration,
) -> Result<Child, QuartzBuildError> {
    check_build_deadline(deadline, timeout)?;
    command.spawn().map_err(QuartzBuildError::Io)
}

struct ChildGuard {
    child: Option<Child>,
    group_signal_armed: bool,
    group_wait_armed: bool,
    #[cfg(unix)]
    process_group: Pid,
}

impl ChildGuard {
    fn new(mut child: Child) -> Result<Self, QuartzBuildError> {
        #[cfg(unix)]
        let process_group = match child.id().try_into() {
            Ok(identifier) => Pid::from_raw(identifier),
            Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(QuartzBuildError::InvalidProcessState);
            }
        };
        Ok(Self {
            child: Some(child),
            group_signal_armed: true,
            group_wait_armed: true,
            #[cfg(unix)]
            process_group,
        })
    }

    fn try_wait(&mut self) -> Result<Option<ExitStatus>, QuartzBuildError> {
        let status = self
            .child
            .as_mut()
            .ok_or(QuartzBuildError::InvalidProcessState)?
            .try_wait()
            .map_err(QuartzBuildError::Io)?;
        if status.is_some() {
            self.child = None;
        }
        Ok(status)
    }

    fn terminate_group(&mut self) -> Result<(), QuartzBuildError> {
        if !self.group_signal_armed {
            return Ok(());
        }
        #[cfg(unix)]
        let result = match killpg(self.process_group, Signal::SIGKILL) {
            Ok(()) | Err(Errno::ESRCH) => Ok(()),
            Err(error) => Err(process_error(error as i32)),
        };
        #[cfg(not(unix))]
        let result: Result<(), QuartzBuildError> = Ok(());
        if result.is_ok() {
            self.group_signal_armed = false;
        }
        result
    }

    fn terminate_group_and_wait(
        &mut self,
        deadline: Instant,
        timeout: Duration,
    ) -> Result<(), QuartzBuildError> {
        self.terminate_group()?;
        #[cfg(not(unix))]
        {
            let _ = (deadline, timeout);
            self.disarm();
            Ok(())
        }
        #[cfg(unix)]
        self.wait_for_group_then_disarm(deadline, timeout, wait_for_process_group)
    }

    #[cfg(unix)]
    fn wait_for_group_then_disarm(
        &mut self,
        deadline: Instant,
        timeout: Duration,
        wait: impl FnOnce(Pid, Instant, Duration) -> Result<(), QuartzBuildError>,
    ) -> Result<(), QuartzBuildError> {
        wait(self.process_group, deadline, timeout)?;
        self.disarm();
        Ok(())
    }

    fn terminate(&mut self) {
        if !self.group_signal_armed && !self.group_wait_armed && self.child.is_none() {
            return;
        }
        #[cfg(unix)]
        let process_group = self.process_group;
        let _ = self.terminate_group();
        self.group_signal_armed = false;
        if let Some(child) = self.child.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
        self.child = None;
        #[cfg(unix)]
        if self.group_wait_armed {
            let deadline = Instant::now()
                .checked_add(TERMINATION_GRACE)
                .unwrap_or_else(Instant::now);
            let _ = wait_for_process_group(process_group, deadline, TERMINATION_GRACE);
        }
        self.group_wait_armed = false;
    }

    fn disarm(&mut self) {
        self.group_signal_armed = false;
        self.group_wait_armed = false;
        self.child = None;
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        self.terminate();
    }
}

struct BuildProcessLease {
    _lock: MutexGuard<'static, ()>,
    #[cfg(target_os = "linux")]
    previous_subreaper: bool,
}

impl BuildProcessLease {
    fn acquire() -> Result<Self, QuartzBuildError> {
        let lock = BUILD_PROCESS_LEASE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        #[cfg(target_os = "linux")]
        {
            let previous_subreaper = nix::sys::prctl::get_child_subreaper()
                .map_err(|error| process_error(error as i32))?;
            if !previous_subreaper {
                nix::sys::prctl::set_child_subreaper(true)
                    .map_err(|error| process_error(error as i32))?;
            }
            Ok(Self {
                _lock: lock,
                previous_subreaper,
            })
        }
        #[cfg(not(target_os = "linux"))]
        Ok(Self { _lock: lock })
    }
}

impl Drop for BuildProcessLease {
    fn drop(&mut self) {
        #[cfg(target_os = "linux")]
        if !self.previous_subreaper {
            let _ = nix::sys::prctl::set_child_subreaper(false);
        }
    }
}

#[cfg(unix)]
fn wait_for_process_group(
    process_group: Pid,
    deadline: Instant,
    timeout: Duration,
) -> Result<(), QuartzBuildError> {
    loop {
        reap_process_group_children(process_group)?;
        match killpg(process_group, None) {
            Err(Errno::ESRCH) => return Ok(()),
            Ok(()) => {}
            Err(error) => return Err(process_error(error as i32)),
        }
        check_build_deadline(deadline, timeout)?;
        thread::sleep(POLL_INTERVAL.min(deadline.saturating_duration_since(Instant::now())));
    }
}

#[cfg(target_os = "linux")]
fn reap_process_group_children(process_group: Pid) -> Result<(), QuartzBuildError> {
    let group_selector = Pid::from_raw(-process_group.as_raw());
    loop {
        match waitpid(group_selector, Some(WaitPidFlag::WNOHANG)) {
            Ok(WaitStatus::StillAlive) | Err(Errno::ECHILD) => return Ok(()),
            Ok(_) | Err(Errno::EINTR) => {}
            Err(error) => return Err(process_error(error as i32)),
        }
    }
}

#[cfg(all(unix, not(target_os = "linux")))]
fn reap_process_group_children(_process_group: Pid) -> Result<(), QuartzBuildError> {
    Ok(())
}

fn process_error(error: i32) -> QuartzBuildError {
    QuartzBuildError::Io(io::Error::from_raw_os_error(error))
}

fn canonical_regular_file(path: &Path) -> Result<PathBuf, QuartzBuildError> {
    if !path.is_absolute() {
        return Err(QuartzBuildError::ProgramMustBeAbsolute);
    }
    let path = fs::canonicalize(path).map_err(QuartzBuildError::Io)?;
    let metadata = fs::symlink_metadata(&path).map_err(QuartzBuildError::Io)?;
    if metadata.file_type().is_file() {
        Ok(path)
    } else {
        Err(QuartzBuildError::InvalidProgram)
    }
}

fn canonical_directory(path: &Path) -> Result<PathBuf, QuartzBuildError> {
    let path = fs::canonicalize(path).map_err(QuartzBuildError::Io)?;
    let metadata = fs::symlink_metadata(&path).map_err(QuartzBuildError::Io)?;
    if metadata.file_type().is_dir() {
        Ok(path)
    } else {
        Err(QuartzBuildError::InvalidDirectory)
    }
}

fn stable_file_path(handle: &File, configured: &Path) -> Result<PathBuf, QuartzBuildError> {
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        let _ = configured;
        let path = PathBuf::from(format!(
            "/proc/{}/fd/{}",
            std::process::id(),
            handle.as_raw_fd()
        ));
        fs::metadata(&path).map_err(QuartzBuildError::Io)?;
        Ok(path)
    }
    #[cfg(not(target_os = "linux"))]
    {
        Ok(configured.into())
    }
}

fn validate_pinned_file(path: &Path, pinned: &File) -> Result<(), QuartzBuildError> {
    let configured =
        fs::symlink_metadata(path).map_err(|_| QuartzBuildError::CommandIdentityChanged)?;
    let pinned_metadata = pinned.metadata().map_err(QuartzBuildError::Io)?;
    if !configured.file_type().is_file() || !same_metadata(&configured, &pinned_metadata) {
        return Err(QuartzBuildError::CommandIdentityChanged);
    }
    validate_live_mount(path, pinned)
}

fn validate_pinned_directory(path: &Path, pinned: &File) -> Result<(), QuartzBuildError> {
    let configured =
        fs::symlink_metadata(path).map_err(|_| QuartzBuildError::CommandIdentityChanged)?;
    let pinned_metadata = pinned.metadata().map_err(QuartzBuildError::Io)?;
    if !configured.file_type().is_dir() || !same_metadata(&configured, &pinned_metadata) {
        return Err(QuartzBuildError::CommandIdentityChanged);
    }
    validate_live_mount(path, pinned)
}

#[cfg(target_os = "linux")]
fn validate_live_mount(path: &Path, pinned: &File) -> Result<(), QuartzBuildError> {
    let live = File::open(path).map_err(|_| QuartzBuildError::CommandIdentityChanged)?;
    if !same_metadata(
        &live.metadata().map_err(QuartzBuildError::Io)?,
        &pinned.metadata().map_err(QuartzBuildError::Io)?,
    ) || mount_id(&live)? != mount_id(pinned)?
    {
        return Err(QuartzBuildError::CommandIdentityChanged);
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn validate_live_mount(_path: &Path, _pinned: &File) -> Result<(), QuartzBuildError> {
    Ok(())
}

#[cfg(unix)]
fn same_metadata(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt;
    left.dev() == right.dev() && left.ino() == right.ino()
}

#[cfg(not(unix))]
fn same_metadata(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    left.file_type() == right.file_type() && left.len() == right.len()
}

#[cfg(target_os = "linux")]
fn mount_id(file: &File) -> Result<u64, QuartzBuildError> {
    use std::io::Read;
    use std::os::fd::AsRawFd;
    const MAXIMUM_FDINFO_BYTES: u64 = 16 * 1024;
    let mut bytes = Vec::with_capacity(MAXIMUM_FDINFO_BYTES as usize);
    File::open(format!("/proc/self/fdinfo/{}", file.as_raw_fd()))
        .and_then(|file| file.take(MAXIMUM_FDINFO_BYTES + 1).read_to_end(&mut bytes))
        .map_err(QuartzBuildError::Io)?;
    if bytes.len() as u64 > MAXIMUM_FDINFO_BYTES {
        return Err(QuartzBuildError::CommandIdentityChanged);
    }
    let contents =
        std::str::from_utf8(&bytes).map_err(|_| QuartzBuildError::CommandIdentityChanged)?;
    contents
        .lines()
        .find_map(|line| line.strip_prefix("mnt_id:").map(str::trim))
        .ok_or(QuartzBuildError::CommandIdentityChanged)?
        .parse()
        .map_err(|_| QuartzBuildError::CommandIdentityChanged)
}

fn ensure_directory_target(path: &Path) -> Result<(), QuartzBuildError> {
    let metadata = fs::metadata(path).map_err(QuartzBuildError::Io)?;
    if metadata.is_dir() {
        Ok(())
    } else {
        Err(QuartzBuildError::InvalidDirectory)
    }
}

fn ensure_empty_directory(path: &Path) -> Result<(), QuartzBuildError> {
    let metadata = fs::symlink_metadata(path).map_err(QuartzBuildError::Io)?;
    if !metadata.file_type().is_dir() {
        return Err(QuartzBuildError::InvalidDirectory);
    }
    if fs::read_dir(path)
        .map_err(QuartzBuildError::Io)?
        .next()
        .transpose()
        .map_err(QuartzBuildError::Io)?
        .is_some()
    {
        return Err(QuartzBuildError::OutputNotEmpty);
    }
    Ok(())
}

fn ensure_nonempty_directory(path: &Path) -> Result<(), QuartzBuildError> {
    if fs::read_dir(path)
        .map_err(QuartzBuildError::Io)?
        .next()
        .transpose()
        .map_err(QuartzBuildError::Io)?
        .is_some()
    {
        Ok(())
    } else {
        Err(QuartzBuildError::OutputEmpty)
    }
}

fn enforce_output_limits(
    output: &Path,
    policy: ReleasePolicy,
    deadline: Instant,
    timeout: Duration,
) -> Result<(), QuartzBuildError> {
    check_build_deadline(deadline, timeout)?;
    let listed = match fs::symlink_metadata(output) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(QuartzBuildError::Io(error)),
    };
    if !listed.file_type().is_dir() {
        return Err(QuartzBuildError::OutputLimitExceeded);
    }
    let root = match open_scan_directory(output) {
        Ok(root) => root,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(QuartzBuildError::Io(error)),
    };
    if !same_metadata(&listed, &root.metadata().map_err(QuartzBuildError::Io)?) {
        return Ok(());
    }

    let mut usage = OutputUsage::default();
    scan_output_directory(&root, output, 0, policy, deadline, timeout, &mut usage)
}

#[derive(Default)]
struct OutputUsage {
    entries: u64,
    total_bytes: u64,
}

fn scan_output_directory(
    directory: &File,
    configured: &Path,
    depth: usize,
    policy: ReleasePolicy,
    deadline: Instant,
    timeout: Duration,
    usage: &mut OutputUsage,
) -> Result<(), QuartzBuildError> {
    check_build_deadline(deadline, timeout)?;
    let stable = stable_file_path(directory, configured)?;
    let mut children = Vec::new();
    for entry in fs::read_dir(&stable).map_err(QuartzBuildError::Io)? {
        check_build_deadline(deadline, timeout)?;
        usage.entries = usage
            .entries
            .checked_add(1)
            .ok_or(QuartzBuildError::OutputLimitExceeded)?;
        if usage.entries > policy.maximum_entries {
            return Err(QuartzBuildError::OutputLimitExceeded);
        }
        children.push(entry.map_err(QuartzBuildError::Io)?.path());
    }
    for path in children {
        check_build_deadline(deadline, timeout)?;
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(QuartzBuildError::Io(error)),
        };
        if metadata.file_type().is_dir() {
            if depth == MAXIMUM_RELEASE_TREE_DEPTH {
                return Err(QuartzBuildError::OutputLimitExceeded);
            }
            let child = match open_scan_directory(&path) {
                Ok(child) => child,
                Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                Err(error) => return Err(QuartzBuildError::Io(error)),
            };
            if !same_metadata(&metadata, &child.metadata().map_err(QuartzBuildError::Io)?) {
                continue;
            }
            scan_output_directory(&child, &path, depth + 1, policy, deadline, timeout, usage)?;
        } else if metadata.file_type().is_file() {
            if metadata.len() > policy.maximum_file_bytes {
                return Err(QuartzBuildError::OutputLimitExceeded);
            }
            usage.total_bytes = usage
                .total_bytes
                .checked_add(metadata.len())
                .ok_or(QuartzBuildError::OutputLimitExceeded)?;
            if usage.total_bytes > policy.maximum_total_bytes {
                return Err(QuartzBuildError::OutputLimitExceeded);
            }
        } else {
            return Err(QuartzBuildError::OutputLimitExceeded);
        }
    }
    Ok(())
}

fn check_build_deadline(deadline: Instant, timeout: Duration) -> Result<(), QuartzBuildError> {
    if Instant::now() >= deadline {
        Err(QuartzBuildError::TimedOut { timeout })
    } else {
        Ok(())
    }
}

#[cfg(unix)]
fn open_scan_directory(path: &Path) -> io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt;

    OpenOptions::new()
        .read(true)
        .custom_flags(nix::libc::O_CLOEXEC | nix::libc::O_DIRECTORY | nix::libc::O_NOFOLLOW)
        .open(path)
}

#[cfg(not(unix))]
fn open_scan_directory(path: &Path) -> io::Result<File> {
    File::open(path)
}

/// Quartz process or output validation failure.
#[derive(Debug)]
pub enum QuartzBuildError {
    ProgramMustBeAbsolute,
    InvalidProgram,
    InvalidDirectory,
    BuildPathsMustBeAbsolute,
    InvalidTimeout,
    InvalidOutputLimits,
    InvalidProcessState,
    CommandIdentityChanged,
    OverlappingPaths,
    OutputNotEmpty,
    OutputEmpty,
    OutputLimitExceeded,
    TimedOut { timeout: Duration },
    CommandFailed { status: ExitStatus },
    Io(io::Error),
}

impl fmt::Display for QuartzBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ProgramMustBeAbsolute => {
                formatter.write_str("Quartz program path must be absolute")
            }
            Self::InvalidProgram => formatter.write_str("Quartz program is not a regular file"),
            Self::InvalidDirectory => formatter.write_str("Quartz path is not a real directory"),
            Self::BuildPathsMustBeAbsolute => {
                formatter.write_str("Quartz content and output paths must be absolute")
            }
            Self::InvalidTimeout => formatter.write_str("Quartz timeout must be positive"),
            Self::InvalidOutputLimits => formatter.write_str("Quartz output limits are invalid"),
            Self::InvalidProcessState => formatter.write_str("Quartz process state is invalid"),
            Self::CommandIdentityChanged => formatter.write_str("Quartz command identity changed"),
            Self::OverlappingPaths => {
                formatter.write_str("Quartz content and output paths overlap")
            }
            Self::OutputNotEmpty => formatter.write_str("Quartz output directory is not empty"),
            Self::OutputEmpty => formatter.write_str("Quartz produced an empty output directory"),
            Self::OutputLimitExceeded => {
                formatter.write_str("Quartz output exceeded live build limits")
            }
            Self::TimedOut { timeout } => {
                write!(formatter, "Quartz build exceeded {timeout:?}")
            }
            Self::CommandFailed { status } => {
                write!(formatter, "Quartz build failed with {status}")
            }
            Self::Io(error) => write!(formatter, "Quartz build I/O failed: {error}"),
        }
    }
}

impl std::error::Error for QuartzBuildError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests;
