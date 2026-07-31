use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

#[cfg(unix)]
use nix::errno::Errno;
#[cfg(unix)]
use nix::sys::signal::{Signal, killpg};
#[cfg(unix)]
use nix::unistd::Pid;
#[cfg(unix)]
use std::os::unix::process::CommandExt;

const POLL_INTERVAL: Duration = Duration::from_millis(10);

/// A bounded invocation of the configured Quartz CLI.
#[derive(Clone, Debug)]
pub struct QuartzBuilder {
    configured_program: PathBuf,
    program: PathBuf,
    program_handle: Arc<File>,
    configured_integration_directory: PathBuf,
    integration_directory: PathBuf,
    integration_handle: Arc<File>,
    prefix_arguments: Vec<OsString>,
    timeout: Duration,
}

impl QuartzBuilder {
    /// Validates and pins operator-controlled Quartz command configuration.
    ///
    /// `prefix_arguments` typically contains `quartz` when `program` is an
    /// absolute `npx` path. The builder appends `build -d <content> -o
    /// <output>` without invoking a shell.
    pub fn new(
        program: impl AsRef<Path>,
        integration_directory: impl AsRef<Path>,
        prefix_arguments: Vec<OsString>,
        timeout: Duration,
    ) -> Result<Self, QuartzBuildError> {
        if timeout.is_zero() {
            return Err(QuartzBuildError::InvalidTimeout);
        }
        Instant::now()
            .checked_add(timeout)
            .ok_or(QuartzBuildError::InvalidTimeout)?;
        let configured_program = canonical_regular_file(program.as_ref())?;
        let program_handle =
            Arc::new(File::open(&configured_program).map_err(QuartzBuildError::Io)?);
        let program = stable_file_path(&program_handle, &configured_program)?;
        let configured_integration_directory = canonical_directory(integration_directory.as_ref())?;
        let integration_handle =
            Arc::new(File::open(&configured_integration_directory).map_err(QuartzBuildError::Io)?);
        let integration_directory =
            stable_file_path(&integration_handle, &configured_integration_directory)?;
        Ok(Self {
            configured_program,
            program,
            program_handle,
            configured_integration_directory,
            integration_directory,
            integration_handle,
            prefix_arguments,
            timeout,
        })
    }

    /// Builds one content tree into an existing empty output directory.
    pub fn build(&self, content: &Path, output: &Path) -> Result<(), QuartzBuildError> {
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
        let deadline = Instant::now()
            .checked_add(self.timeout)
            .ok_or(QuartzBuildError::InvalidTimeout)?;

        let mut command = Command::new(&self.program);
        command
            .current_dir(&self.integration_directory)
            .args(&self.prefix_arguments)
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
        let child = command.spawn().map_err(QuartzBuildError::Io)?;
        let mut child = ChildGuard::new(child)?;
        let status = loop {
            if let Some(status) = child.try_wait()? {
                break status;
            }
            if Instant::now() >= deadline {
                child.terminate();
                return Err(QuartzBuildError::TimedOut {
                    timeout: self.timeout,
                });
            }
            thread::sleep(POLL_INTERVAL.min(self.timeout));
        };
        child.terminate_group()?;
        child.disarm();
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

struct ChildGuard {
    child: Option<Child>,
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
            #[cfg(unix)]
            process_group,
        })
    }

    fn try_wait(&mut self) -> Result<Option<ExitStatus>, QuartzBuildError> {
        self.child
            .as_mut()
            .ok_or(QuartzBuildError::InvalidProcessState)?
            .try_wait()
            .map_err(QuartzBuildError::Io)
    }

    fn terminate_group(&self) -> Result<(), QuartzBuildError> {
        #[cfg(unix)]
        match killpg(self.process_group, Signal::SIGKILL) {
            Ok(()) | Err(Errno::ESRCH) => Ok(()),
            Err(error) => Err(QuartzBuildError::Io(io::Error::from_raw_os_error(
                error as i32,
            ))),
        }
        #[cfg(not(unix))]
        Ok(())
    }

    fn terminate(&mut self) {
        let _ = self.terminate_group();
        if let Some(child) = self.child.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
        self.child = None;
    }

    fn disarm(&mut self) {
        self.child = None;
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        self.terminate();
    }
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

/// Quartz process or output validation failure.
#[derive(Debug)]
pub enum QuartzBuildError {
    ProgramMustBeAbsolute,
    InvalidProgram,
    InvalidDirectory,
    BuildPathsMustBeAbsolute,
    InvalidTimeout,
    InvalidProcessState,
    CommandIdentityChanged,
    OverlappingPaths,
    OutputNotEmpty,
    OutputEmpty,
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
            Self::InvalidProcessState => formatter.write_str("Quartz process state is invalid"),
            Self::CommandIdentityChanged => formatter.write_str("Quartz command identity changed"),
            Self::OverlappingPaths => {
                formatter.write_str("Quartz content and output paths overlap")
            }
            Self::OutputNotEmpty => formatter.write_str("Quartz output directory is not empty"),
            Self::OutputEmpty => formatter.write_str("Quartz produced an empty output directory"),
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
