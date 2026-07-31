use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::thread;
use std::time::{Duration, Instant};

const POLL_INTERVAL: Duration = Duration::from_millis(10);

/// A bounded invocation of the configured Quartz CLI.
#[derive(Clone, Debug)]
pub struct QuartzBuilder {
    program: PathBuf,
    integration_directory: PathBuf,
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
        let program = canonical_regular_file(program.as_ref())?;
        let integration_directory = canonical_directory(integration_directory.as_ref())?;
        Ok(Self {
            program,
            integration_directory,
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
        let mut child = command.spawn().map_err(QuartzBuildError::Io)?;
        let deadline = Instant::now()
            .checked_add(self.timeout)
            .ok_or(QuartzBuildError::InvalidTimeout)?;
        let status = loop {
            if let Some(status) = child.try_wait().map_err(QuartzBuildError::Io)? {
                break status;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                return Err(QuartzBuildError::TimedOut {
                    timeout: self.timeout,
                });
            }
            thread::sleep(POLL_INTERVAL.min(self.timeout));
        };
        if !status.success() {
            return Err(QuartzBuildError::CommandFailed { status });
        }
        ensure_nonempty_directory(output)
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
