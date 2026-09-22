//! Best-effort public release checks. No knowledge or SSH context enters this module.
use std::fs::{self, File, OpenOptions};
use std::io::{self, IsTerminal, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

mod fetch;
#[cfg(test)]
mod tests;

pub(crate) const TTL_SECONDS: u64 = 24 * 60 * 60;
const MAXIMUM_CACHE_BYTES: u64 = 16 * 1024;
const CACHE_NAME: &str = "stable-release-v1.json";

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Policy {
    Auto,
    On,
    Off,
    Invalid,
}

#[derive(Clone, Debug)]
pub(crate) struct Config {
    policy: Policy,
    directory: Option<PathBuf>,
}

impl Config {
    pub(crate) fn from_env() -> Self {
        Self::from_values(
            std::env::var_os("AGENT_KNOWLEDGE_UPDATE_CHECK"),
            std::env::var_os("AGENT_KNOWLEDGE_CACHE_DIR"),
            std::env::var_os("XDG_CACHE_HOME"),
            std::env::var_os("HOME"),
        )
    }

    fn from_values(
        policy: Option<std::ffi::OsString>,
        directory: Option<std::ffi::OsString>,
        xdg: Option<std::ffi::OsString>,
        home: Option<std::ffi::OsString>,
    ) -> Self {
        let policy = match policy.as_deref().and_then(std::ffi::OsStr::to_str) {
            None if policy.is_none() => Policy::Auto,
            Some("auto") => Policy::Auto,
            Some("on") => Policy::On,
            Some("off") => Policy::Off,
            _ => Policy::Invalid,
        };
        let absolute = |value: std::ffi::OsString| {
            let path = PathBuf::from(value);
            path.is_absolute().then_some(path)
        };
        let directory = match directory {
            Some(path) => absolute(path),
            None => xdg
                .and_then(absolute)
                .map(|path| path.join("agent-knowledge"))
                .or_else(|| {
                    home.and_then(absolute)
                        .map(|path| path.join(".cache/agent-knowledge"))
                }),
        };
        Self { policy, directory }
    }

    fn enabled(&self) -> bool {
        matches!(self.policy, Policy::Auto | Policy::On)
    }

    fn automatic(&self, interactive: bool) -> bool {
        self.policy == Policy::On || (self.policy == Policy::Auto && interactive)
    }
}

/// A validated stable release from the fixed public upstream repository.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Release {
    /// Stable semantic version without a leading v.
    pub version: String,
    /// Public GitHub release page, restricted to the upstream repository.
    pub url: String,
}

impl Release {
    fn valid(&self) -> bool {
        stable_version(&self.version).is_some()
            && [
                format!(
                    "https://github.com/neodymium6/agent-knowledge/releases/tag/v{}",
                    self.version
                ),
                format!(
                    "https://github.com/neodymium6/agent-knowledge/releases/tag/{}",
                    self.version
                ),
            ]
            .contains(&self.url)
    }
}

fn stable_version(text: &str) -> Option<semver::Version> {
    if text.len() > 128 {
        return None;
    }
    semver::Version::parse(text)
        .ok()
        .filter(|version| version.pre.is_empty())
}

/// Public, non-sensitive failure classes. Raw network diagnostics are never persisted.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckError {
    /// An attempt was reserved but has not completed, or its process was interrupted.
    Incomplete,
    /// Network, TLS, or the bounded request deadline failed.
    Network,
    /// The public API refused a request because of rate limiting.
    RateLimited,
    /// Upstream returned another unsuccessful HTTP response.
    Http,
    /// No stable release was available at the upstream endpoint.
    NoStableRelease,
    /// Invalid, oversized, or unexpected release metadata.
    InvalidResponse,
    /// Local cache is missing a usable location or cannot be read/written.
    CacheUnavailable,
    /// Local cache is malformed or uses an unknown schema.
    CacheInvalid,
    /// Another process is updating the cache.
    CacheBusy,
}

/// Structured release awareness shared by CLI and MCP.
#[derive(Clone, Debug, Serialize)]
pub struct UpdateStatus {
    /// available, unknown, unavailable, or disabled.
    pub status: &'static str,
    /// Effective update-check policy, including invalid configuration.
    pub(crate) policy: Policy,
    /// Last successfully observed stable release, even after a later failed check.
    pub latest: Option<Release>,
    /// Unix UTC seconds of the last successful upstream check.
    pub checked_at: Option<u64>,
    /// Unix UTC seconds of the last attempted upstream check.
    pub last_attempt_at: Option<u64>,
    /// Whether latest metadata came from persistent cache in this invocation.
    pub cached: bool,
    /// Whether metadata is older than 24 hours or a later attempt failed.
    pub stale: bool,
    /// Explicit failure classification, without private environment details.
    pub error: Option<CheckError>,
}

impl UpdateStatus {
    fn empty(config: &Config, error: Option<CheckError>) -> Self {
        Self {
            status: if !config.enabled() {
                "disabled"
            } else if error.is_some() {
                "unavailable"
            } else {
                "unknown"
            },
            policy: config.policy,
            latest: None,
            checked_at: None,
            last_attempt_at: None,
            cached: false,
            stale: false,
            error,
        }
    }

    /// Compares a release independently of wire protocol compatibility.
    #[must_use]
    pub fn newer_than(&self, installed: &str) -> Option<bool> {
        let latest = stable_version(&self.latest.as_ref()?.version)?;
        let installed = semver::Version::parse(installed).ok()?;
        Some(latest.cmp_precedence(&installed).is_gt())
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Cache {
    schema_version: u8,
    last_attempt_at: u64,
    latest: Option<Release>,
    checked_at: Option<u64>,
    last_error: Option<CheckError>,
    notified_version: Option<String>,
}

impl Cache {
    fn due(&self, now: u64) -> bool {
        now.saturating_sub(self.last_attempt_at) >= TTL_SECONDS
    }

    fn status(&self, config: &Config, now: u64, cached: bool) -> UpdateStatus {
        UpdateStatus {
            status: if self.last_error.is_some() {
                "unavailable"
            } else if self.latest.is_some() {
                "available"
            } else {
                "unknown"
            },
            policy: config.policy,
            latest: self.latest.clone(),
            checked_at: self.checked_at,
            last_attempt_at: Some(self.last_attempt_at),
            cached: cached && self.latest.is_some(),
            stale: self.latest.is_some()
                && (self.last_error.is_some()
                    || self
                        .checked_at
                        .is_none_or(|time| now.saturating_sub(time) >= TTL_SECONDS)),
            error: self.last_error,
        }
    }
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn read_cache(directory: &Path) -> Result<Option<Cache>, CheckError> {
    let file = match File::open(directory.join(CACHE_NAME)) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(CheckError::CacheUnavailable),
    };
    let mut bytes = Vec::new();
    file.take(MAXIMUM_CACHE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| CheckError::CacheUnavailable)?;
    if bytes.len() as u64 > MAXIMUM_CACHE_BYTES {
        return Err(CheckError::CacheInvalid);
    }
    let cache: Cache = serde_json::from_slice(&bytes).map_err(|_| CheckError::CacheInvalid)?;
    if cache.schema_version != 1
        || cache
            .latest
            .as_ref()
            .is_some_and(|release| !release.valid())
        || cache.latest.is_some() != cache.checked_at.is_some()
        || cache
            .notified_version
            .as_ref()
            .is_some_and(|version| stable_version(version).is_none())
    {
        return Err(CheckError::CacheInvalid);
    }
    Ok(Some(cache))
}

fn save_cache(directory: &Path, cache: &Cache) -> Result<(), CheckError> {
    let mut temp =
        tempfile::NamedTempFile::new_in(directory).map_err(|_| CheckError::CacheUnavailable)?;
    serde_json::to_writer(&mut temp, cache).map_err(|_| CheckError::CacheUnavailable)?;
    temp.flush().map_err(|_| CheckError::CacheUnavailable)?;
    temp.persist(directory.join(CACHE_NAME))
        .map_err(|_| CheckError::CacheUnavailable)?;
    Ok(())
}

struct CacheLock(File);

impl Drop for CacheLock {
    fn drop(&mut self) {
        // SSH can fork concurrently. Explicit unlock releases the shared lock even
        // while a child briefly retains the close-on-exec file descriptor.
        let _ = self.0.unlock();
    }
}

fn lock_cache(directory: &Path) -> Result<CacheLock, CheckError> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder
        .create(directory)
        .map_err(|_| CheckError::CacheUnavailable)?;
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options
        .open(directory.join("stable-release.lock"))
        .map_err(|_| CheckError::CacheUnavailable)?;
    file.try_lock().map_err(|_| CheckError::CacheBusy)?;
    Ok(CacheLock(file))
}

pub(crate) fn status(check: bool) -> UpdateStatus {
    check_with(&Config::from_env(), now(), check, fetch::latest)
}

fn check_with(
    config: &Config,
    time: u64,
    check: bool,
    fetch: impl FnOnce() -> Result<Release, CheckError>,
) -> UpdateStatus {
    if !config.enabled() {
        return UpdateStatus::empty(config, None);
    }
    match check_inner(config, time, check, fetch) {
        Ok(status) => status,
        Err(error) => UpdateStatus::empty(config, Some(error)),
    }
}

fn check_inner(
    config: &Config,
    time: u64,
    check: bool,
    fetch: impl FnOnce() -> Result<Release, CheckError>,
) -> Result<UpdateStatus, CheckError> {
    let directory = config
        .directory
        .as_deref()
        .ok_or(CheckError::CacheUnavailable)?;
    // Read-only observations neither create directories nor take a blocking lock.
    if !check {
        return Ok(read_cache(directory)?.map_or_else(
            || UpdateStatus::empty(config, None),
            |cache| cache.status(config, time, true),
        ));
    }
    let _lock = lock_cache(directory)?;
    let previous = read_cache(directory)?;
    if let Some(cache) = previous.as_ref().filter(|cache| !cache.due(time)) {
        return Ok(cache.status(config, time, true));
    }
    let mut cache = previous.unwrap_or(Cache {
        schema_version: 1,
        last_attempt_at: 0,
        latest: None,
        checked_at: None,
        last_error: None,
        notified_version: None,
    });
    // Reserve this attempt durably before networking, including failures and process crashes.
    cache.last_attempt_at = time;
    cache.last_error = Some(CheckError::Incomplete);
    save_cache(directory, &cache)?;
    let cached = match fetch() {
        Ok(release) if release.valid() => {
            cache.latest = Some(release);
            cache.checked_at = Some(time);
            cache.last_error = None;
            false
        }
        Ok(_) => {
            cache.last_error = Some(CheckError::InvalidResponse);
            true
        }
        Err(error) => {
            cache.last_error = Some(error);
            true
        }
    };
    save_cache(directory, &cache)?;
    Ok(cache.status(config, time, cached))
}

// Only called for ordinary CLI operations, never for an MCP server or explicit reports.
pub(crate) fn start_automatic() -> Option<Config> {
    let config = Config::from_env();
    if !config.automatic(io::stderr().is_terminal()) {
        return None;
    }
    let directory = config.directory.as_deref()?;
    if read_cache(directory)
        .ok()?
        .is_none_or(|cache| cache.due(now()))
    {
        spawn_refresh(directory);
    }
    Some(config)
}

fn spawn_refresh(directory: &Path) {
    let Ok(executable) = std::env::current_exe() else {
        return;
    };
    let mut command = Command::new(executable);
    // The full server binary routes client commands through a subcommand.
    if std::env::args_os()
        .nth(1)
        .is_some_and(|arg| arg == "client")
    {
        command.arg("client");
    }
    command
        .arg("__update-cache")
        .env_clear()
        .env("AGENT_KNOWLEDGE_UPDATE_CHECK", "on")
        .env("AGENT_KNOWLEDGE_CACHE_DIR", directory)
        .current_dir("/")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    // Preserve only public TLS trust configuration, never SSH or GitHub credentials.
    for key in ["SSL_CERT_FILE", "SSL_CERT_DIR"] {
        if let Some(value) = std::env::var_os(key) {
            command.env(key, value);
        }
    }
    if let Ok(mut child) = command.spawn() {
        let _ = std::thread::Builder::new()
            .name("release-check-reaper".to_owned())
            .spawn(move || {
                let _ = child.wait();
            });
    }
}

pub(crate) fn notify(config: &Config, output: impl Write) {
    let _ = notify_inner(config, now(), env!("CARGO_PKG_VERSION"), output);
}

fn notify_inner(
    config: &Config,
    time: u64,
    installed: &str,
    mut output: impl Write,
) -> Result<(), CheckError> {
    if !config.enabled() {
        return Ok(());
    }
    let directory = config
        .directory
        .as_deref()
        .ok_or(CheckError::CacheUnavailable)?;
    let _lock = lock_cache(directory)?;
    let Some(mut cache) = read_cache(directory)? else {
        return Ok(());
    };
    let status = cache.status(config, time, true);
    if status.stale || status.newer_than(installed) != Some(true) {
        return Ok(());
    }
    let Some(release) = cache.latest.clone() else {
        return Ok(());
    };
    if cache.notified_version.as_deref() == Some(&release.version) {
        return Ok(());
    }
    cache.notified_version = Some(release.version.clone());
    save_cache(directory, &cache)?;
    // Notification failures must never change a successful operation's exit status.
    let _ = writeln!(
        output,
        "A newer stable agent-knowledge client is available: {} (installed {}). {}",
        release.version, installed, release.url
    );
    Ok(())
}
