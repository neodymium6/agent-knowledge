use super::*;
use std::cell::Cell;

fn fixture() -> (tempfile::TempDir, Config) {
    let temp = tempfile::tempdir().unwrap_or_else(|e| panic!("fixture: {e}"));
    let config = Config {
        policy: Policy::On,
        directory: Some(temp.path().join("cache")),
    };
    (temp, config)
}

fn release() -> Release {
    Release {
        version: "9.8.7".to_owned(),
        url: "https://github.com/neodymium6/agent-knowledge/releases/tag/v9.8.7".to_owned(),
    }
}

#[test]
fn success_failure_and_clock_rollback_share_the_persistent_ttl() {
    let (_temp, config) = fixture();
    let calls = Cell::new(0);
    let fetch = || {
        calls.set(calls.get() + 1);
        Ok(release())
    };
    let first = check_with(&config, 0, true, fetch);
    assert_eq!(first.status, "available");
    assert_eq!(first.checked_at, Some(0));
    assert!(!first.cached);
    let cached = check_with(&config, TTL_SECONDS - 1, true, fetch);
    assert!(cached.cached);
    assert_eq!(calls.get(), 1);
    let failed = check_with(&config, TTL_SECONDS, true, || {
        calls.set(calls.get() + 1);
        Err(CheckError::RateLimited)
    });
    assert_eq!(failed.status, "unavailable");
    assert_eq!(failed.error, Some(CheckError::RateLimited));
    assert_eq!(failed.checked_at, Some(0));
    assert!(failed.cached && failed.stale);
    assert_eq!(failed.last_attempt_at, Some(TTL_SECONDS));
    check_with(&config, TTL_SECONDS * 2 - 1, true, fetch);
    check_with(&config, 0, true, fetch);
    assert_eq!(calls.get(), 2);
    let refreshed = check_with(&config, TTL_SECONDS * 2, true, fetch);
    assert_eq!(refreshed.status, "available", "{refreshed:?}");
    assert!(!refreshed.cached && !refreshed.stale);
    assert_eq!(calls.get(), 3);
}

#[test]
fn disabled_invalid_config_and_cache_only_observations_never_fetch() {
    let (temp, mut config) = fixture();
    for policy in [Policy::Off, Policy::Invalid] {
        config.policy = policy;
        for check in [true, false] {
            let status = check_with(&config, 123, check, || panic!("disabled must not fetch"));
            assert_eq!(status.status, "disabled");
        }
    }
    config.policy = Policy::Auto;
    assert!(!config.automatic(false));
    assert!(config.automatic(true));
    assert_eq!(
        check_with(&config, 123, false, || panic!("cache only")).status,
        "unknown"
    );
    assert!(!temp.path().join("cache").exists());
    let invalid = Config::from_values(Some("typo".into()), Some("relative".into()), None, None);
    assert_eq!(invalid.policy, Policy::Invalid);
    assert!(invalid.directory.is_none());
    let defaults = Config::from_values(
        None,
        None,
        Some("/fictional/cache".into()),
        Some("/fictional/home".into()),
    );
    assert_eq!(
        defaults.directory.as_deref(),
        Some(Path::new("/fictional/cache/agent-knowledge"))
    );
}

#[test]
fn broken_cache_and_failed_attempts_do_not_retry_on_each_invocation() {
    let (temp, config) = fixture();
    let directory = config
        .directory
        .as_deref()
        .unwrap_or_else(|| panic!("fixture"));
    fs::create_dir_all(directory).unwrap_or_else(|e| panic!("fixture: {e}"));
    for bytes in [
        b"not json".to_vec(),
        vec![b' '; MAXIMUM_CACHE_BYTES as usize + 1],
    ] {
        fs::write(directory.join(CACHE_NAME), bytes).unwrap_or_else(|e| panic!("fixture: {e}"));
        assert_eq!(
            check_with(&config, 123, true, || panic!(
                "invalid cache must not fetch"
            ))
            .error,
            Some(CheckError::CacheInvalid)
        );
    }
    fs::remove_file(directory.join(CACHE_NAME)).unwrap_or_else(|e| panic!("fixture: {e}"));
    let status = check_with(&config, 123, true, || Err(CheckError::Network));
    assert_eq!(status.error, Some(CheckError::Network));
    assert!(status.latest.is_none());
    assert_eq!(
        check_with(&config, 124, true, || panic!(
            "failed attempt must be cached"
        ))
        .error,
        Some(CheckError::Network)
    );
    let invalid_path = temp.path().join("file");
    fs::write(&invalid_path, b"fixture").unwrap_or_else(|e| panic!("fixture: {e}"));
    let config = Config {
        directory: Some(invalid_path),
        ..config
    };
    assert_eq!(
        check_with(&config, 123, true, || panic!(
            "unwritable cache must not fetch"
        ))
        .error,
        Some(CheckError::CacheUnavailable)
    );
}

#[test]
fn overlapping_callers_and_an_interrupted_attempt_are_throttled() {
    let (_temp, config) = fixture();
    let result = check_with(&config, 123, true, || {
        assert_eq!(
            check_with(&config, 123, true, || panic!("overlapping request")).error,
            Some(CheckError::CacheBusy)
        );
        // Reservation is already on disk while the first process is networking.
        let reserved = check_with(&config, 123, false, || panic!("read only"));
        assert_eq!(reserved.last_attempt_at, Some(123));
        assert_eq!(reserved.error, Some(CheckError::Incomplete));
        Ok(release())
    });
    assert_eq!(result.status, "available");
    check_with(&config, 124, true, || panic!("persistent reservation"));
}

#[test]
fn notices_are_once_per_release_and_failures_are_best_effort() {
    let (_temp, config) = fixture();
    check_with(&config, 123, true, || Ok(release()));
    let mut output = Vec::new();
    notify_inner(&config, 123, "0.1.0", &mut output).unwrap_or_else(|e| panic!("notice: {e:?}"));
    assert!(String::from_utf8_lossy(&output).contains("9.8.7"));
    output.clear();
    notify_inner(&config, 123, "0.1.0", &mut output).unwrap_or_else(|e| panic!("notice: {e:?}"));
    assert!(output.is_empty());
    let next = Release {
        version: "10.0.0".to_owned(),
        url: "https://github.com/neodymium6/agent-knowledge/releases/tag/v10.0.0".to_owned(),
    };
    check_with(&config, 123 + TTL_SECONDS, true, || Ok(next));
    notify_inner(&config, 123 + TTL_SECONDS, "10.0.0", &mut output)
        .unwrap_or_else(|e| panic!("notice: {e:?}"));
    assert!(output.is_empty());
    struct Broken;
    impl Write for Broken {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            Err(io::ErrorKind::BrokenPipe.into())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    assert!(notify_inner(&config, 123 + TTL_SECONDS, "0.1.0", Broken).is_ok());
    notify_inner(&config, 123 + TTL_SECONDS, "0.1.0", &mut output)
        .unwrap_or_else(|e| panic!("notice: {e:?}"));
    assert!(output.is_empty());
}

#[test]
fn stale_metadata_and_prerelease_or_injected_values_do_not_generate_notices() {
    let (_temp, config) = fixture();
    let status = check_with(&config, 123, true, || Ok(release()));
    assert_eq!(status.newer_than("9.8.7+build.2"), Some(false));
    assert_eq!(status.newer_than("9.8.7-rc.1"), Some(true));
    assert_eq!(status.newer_than("development"), None);
    let mut output = Vec::new();
    notify_inner(&config, 123 + TTL_SECONDS, "0.1.0", &mut output)
        .unwrap_or_else(|e| panic!("notice: {e:?}"));
    assert!(output.is_empty());
    for value in ["9.8.7-rc.1", "9.8.7\n", "v9.8.7", "009.8.7"] {
        let invalid = Release {
            version: value.to_owned(),
            ..release()
        };
        assert!(!invalid.valid());
    }
    let invalid = Release {
        url: "https://fictional.invalid/releases/tag/v9.8.7".to_owned(),
        ..release()
    };
    assert!(!invalid.valid());
}

#[test]
fn releasing_cache_lock_does_not_wait_for_inherited_descriptors() {
    let (_temp, config) = fixture();
    let directory = config
        .directory
        .as_deref()
        .unwrap_or_else(|| panic!("fixture"));
    let lock = lock_cache(directory).unwrap_or_else(|e| panic!("lock: {e:?}"));
    // A cloned descriptor has the same lock lifetime as one inherited across fork.
    let inherited = lock
        .0
        .try_clone()
        .unwrap_or_else(|e| panic!("descriptor: {e}"));
    drop(lock);
    let next = lock_cache(directory)
        .unwrap_or_else(|e| panic!("finished check must release its lock: {e:?}"));
    drop(next);
    drop(inherited);
}
