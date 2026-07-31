use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use ulid::Ulid;

use super::{QuartzBuildError, QuartzBuilder, enforce_output_limits};
use crate::ReleasePolicy;

struct TestDirectory(PathBuf);

impl TestDirectory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "agent-knowledge-quartz-builder-test-{}",
            Ulid::generate()
        ));
        fs::create_dir(&path)
            .unwrap_or_else(|error| panic!("test directory must be created: {error}"));
        Self(path)
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.0)
            && error.kind() != io::ErrorKind::NotFound
        {
            panic!("test directory must be removed: {error}");
        }
    }
}

#[cfg(unix)]
fn executable(path: &Path, script: &str) {
    fs::write(path, script)
        .unwrap_or_else(|error| panic!("fake Quartz command must be written: {error}"));
    let mut permissions = fs::metadata(path)
        .unwrap_or_else(|error| panic!("fake Quartz metadata must be readable: {error}"))
        .permissions();
    permissions.set_mode(0o700);
    fs::set_permissions(path, permissions)
        .unwrap_or_else(|error| panic!("fake Quartz command must be executable: {error}"));
}

#[cfg(unix)]
#[test]
fn invokes_quartz_with_fixed_content_and_output_arguments() {
    let root = TestDirectory::new();
    let integration = root.0.join("integration");
    let content = root.0.join("content");
    let output = root.0.join("output");
    fs::create_dir(&integration)
        .unwrap_or_else(|error| panic!("integration directory must be created: {error}"));
    fs::create_dir(&content)
        .unwrap_or_else(|error| panic!("content directory must be created: {error}"));
    fs::create_dir(&output)
        .unwrap_or_else(|error| panic!("output directory must be created: {error}"));
    let program = root.0.join("fake-quartz");
    executable(
        &program,
        "#!/bin/sh\n\
         test \"$1\" = \"fictional-prefix\" || exit 11\n\
         test \"$2\" = \"build\" || exit 12\n\
         test \"$3\" = \"-d\" || exit 13\n\
         test \"$5\" = \"-o\" || exit 14\n\
         printf '%s\\n' '<p>fictional release</p>' > \"$6/index.html\"\n",
    );
    let builder = QuartzBuilder::new(
        &program,
        &integration,
        vec!["fictional-prefix".into()],
        Duration::from_secs(2),
    )
    .unwrap_or_else(|error| panic!("Quartz builder must initialize: {error}"));

    builder
        .build(&content, &output)
        .unwrap_or_else(|error| panic!("fake Quartz build must succeed: {error}"));
    assert_eq!(
        fs::read_to_string(output.join("index.html"))
            .unwrap_or_else(|error| panic!("fake output must be readable: {error}")),
        "<p>fictional release</p>\n"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn allows_quartz_to_replace_output_below_a_pinned_container() {
    use std::fs::File;
    use std::os::fd::AsRawFd;

    let root = TestDirectory::new();
    let integration = root.0.join("integration");
    let content = root.0.join("content");
    let container = root.0.join("container");
    for directory in [&integration, &content, &container] {
        fs::create_dir(directory)
            .unwrap_or_else(|error| panic!("fixture directory must be created: {error}"));
    }
    let container_handle = File::open(&container)
        .unwrap_or_else(|error| panic!("container directory must be pinned: {error}"));
    let output = PathBuf::from(format!(
        "/proc/{}/fd/{}/site",
        std::process::id(),
        container_handle.as_raw_fd()
    ));
    fs::create_dir(&output)
        .unwrap_or_else(|error| panic!("initial output directory must be created: {error}"));
    let program = root.0.join("fake-replacing-quartz");
    executable(
        &program,
        "#!/bin/sh\n\
         rm -rf \"$5\" || exit 21\n\
         mkdir \"$5\" || exit 22\n\
         printf '%s\\n' '<p>fictional replaced release</p>' > \"$5/index.html\"\n",
    );
    let builder = QuartzBuilder::new(&program, &integration, Vec::new(), Duration::from_secs(2))
        .unwrap_or_else(|error| panic!("Quartz builder must initialize: {error}"));

    builder
        .build(&content, &output)
        .unwrap_or_else(|error| panic!("Quartz-style output replacement must succeed: {error}"));
    assert!(container.join("site").join("index.html").is_file());
}

#[cfg(unix)]
#[test]
fn terminates_a_quartz_command_after_its_deadline() {
    let root = TestDirectory::new();
    let integration = root.0.join("integration");
    let content = root.0.join("content");
    let output = root.0.join("output");
    for directory in [&integration, &content, &output] {
        fs::create_dir(directory)
            .unwrap_or_else(|error| panic!("fixture directory must be created: {error}"));
    }
    let program = root.0.join("fake-hanging-quartz");
    executable(&program, "#!/bin/sh\nwhile :; do :; done\n");
    let builder = QuartzBuilder::new(
        &program,
        &integration,
        Vec::new(),
        Duration::from_millis(25),
    )
    .unwrap_or_else(|error| panic!("Quartz builder must initialize: {error}"));

    assert!(matches!(
        builder.build(&content, &output),
        Err(QuartzBuildError::TimedOut { .. })
    ));
}

#[cfg(target_os = "linux")]
#[test]
fn terminates_quartz_descendants_after_a_timeout() {
    let root = TestDirectory::new();
    let integration = root.0.join("integration");
    let content = root.0.join("content");
    let output = root.0.join("output");
    for directory in [&integration, &content, &output] {
        fs::create_dir(directory)
            .unwrap_or_else(|error| panic!("fixture directory must be created: {error}"));
    }
    let program = root.0.join("fake-descendant-quartz");
    executable(
        &program,
        "#!/bin/sh\n\
         (sleep 0.15; printf '%s\\n' escaped > \"$5/escaped\") &\n\
         while :; do :; done\n",
    );
    let builder = QuartzBuilder::new(
        &program,
        &integration,
        Vec::new(),
        Duration::from_millis(25),
    )
    .unwrap_or_else(|error| panic!("Quartz builder must initialize: {error}"));

    assert!(matches!(
        builder.build(&content, &output),
        Err(QuartzBuildError::TimedOut { .. })
    ));
    std::thread::sleep(Duration::from_millis(250));
    assert!(!output.join("escaped").exists());
}

#[cfg(unix)]
#[test]
fn terminates_quartz_when_live_output_exceeds_policy() {
    let root = TestDirectory::new();
    let integration = root.0.join("integration");
    let content = root.0.join("content");
    let output = root.0.join("output");
    for directory in [&integration, &content, &output] {
        fs::create_dir(directory)
            .unwrap_or_else(|error| panic!("fixture directory must be created: {error}"));
    }
    let program = root.0.join("fake-oversized-quartz");
    executable(
        &program,
        "#!/bin/sh\n\
         dd if=/dev/zero of=\"$5/oversized.bin\" bs=1024 count=64 2>/dev/null\n\
         sleep 2\n",
    );
    let builder = QuartzBuilder::new_with_policy(
        &program,
        &integration,
        Vec::new(),
        Duration::from_secs(3),
        ReleasePolicy {
            maximum_entries: 10,
            maximum_file_bytes: 32,
            maximum_total_bytes: 32,
        },
    )
    .unwrap_or_else(|error| panic!("Quartz builder must initialize: {error}"));

    let started = std::time::Instant::now();
    assert!(matches!(
        builder.build(&content, &output),
        Err(QuartzBuildError::OutputLimitExceeded)
    ));
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[cfg(unix)]
#[test]
fn live_output_scan_bounds_descriptors_for_wide_trees() {
    const CHILD_MARKER: &str = "AGENT_KNOWLEDGE_LOW_FD_SCAN";
    if std::env::var_os(CHILD_MARKER).is_none() {
        let executable = std::env::current_exe()
            .unwrap_or_else(|error| panic!("test executable path must be available: {error}"));
        let status = std::process::Command::new("/bin/sh")
            .arg("-c")
            .arg(
                "ulimit -n 64 && exec \"$1\" --exact \
                 builder::tests::live_output_scan_bounds_descriptors_for_wide_trees --nocapture",
            )
            .arg("sh")
            .arg(executable)
            .env(CHILD_MARKER, "1")
            .status()
            .unwrap_or_else(|error| panic!("low-descriptor test process must start: {error}"));
        assert!(status.success());
        return;
    }

    let root = TestDirectory::new();
    let output = root.0.join("output");
    fs::create_dir(&output)
        .unwrap_or_else(|error| panic!("output directory must be created: {error}"));
    for index in 0..256 {
        fs::create_dir(output.join(format!("directory-{index:04}")))
            .unwrap_or_else(|error| panic!("wide output directory must be created: {error}"));
    }
    enforce_output_limits(
        &output,
        ReleasePolicy {
            maximum_entries: 512,
            maximum_file_bytes: 64,
            maximum_total_bytes: 64,
        },
        std::time::Instant::now() + Duration::from_secs(5),
        Duration::from_secs(5),
    )
    .unwrap_or_else(|error| panic!("wide output must scan within descriptor limits: {error}"));
}

#[test]
fn live_output_scan_observes_the_build_deadline() {
    let root = TestDirectory::new();
    let output = root.0.join("output");
    fs::create_dir(&output)
        .unwrap_or_else(|error| panic!("output directory must be created: {error}"));

    assert!(matches!(
        enforce_output_limits(
            &output,
            ReleasePolicy::default(),
            std::time::Instant::now(),
            Duration::from_millis(10),
        ),
        Err(QuartzBuildError::TimedOut { .. })
    ));
}

#[cfg(unix)]
#[test]
fn rejects_a_deadline_that_cannot_be_represented() {
    let root = TestDirectory::new();
    let integration = root.0.join("integration");
    fs::create_dir(&integration)
        .unwrap_or_else(|error| panic!("integration directory must be created: {error}"));
    let program = root.0.join("fake-quartz");
    executable(&program, "#!/bin/sh\nexit 0\n");

    assert!(matches!(
        QuartzBuilder::new(&program, &integration, Vec::new(), Duration::MAX),
        Err(QuartzBuildError::InvalidTimeout)
    ));
}

#[cfg(unix)]
#[test]
fn rejects_replaced_command_configuration() {
    let root = TestDirectory::new();
    let integration = root.0.join("integration");
    let content = root.0.join("content");
    let output = root.0.join("output");
    for directory in [&integration, &content, &output] {
        fs::create_dir(directory)
            .unwrap_or_else(|error| panic!("fixture directory must be created: {error}"));
    }
    let program = root.0.join("fake-quartz");
    executable(
        &program,
        "#!/bin/sh\nprintf '%s\\n' safe > \"$5/index.html\"\n",
    );
    let builder = QuartzBuilder::new(&program, &integration, Vec::new(), Duration::from_secs(2))
        .unwrap_or_else(|error| panic!("Quartz builder must initialize: {error}"));
    fs::rename(&program, root.0.join("detached-fake-quartz"))
        .unwrap_or_else(|error| panic!("configured program must be moved: {error}"));
    executable(
        &program,
        "#!/bin/sh\nprintf '%s\\n' replacement > \"$5/index.html\"\n",
    );

    assert!(matches!(
        builder.build(&content, &output),
        Err(QuartzBuildError::CommandIdentityChanged)
    ));
    assert!(
        fs::read_dir(&output)
            .unwrap_or_else(|error| panic!("output directory must be readable: {error}"))
            .next()
            .is_none()
    );
}

#[cfg(unix)]
#[test]
fn rejects_a_successful_command_that_produces_no_site() {
    let root = TestDirectory::new();
    let integration = root.0.join("integration");
    let content = root.0.join("content");
    let output = root.0.join("output");
    for directory in [&integration, &content, &output] {
        fs::create_dir(directory)
            .unwrap_or_else(|error| panic!("fixture directory must be created: {error}"));
    }
    let program = root.0.join("fake-empty-quartz");
    executable(&program, "#!/bin/sh\nexit 0\n");
    let builder = QuartzBuilder::new(&program, &integration, Vec::new(), Duration::from_secs(2))
        .unwrap_or_else(|error| panic!("Quartz builder must initialize: {error}"));

    assert!(matches!(
        builder.build(&content, &output),
        Err(QuartzBuildError::OutputEmpty)
    ));
}
