use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::Path;

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

use agent_knowledge_core::{ChangeRequest, PayloadPath};
use agent_knowledge_queue::PackagePolicy;
use tempfile::TempDir;

#[derive(Debug, Eq, PartialEq)]
pub(super) struct PayloadFile {
    path: PayloadPath,
    contents: Vec<u8>,
}

impl PayloadFile {
    pub(super) fn new(path: PayloadPath, contents: Vec<u8>) -> Result<Self, String> {
        if path.as_str().contains('/') {
            return Err("structured MCP payload paths must be single file names".to_owned());
        }
        Ok(Self { path, contents })
    }
}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct PreparedPackage {
    request_json: Vec<u8>,
    payload: Vec<PayloadFile>,
}

impl PreparedPackage {
    pub(super) fn new(request: ChangeRequest, payload: Vec<PayloadFile>) -> Result<Self, String> {
        let limits = PackagePolicy::default().limits();
        request
            .validate(limits.request)
            .map_err(|error| error.to_string())?;
        let request_json = serde_json::to_vec_pretty(&request)
            .map_err(|_| "could not encode the change request".to_owned())?;

        let file_count = payload
            .len()
            .checked_add(1)
            .ok_or_else(|| "request package contains too many files".to_owned())?;
        if file_count > limits.maximum_file_count {
            return Err(format!(
                "request package exceeds {} files",
                limits.maximum_file_count
            ));
        }
        if file_count > limits.maximum_entry_count {
            return Err(format!(
                "request package exceeds {} entries",
                limits.maximum_entry_count
            ));
        }

        let request_bytes = u64::try_from(request_json.len())
            .map_err(|_| "change request is too large".to_owned())?;
        enforce_file_size("change request", request_bytes, limits.maximum_file_bytes)?;
        let mut total_bytes = request_bytes;
        for file in &payload {
            let file_bytes = u64::try_from(file.contents.len())
                .map_err(|_| "payload file is too large".to_owned())?;
            enforce_file_size("payload file", file_bytes, limits.maximum_file_bytes)?;
            total_bytes = total_bytes
                .checked_add(file_bytes)
                .ok_or_else(|| "request package is too large".to_owned())?;
        }
        if total_bytes > limits.maximum_total_bytes {
            return Err(format!(
                "request package exceeds {} bytes",
                limits.maximum_total_bytes
            ));
        }

        Ok(Self {
            request_json,
            payload,
        })
    }

    pub(super) fn materialize(&self) -> io::Result<TempDir> {
        self.materialize_with_parent(None)
    }

    fn materialize_with_parent(&self, parent: Option<&Path>) -> io::Result<TempDir> {
        let mut builder = tempfile::Builder::new();
        builder.prefix("agent-knowledge-mcp-");
        let root = match parent {
            Some(parent) => builder.tempdir_in(parent)?,
            None => builder.tempdir()?,
        };
        let payload_root = root.path().join("payload");
        fs::create_dir(&payload_root)?;
        set_private_directory(root.path())?;
        set_private_directory(&payload_root)?;
        write_private_file(&root.path().join("request.json"), &self.request_json)?;
        for file in &self.payload {
            write_private_file(&payload_root.join(file.path.as_str()), &file.contents)?;
        }
        Ok(root)
    }
}

fn enforce_file_size(name: &str, actual: u64, maximum: u64) -> Result<(), String> {
    if actual > maximum {
        return Err(format!("{name} exceeds {maximum} bytes"));
    }
    Ok(())
}

#[cfg(unix)]
fn set_private_directory(path: &Path) -> io::Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
}

#[cfg(not(unix))]
fn set_private_directory(_path: &Path) -> io::Result<()> {
    Ok(())
}

fn write_private_file(path: &Path, contents: &[u8]) -> io::Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options.open(path)?;
    file.write_all(contents)
}

#[cfg(test)]
mod tests {
    use std::fs;

    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    use agent_knowledge_core::{PayloadPath, RequestId};
    use agent_knowledge_queue::{PackagePolicy, validate_package};

    use super::{PayloadFile, PreparedPackage, enforce_file_size};

    fn archive_package() -> PreparedPackage {
        let request = agent_knowledge_core::ChangeRequest::decode_json(
            br#"{
              "protocol_version": 1,
              "request_id": "01K00000000000000000000000",
              "title": "Archive fictional result",
              "project": "fictional-solver",
              "document_type": "experiment",
              "created_at": "2026-08-06T10:00:00Z",
              "operations": [{
                "type": "archive_document",
                "document_id": "01K00000000000000000000001",
                "expected_revision": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
              }]
            }"#,
        )
        .unwrap_or_else(|error| panic!("archive request must decode: {error}"));
        PreparedPackage::new(request, Vec::new())
            .unwrap_or_else(|error| panic!("archive package must prepare: {error}"))
    }

    #[test]
    fn materializes_a_private_empty_payload_package_and_removes_it() {
        let package = archive_package();
        let temporary = package
            .materialize()
            .unwrap_or_else(|error| panic!("archive package must materialize: {error}"));
        let validated = validate_package(temporary.path(), &PackagePolicy::default())
            .unwrap_or_else(|error| panic!("archive package must validate: {error}"));
        assert_eq!(
            validated.request().request_id,
            "01K00000000000000000000000"
                .parse::<RequestId>()
                .unwrap_or_else(|error| panic!("request ID must parse: {error}"))
        );
        assert_eq!(
            fs::read_dir(temporary.path().join("payload"))
                .unwrap_or_else(|error| panic!("payload directory must be readable: {error}"))
                .count(),
            0
        );

        #[cfg(unix)]
        for path in [temporary.path(), &temporary.path().join("payload")] {
            let mode = fs::metadata(path)
                .unwrap_or_else(|error| panic!("private directory metadata must exist: {error}"))
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o700);
        }

        #[cfg(unix)]
        {
            let mode = fs::metadata(temporary.path().join("request.json"))
                .unwrap_or_else(|error| panic!("request file metadata must exist: {error}"))
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o600);
        }

        let path = temporary.path().to_owned();
        drop(temporary);
        assert!(!path.exists());
    }

    #[test]
    fn removes_partial_materialization_after_a_local_failure() {
        let parent = tempfile::tempdir()
            .unwrap_or_else(|error| panic!("test parent must be created: {error}"));
        let path = "duplicate.md"
            .parse::<PayloadPath>()
            .unwrap_or_else(|error| panic!("payload path must parse: {error}"));
        let request = archive_package();
        let package = PreparedPackage {
            request_json: request.request_json,
            payload: vec![
                PayloadFile::new(path.clone(), b"first".to_vec())
                    .unwrap_or_else(|error| panic!("payload must prepare: {error}")),
                PayloadFile::new(path, b"second".to_vec())
                    .unwrap_or_else(|error| panic!("payload must prepare: {error}")),
            ],
        };

        assert!(
            package
                .materialize_with_parent(Some(parent.path()))
                .is_err()
        );
        assert_eq!(
            fs::read_dir(parent.path())
                .unwrap_or_else(|error| panic!("test parent must be readable: {error}"))
                .count(),
            0
        );
    }

    #[test]
    fn rejects_files_above_the_configured_limit() {
        assert_eq!(
            enforce_file_size("payload file", 11, 10).err().as_deref(),
            Some("payload file exceeds 10 bytes")
        );
    }
}
