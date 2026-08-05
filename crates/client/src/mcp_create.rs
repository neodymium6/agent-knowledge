use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::Path;

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

use agent_knowledge_core::{
    CURRENT_DOCUMENT_SCHEMA_VERSION, CURRENT_PROTOCOL_VERSION, ChangeRequest, DocumentId,
    DocumentMetadata, DocumentStatus, DocumentType, Operation, PayloadPath, ProjectId, RequestId,
    SessionId,
};
use agent_knowledge_queue::PackagePolicy;
use schemars::JsonSchema;
use serde::Deserialize;
use tempfile::TempDir;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

const DOCUMENT_PAYLOAD_PATH: &str = "document.md";

#[derive(Clone, Copy, Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(super) enum CreateDocumentType {
    Index,
    Log,
    Experiment,
    Decision,
    Runbook,
    Reference,
}

impl From<CreateDocumentType> for DocumentType {
    fn from(value: CreateDocumentType) -> Self {
        match value {
            CreateDocumentType::Index => Self::Index,
            CreateDocumentType::Log => Self::Log,
            CreateDocumentType::Experiment => Self::Experiment,
            CreateDocumentType::Decision => Self::Decision,
            CreateDocumentType::Runbook => Self::Runbook,
            CreateDocumentType::Reference => Self::Reference,
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct CreateDocumentParameters {
    /// Meaningful title used for the document and Git commit entry.
    title: String,
    /// Markdown body without YAML front matter.
    body: String,
    /// Target project slug. Omit it to place the document in the inbox.
    #[serde(default)]
    project: Option<String>,
    /// Document classification used to derive its canonical path.
    document_type: CreateDocumentType,
    /// Client node associated with the document. Required for logs.
    #[serde(default)]
    node: Option<String>,
    /// Coding agent associated with the document. Required for logs.
    #[serde(default)]
    agent: Option<String>,
    /// Coding-agent session ULID. Required for logs.
    #[serde(default)]
    session: Option<String>,
    /// Cross-cutting classification labels.
    #[serde(default)]
    tags: Vec<String>,
    /// Request ULID. Omit to generate one; reuse it with document_id and created_at after an uncertain response.
    #[serde(default)]
    request_id: Option<String>,
    /// Document ULID. Omit to generate one; reuse it with request_id and created_at after an uncertain response.
    #[serde(default)]
    document_id: Option<String>,
    /// RFC 3339 creation time. Omit to use the current time; reuse it with both IDs after an uncertain response.
    #[serde(default)]
    created_at: Option<String>,
}

pub(super) struct PreparedCreatePackage {
    request_json: Vec<u8>,
    markdown: Vec<u8>,
}

impl CreateDocumentParameters {
    pub(super) fn prepare(self) -> Result<PreparedCreatePackage, String> {
        let policy = PackagePolicy::default();
        let limits = policy.limits();
        let request_id = self
            .request_id
            .map(|value| {
                value
                    .parse::<RequestId>()
                    .map_err(|_| "request_id must be a canonical ULID".to_owned())
            })
            .transpose()?
            .unwrap_or_else(RequestId::generate);
        let document_id = self
            .document_id
            .map(|value| {
                value
                    .parse::<DocumentId>()
                    .map_err(|_| "document_id must be a canonical ULID".to_owned())
            })
            .transpose()?
            .unwrap_or_else(DocumentId::generate);
        let session = self
            .session
            .map(|value| {
                value
                    .parse::<SessionId>()
                    .map_err(|_| "session must be a canonical ULID".to_owned())
            })
            .transpose()?;
        let project = self
            .project
            .map(|value| {
                value
                    .parse::<ProjectId>()
                    .map_err(|_| "project must be a valid project slug".to_owned())
            })
            .transpose()?;
        let created_at = self
            .created_at
            .map(|value| {
                OffsetDateTime::parse(&value, &Rfc3339)
                    .map_err(|_| "created_at must be an RFC 3339 timestamp".to_owned())
            })
            .transpose()?
            .unwrap_or_else(OffsetDateTime::now_utc);
        let document_type = self.document_type.into();
        let content = DOCUMENT_PAYLOAD_PATH
            .parse::<PayloadPath>()
            .map_err(|_| "the built-in document payload path is invalid".to_owned())?;

        let request = ChangeRequest {
            protocol_version: CURRENT_PROTOCOL_VERSION,
            request_id,
            title: self.title.clone(),
            project,
            document_type,
            node: self.node.clone(),
            agent: self.agent.clone(),
            session,
            created_at,
            operations: vec![Operation::CreateDocument {
                document_id,
                content,
            }],
        };
        request
            .validate(limits.request)
            .map_err(|error| error.to_string())?;

        let metadata = DocumentMetadata {
            schema_version: CURRENT_DOCUMENT_SCHEMA_VERSION,
            document_id,
            title: self.title,
            created: created_at,
            updated: None,
            node: self.node,
            agent: self.agent,
            session,
            request_id,
            tags: self.tags,
            status: DocumentStatus::Active,
            superseded_by: None,
        };
        metadata
            .validate(document_type, limits.document)
            .map_err(|error| error.to_string())?;

        let request_json = serde_json::to_vec_pretty(&request)
            .map_err(|_| "could not encode the change request".to_owned())?;
        let yaml = serde_saphyr::to_string(&metadata)
            .map_err(|_| "could not encode document front matter".to_owned())?;
        let front_matter_bytes = yaml
            .len()
            .checked_add(usize::from(!yaml.ends_with('\n')))
            .ok_or_else(|| "document front matter is too large".to_owned())?;
        if front_matter_bytes > limits.maximum_front_matter_bytes {
            return Err(format!(
                "document front matter exceeds {} bytes",
                limits.maximum_front_matter_bytes
            ));
        }
        let mut markdown = Vec::with_capacity(
            front_matter_bytes
                .checked_add(self.body.len())
                .and_then(|length| length.checked_add(9))
                .ok_or_else(|| "document is too large".to_owned())?,
        );
        markdown.extend_from_slice(b"---\n");
        markdown.extend_from_slice(yaml.as_bytes());
        if !yaml.ends_with('\n') {
            markdown.push(b'\n');
        }
        markdown.extend_from_slice(b"---\n\n");
        markdown.extend_from_slice(self.body.as_bytes());

        enforce_package_size(
            &request_json,
            &markdown,
            limits.maximum_file_bytes,
            limits.maximum_total_bytes,
        )?;
        Ok(PreparedCreatePackage {
            request_json,
            markdown,
        })
    }
}

fn enforce_package_size(
    request_json: &[u8],
    markdown: &[u8],
    maximum_file_bytes: u64,
    maximum_total_bytes: u64,
) -> Result<(), String> {
    let request_bytes =
        u64::try_from(request_json.len()).map_err(|_| "change request is too large".to_owned())?;
    let markdown_bytes =
        u64::try_from(markdown.len()).map_err(|_| "document is too large".to_owned())?;
    if request_bytes > maximum_file_bytes {
        return Err(format!("change request exceeds {maximum_file_bytes} bytes"));
    }
    if markdown_bytes > maximum_file_bytes {
        return Err(format!("document exceeds {maximum_file_bytes} bytes"));
    }
    if request_bytes
        .checked_add(markdown_bytes)
        .is_none_or(|total| total > maximum_total_bytes)
    {
        return Err(format!(
            "request package exceeds {maximum_total_bytes} bytes"
        ));
    }
    Ok(())
}

impl PreparedCreatePackage {
    pub(super) fn materialize(&self) -> io::Result<TempDir> {
        let root = tempfile::Builder::new()
            .prefix("agent-knowledge-mcp-")
            .tempdir()?;
        let payload = root.path().join("payload");
        fs::create_dir(&payload)?;
        set_private_directory(root.path())?;
        set_private_directory(&payload)?;
        write_private_file(&root.path().join("request.json"), &self.request_json)?;
        write_private_file(&payload.join(DOCUMENT_PAYLOAD_PATH), &self.markdown)?;
        Ok(root)
    }
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

    use agent_knowledge_core::{DocumentType, decode_document_metadata, markdown_body};
    use agent_knowledge_queue::{PackagePolicy, validate_package};

    use super::{CreateDocumentParameters, enforce_package_size};

    const REQUEST_ID: &str = "01K00000000000000000000000";
    const DOCUMENT_ID: &str = "01K00000000000000000000001";

    fn parameters() -> CreateDocumentParameters {
        serde_json::from_value(serde_json::json!({
            "title": "Record fictional benchmark",
            "body": "# Result\n\nThe fictional benchmark completed.",
            "project": "fictional-solver",
            "document_type": "experiment",
            "node": "fictional-node-a",
            "agent": "codex",
            "session": "01K00000000000000000000002",
            "tags": ["benchmark", "fictional-data"],
            "request_id": REQUEST_ID,
            "document_id": DOCUMENT_ID,
            "created_at": "2026-08-05T10:00:00Z"
        }))
        .unwrap_or_else(|error| panic!("create parameters must decode: {error}"))
    }

    #[test]
    fn prepares_a_valid_deterministic_request_package() {
        let first = parameters()
            .prepare()
            .unwrap_or_else(|error| panic!("package must prepare: {error}"));
        let second = parameters()
            .prepare()
            .unwrap_or_else(|error| panic!("package must prepare again: {error}"));
        assert_eq!(first.request_json, second.request_json);
        assert_eq!(first.markdown, second.markdown);

        let temporary = first
            .materialize()
            .unwrap_or_else(|error| panic!("package must materialize: {error}"));
        let validated = validate_package(temporary.path(), &PackagePolicy::default())
            .unwrap_or_else(|error| panic!("generated package must validate: {error}"));
        assert_eq!(validated.request().request_id.to_string(), REQUEST_ID);
        assert_eq!(validated.request().document_type, DocumentType::Experiment);
        let markdown_path = temporary.path().join("payload/document.md");
        let markdown = fs::read(&markdown_path)
            .unwrap_or_else(|error| panic!("generated Markdown must be readable: {error}"));
        let metadata = decode_document_metadata(
            &markdown,
            PackagePolicy::default().limits().maximum_front_matter_bytes,
        )
        .unwrap_or_else(|error| panic!("generated front matter must decode: {error}"));
        assert_eq!(metadata.document_id.to_string(), DOCUMENT_ID);
        assert_eq!(
            markdown_body(&markdown)
                .unwrap_or_else(|error| panic!("generated Markdown body must decode: {error}")),
            "\n# Result\n\nThe fictional benchmark completed."
        );

        #[cfg(unix)]
        {
            let directory_mode = fs::metadata(temporary.path())
                .unwrap_or_else(|error| panic!("temporary directory metadata must exist: {error}"))
                .permissions()
                .mode()
                & 0o777;
            let file_mode = fs::metadata(markdown_path)
                .unwrap_or_else(|error| panic!("temporary file metadata must exist: {error}"))
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(directory_mode, 0o700);
            assert_eq!(file_mode, 0o600);
        }

        let path = temporary.path().to_owned();
        drop(temporary);
        assert!(!path.exists());
    }

    #[test]
    fn rejects_missing_log_metadata_before_materialization() {
        let parameters = serde_json::from_value::<CreateDocumentParameters>(serde_json::json!({
            "title": "Record fictional log",
            "body": "No node metadata.",
            "document_type": "log"
        }))
        .unwrap_or_else(|error| panic!("create parameters must decode: {error}"));

        assert_eq!(
            parameters.prepare().err().as_deref(),
            Some("log request requires `node` metadata")
        );
    }

    #[test]
    fn rejects_payloads_that_exceed_package_limits() {
        assert_eq!(
            enforce_package_size(b"{}", b"12345", 4, 16)
                .err()
                .as_deref(),
            Some("document exceeds 4 bytes")
        );
        assert_eq!(
            enforce_package_size(b"1234", b"5678", 4, 7)
                .err()
                .as_deref(),
            Some("request package exceeds 7 bytes")
        );
    }
}
