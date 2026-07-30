use super::{DocumentLimits, DocumentMetadata, DocumentValidationError};
use crate::DocumentType;

const METADATA_JSON: &str = r#"{
    "schema_version": 1,
    "document_id": "01K00000000000000000000002",
    "title": "Fictional benchmark",
    "created": "2026-07-31T03:50:00+09:00",
    "node": "fictional-node-a",
    "agent": "codex",
    "session": "01K00000000000000000000001",
    "request_id": "01K00000000000000000000000",
    "tags": ["benchmark", "performance"],
    "status": "active"
}"#;

fn metadata() -> DocumentMetadata {
    match serde_json::from_str(METADATA_JSON) {
        Ok(metadata) => metadata,
        Err(error) => panic!("metadata fixture must parse: {error}"),
    }
}

#[test]
fn validates_typed_document_metadata() {
    assert_eq!(
        metadata().validate(DocumentType::Experiment, DocumentLimits::default()),
        Ok(())
    );
}

#[test]
fn rejects_unknown_fields_and_non_rfc3339_timestamps() {
    let unknown = METADATA_JSON.replace(
        "\"status\": \"active\"",
        "\"status\": \"active\", \"unknown\": true",
    );
    assert!(serde_json::from_str::<DocumentMetadata>(&unknown).is_err());

    let invalid_time = METADATA_JSON.replace("2026-07-31T03:50:00+09:00", "2026-07-31 03:50:00");
    assert!(serde_json::from_str::<DocumentMetadata>(&invalid_time).is_err());
}

#[test]
fn log_metadata_requires_session_identity() {
    let mut metadata = metadata();
    metadata.session = None;
    assert_eq!(
        metadata.validate(DocumentType::Log, DocumentLimits::default()),
        Err(DocumentValidationError::MissingLogMetadata { field: "session" })
    );
}

#[test]
fn rejects_duplicate_tags_and_accepts_descriptive_tags() {
    let mut metadata = metadata();
    metadata.tags.push("benchmark".into());
    assert_eq!(
        metadata.validate(DocumentType::Experiment, DocumentLimits::default()),
        Err(DocumentValidationError::DuplicateTag("benchmark".into()))
    );

    metadata.tags = vec!["two words".into()];
    assert_eq!(
        metadata.validate(DocumentType::Experiment, DocumentLimits::default()),
        Ok(())
    );
}
