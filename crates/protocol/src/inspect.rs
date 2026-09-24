//! Additive read operations; existing v1 response shapes remain unchanged.

use agent_knowledge_core::{DocumentId, ProjectId, Revision};
use serde::{Deserialize, Serialize};

use crate::{DocumentContent, DocumentSummary, ReadFilterRequest};

/// Request for one bounded, committed inspection.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InspectRequest {
    pub protocol_version: u16,
    pub query: InspectQuery,
}

/// Read operations available through the additive inspect command.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum InspectQuery {
    Projects {
        query: Option<String>,
        #[serde(default)]
        search_in: ProjectSearchScope,
        maximum_results: usize,
        description_characters: usize,
        include_archived: bool,
    },
    ProjectsWithHits {
        query: String,
        maximum_results: usize,
        description_characters: usize,
        include_archived: bool,
        hits_per_project: usize,
        excerpt_characters: usize,
    },
    SearchExcerpts {
        query: String,
        filter: ReadFilterRequest,
        maximum_results: usize,
        excerpt_characters: usize,
    },
    Context {
        project: ProjectId,
        query: Option<String>,
        maximum_documents: usize,
        maximum_characters: usize,
    },
    ContextBalanced {
        project: ProjectId,
        query: Option<String>,
        maximum_documents: usize,
        maximum_characters: usize,
        recent_documents: usize,
    },
    History {
        document_id: DocumentId,
        anchor_commit: Option<String>,
        cursor: Option<String>,
        maximum_results: usize,
    },
    GetAt {
        document_id: DocumentId,
        commit: String,
    },
    Diff {
        document_id: DocumentId,
        from_commit: String,
        to_commit: String,
    },
    DiffHunks {
        document_id: DocumentId,
        from_commit: String,
        to_commit: String,
        context_lines: usize,
        maximum_hunks: usize,
        maximum_diff_bytes: usize,
    },
}

/// Fields used to discover projects.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectSearchScope {
    /// Case-insensitive substring of the slug, index title, or index body.
    #[default]
    Project,
    /// The configured document search backend, aggregated by project.
    Documents,
}

/// One project derived from committed documents, including projects without an index.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectSummary {
    pub project: ProjectId,
    /// The project index supplies the display title and description, when present.
    pub index: Option<DocumentSummary>,
    /// Raw prefix of the index body; never an inferred or generated summary.
    pub description: String,
    pub description_truncated: bool,
    /// Number of documents within the requested archive scope.
    pub document_count: usize,
    /// Exact matching document count in documents mode; absent in project mode.
    pub matching_documents: Option<usize>,
    /// Top matching documents, present only when explicitly requested.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hits: Option<Vec<SearchHit>>,
    /// Whether matching documents were omitted; absent when hits are absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hits_truncated: Option<bool>,
}

/// A raw, Unicode-safe excerpt of a document field.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Excerpt {
    pub field: String,
    pub text: String,
    pub truncated: bool,
}

/// One ranked search hit with query-relevant excerpts.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SearchHit {
    pub document: DocumentSummary,
    pub excerpts: Vec<Excerpt>,
}

/// A selected context document, with its selection reason and raw Markdown body.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ContextDocument {
    pub document: DocumentSummary,
    pub reason: String,
    pub body: String,
    pub truncated: bool,
}

/// A committed change to a document's Markdown or canonical path.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistoryEntry {
    pub commit: String,
    pub document: DocumentSummary,
    pub previous_revision: Option<Revision>,
}

/// A contiguous changed line range, excluding common prefix and suffix lines.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BodyDiff {
    pub from_line: usize,
    pub to_line: usize,
    pub removed: String,
    pub added: String,
}

/// Why a concise difference omits some changes.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DiffTruncation {
    InputLineLimit,
    ComputationLimit,
    HunkLimit,
    ByteLimit,
}

/// Bounded, line-preserving changed ranges with local context.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BodyHunks {
    /// True when the complete input bodies differ, including line endings.
    pub changed: bool,
    pub hunks: Vec<BodyDiff>,
    pub truncated: bool,
    pub truncation_reason: Option<DiffTruncation>,
}

/// Successful inspection; every selected document comes from the named commit.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum Inspection {
    Projects {
        commit: String,
        projects: Vec<ProjectSummary>,
        truncated: bool,
    },
    SearchExcerpts {
        commit: String,
        hits: Vec<SearchHit>,
    },
    Context {
        commit: String,
        documents: Vec<ContextDocument>,
        additional: Vec<DocumentSummary>,
        truncated: bool,
    },
    History {
        anchor_commit: String,
        entries: Vec<HistoryEntry>,
        next_cursor: Option<String>,
    },
    GetAt {
        commit: String,
        document: Box<DocumentContent>,
    },
    Diff {
        from_commit: String,
        to_commit: String,
        before: Box<DocumentSummary>,
        after: Box<DocumentSummary>,
        body: BodyDiff,
    },
    DiffHunks {
        from_commit: String,
        to_commit: String,
        before: Box<DocumentSummary>,
        after: Box<DocumentSummary>,
        body: BodyHunks,
    },
}

/// Versioned response for the inspect command.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InspectResponse {
    pub protocol_version: u16,
    pub result: Inspection,
}

#[cfg(test)]
mod tests {
    use super::InspectRequest;
    #[test]
    fn rejects_unknown_inspection_operations_and_fields() {
        for wire in [
            r#"{"protocol_version":1,"query":{"operation":"checkout"}}"#,
            r#"{"protocol_version":1,"query":{"operation":"context","project":"fictional-project","query":null,"maximum_documents":10,"maximum_characters":2000,"extra":true}}"#,
        ] {
            assert!(serde_json::from_str::<InspectRequest>(wire).is_err());
        }
    }
}
