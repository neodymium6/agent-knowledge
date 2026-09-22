use super::*;
use agent_knowledge_protocol::{CURRENT_GATEWAY_PROTOCOL_VERSION, InspectQuery, InspectRequest};

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct ExcerptParameters {
    /// Search expression using the configured search backend.
    query: String,
    #[serde(default)]
    project: Option<String>,
    #[serde(default)]
    tag: Option<String>,
    #[serde(default)]
    session: Option<String>,
    #[serde(default)]
    include_archived: bool,
    /// Maximum hits. Defaults to 10.
    #[serde(default)]
    maximum_results: Option<usize>,
    /// Maximum Unicode characters per field excerpt, 1..2000. Defaults to 300.
    #[serde(default)]
    excerpt_characters: Option<usize>,
}
impl ExcerptParameters {
    pub(super) fn request(self) -> Result<InspectRequest, String> {
        let search = SearchParameters {
            query: self.query,
            project: self.project,
            tag: self.tag,
            session: self.session,
            include_archived: self.include_archived,
            maximum_results: Some(self.maximum_results.unwrap_or(10)),
        }
        .into_request()?;
        Ok(request(InspectQuery::SearchExcerpts {
            query: search.query,
            filter: search.filter,
            maximum_results: search.maximum_results,
            excerpt_characters: bounded(self.excerpt_characters.unwrap_or(300), 2000)?,
        }))
    }
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct ContextParameters {
    /// Project whose index, guidance and recent records should be selected.
    project: String,
    /// Optional task-related search expression.
    #[serde(default)]
    query: Option<String>,
    /// Maximum selected documents. Defaults to 10.
    #[serde(default)]
    maximum_documents: Option<usize>,
    /// Total Unicode characters in returned bodies, 1..100000. Defaults to 20000.
    #[serde(default)]
    maximum_characters: Option<usize>,
}
impl ContextParameters {
    pub(super) fn request(self) -> Result<InspectRequest, String> {
        Ok(request(InspectQuery::Context {
            project: self.project.parse().map_err(|_| "invalid project slug")?,
            query: self.query,
            maximum_documents: bounded(self.maximum_documents.unwrap_or(10), MAXIMUM_RESULTS)?,
            maximum_characters: bounded(self.maximum_characters.unwrap_or(20000), 100000)?,
        }))
    }
}
fn request(query: InspectQuery) -> InspectRequest {
    InspectRequest {
        protocol_version: CURRENT_GATEWAY_PROTOCOL_VERSION,
        query,
    }
}
fn bounded(value: usize, maximum: usize) -> Result<usize, String> {
    if value == 0 || value > maximum {
        Err(format!("limit must be between 1 and {maximum}"))
    } else {
        Ok(value)
    }
}
