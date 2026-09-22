use super::*;
use agent_knowledge_protocol::{
    INSPECT_COMMAND, InspectQuery, InspectRequest, InspectResponse, Inspection,
};

impl SshClient {
    /// Reads excerpts, context, or history through the authenticated SSH boundary.
    ///
    /// # Errors
    /// Rejects invalid transport responses or a response for another operation/document.
    pub fn inspect(&self, request: &InspectRequest) -> Result<InspectResponse, ClientCommandError> {
        let (response, _) = control_response_with_program::<_, InspectResponse>(
            OsStr::new(SSH_PROGRAM),
            &self.destination,
            ControlOperation::new(INSPECT_COMMAND, MAXIMUM_CONTROL_RESPONSE_BYTES),
            request,
            self.timeout,
        )?;
        validate(request, &response)?;
        Ok(response)
    }
}

fn validate(
    request: &InspectRequest,
    response: &InspectResponse,
) -> Result<(), ClientCommandError> {
    let valid = match (&request.query, &response.result) {
        (
            InspectQuery::SearchExcerpts {
                maximum_results,
                excerpt_characters,
                ..
            },
            Inspection::SearchExcerpts { hits, .. },
        ) => {
            hits.len() <= *maximum_results
                && hits.iter().all(|h| {
                    h.excerpts
                        .iter()
                        .all(|e| e.text.chars().count() <= *excerpt_characters)
                })
        }
        (
            InspectQuery::Context {
                project,
                maximum_documents,
                maximum_characters,
                ..
            },
            Inspection::Context {
                documents,
                additional,
                ..
            },
        ) => {
            documents.len() <= *maximum_documents
                && additional.len() <= *maximum_documents
                && documents
                    .iter()
                    .map(|d| d.body.chars().count())
                    .sum::<usize>()
                    <= *maximum_characters
                && documents
                    .iter()
                    .all(|d| d.document.project.as_ref() == Some(project))
                && additional
                    .iter()
                    .all(|d| d.project.as_ref() == Some(project))
        }
        (
            InspectQuery::History {
                document_id,
                anchor_commit,
                maximum_results,
                ..
            },
            Inspection::History {
                anchor_commit: actual,
                entries,
                ..
            },
        ) => {
            anchor_commit.as_ref().is_none_or(|c| c == actual)
                && entries.len() <= *maximum_results
                && entries
                    .iter()
                    .all(|e| e.document.metadata.document_id == *document_id)
        }
        (
            InspectQuery::GetAt {
                document_id,
                commit,
            },
            Inspection::GetAt {
                commit: actual,
                document,
            },
        ) => {
            commit == actual
                && document.summary.metadata.document_id == *document_id
                && Revision::from_bytes(Sha256::digest(document.markdown.as_bytes()).into())
                    == document.summary.revision
                && decode_document_metadata(
                    document.markdown.as_bytes(),
                    PackagePolicy::default().limits().maximum_front_matter_bytes,
                )
                .is_ok_and(|m| m == document.summary.metadata)
        }
        (
            InspectQuery::Diff {
                document_id,
                from_commit,
                to_commit,
            },
            Inspection::Diff {
                from_commit: from,
                to_commit: to,
                before,
                after,
                ..
            },
        ) => {
            from_commit == from
                && to_commit == to
                && before.metadata.document_id == *document_id
                && after.metadata.document_id == *document_id
        }
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(ClientCommandError::DocumentResponseMismatch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_wrong_operation_and_accepts_empty_context() {
        let request = InspectRequest {
            protocol_version: 1,
            query: InspectQuery::Context {
                project: "fictional-project"
                    .parse()
                    .unwrap_or_else(|e| panic!("project: {e}")),
                query: None,
                maximum_documents: 1,
                maximum_characters: 10,
            },
        };
        let response = InspectResponse {
            protocol_version: 1,
            result: Inspection::SearchExcerpts {
                commit: "a".repeat(40),
                hits: vec![],
            },
        };
        assert!(validate(&request, &response).is_err());
        let response = InspectResponse {
            protocol_version: 1,
            result: Inspection::Context {
                commit: "a".repeat(40),
                documents: vec![],
                additional: vec![],
                truncated: false,
            },
        };
        assert!(validate(&request, &response).is_ok());
    }
}
