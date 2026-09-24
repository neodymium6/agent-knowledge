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
            }
            | InspectQuery::ContextBalanced {
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
                && (!matches!(request.query, InspectQuery::ContextBalanced { .. })
                    || (*maximum_documents > 0
                        && documents.iter().all(|d| {
                            d.body.chars().count() <= maximum_characters / maximum_documents
                        })))
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
        (
            InspectQuery::DiffHunks {
                document_id,
                from_commit,
                to_commit,
                maximum_hunks,
                maximum_diff_bytes,
                ..
            },
            Inspection::DiffHunks {
                from_commit: from,
                to_commit: to,
                before,
                after,
                body,
            },
        ) => {
            from_commit == from
                && to_commit == to
                && before.metadata.document_id == *document_id
                && after.metadata.document_id == *document_id
                && body.hunks.len() <= *maximum_hunks
                && body.truncated == body.truncation_reason.is_some()
                && (body.changed || (body.hunks.is_empty() && !body.truncated))
                && (!body.changed || !body.hunks.is_empty() || body.truncated)
                && body.hunks.iter().all(|h| h.from_line > 0 && h.to_line > 0)
                && serde_json::to_vec(body).is_ok_and(|bytes| bytes.len() <= *maximum_diff_bytes)
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
    #[test]
    fn concise_diff_rejects_excess_bytes_counts_and_mismatched_endpoints() {
        let summary = serde_json::json!({"path":"projects/fictional-project/index.md","document_type":"index","project":"fictional-project","archived":false,"revision":format!("sha256:{}", "a".repeat(64)),"metadata":{"schema_version":1,"document_id":"01K00000000000000000000001","title":"Fictional project","created":"2026-09-24T00:00:00Z","request_id":"01K00000000000000000000002","status":"active"}});
        let wire = serde_json::json!({"protocol_version":1,"result":{"operation":"diff_hunks","from_commit":"a".repeat(40),"to_commit":"b".repeat(40),"before":summary,"after":summary,"body":{"changed":true,"hunks":[{"from_line":1,"to_line":1,"removed":"old","added":"new"}],"truncated":false,"truncation_reason":null}}});
        let request = InspectRequest {
            protocol_version: 1,
            query: InspectQuery::DiffHunks {
                document_id: "01K00000000000000000000001"
                    .parse()
                    .unwrap_or_else(|e| panic!("ID: {e}")),
                from_commit: "a".repeat(40),
                to_commit: "b".repeat(40),
                context_lines: 0,
                maximum_hunks: 1,
                maximum_diff_bytes: 256,
            },
        };
        let valid_response: InspectResponse =
            serde_json::from_value(wire.clone()).unwrap_or_else(|e| panic!("JSON: {e}"));
        assert!(validate(&request, &valid_response).is_ok());
        let mut cases = Vec::new();
        let mut value = wire.clone();
        value["result"]["from_commit"] = "c".repeat(40).into();
        cases.push(value);
        let mut value = wire.clone();
        value["result"]["body"]["hunks"][0]["added"] = "x".repeat(256).into();
        cases.push(value);
        let mut value = wire.clone();
        value["result"]["body"]["hunks"] = serde_json::json!([
            wire["result"]["body"]["hunks"][0],
            wire["result"]["body"]["hunks"][0]
        ]);
        cases.push(value);
        let mut value = wire;
        value["result"]["body"]["truncated"] = true.into();
        cases.push(value);
        for value in cases {
            let response: InspectResponse =
                serde_json::from_value(value).unwrap_or_else(|e| panic!("JSON: {e}"));
            assert!(validate(&request, &response).is_err());
        }
    }
}
