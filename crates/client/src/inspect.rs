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
        if matches!(request.query, InspectQuery::ProjectsWithHits { .. }) {
            match self.server_version() {
                crate::version::ServerVersion::Available {
                    gateway,
                    protocol_matches,
                } => {
                    if !protocol_matches {
                        return Err(ClientCommandError::UnsupportedProtocolVersion {
                            actual: gateway.protocol_version,
                        });
                    }
                    if !gateway
                        .inspect_queries
                        .iter()
                        .any(|q| q == "projects_with_hits")
                    {
                        return Err(ClientCommandError::UnsupportedInspection(
                            "projects_with_hits",
                        ));
                    }
                }
                crate::version::ServerVersion::Unsupported => {
                    return Err(ClientCommandError::UnsupportedInspection(
                        "projects_with_hits",
                    ));
                }
                _ => return Err(ClientCommandError::GatewayCapabilitiesUnavailable),
            }
        }
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
            InspectQuery::Projects {
                maximum_results,
                description_characters,
                ..
            }
            | InspectQuery::ProjectsWithHits {
                maximum_results,
                description_characters,
                ..
            },
            Inspection::Projects {
                projects,
                truncated,
                ..
            },
        ) => {
            use agent_knowledge_protocol::ProjectSearchScope;
            let (documents, hit_options, archived) = match &request.query {
                InspectQuery::Projects {
                    search_in,
                    include_archived,
                    ..
                } => (
                    *search_in == ProjectSearchScope::Documents,
                    None,
                    *include_archived,
                ),
                InspectQuery::ProjectsWithHits {
                    hits_per_project,
                    excerpt_characters,
                    include_archived,
                    ..
                } => (
                    true,
                    Some((*hits_per_project, *excerpt_characters)),
                    *include_archived,
                ),
                _ => unreachable!(),
            };
            let mut seen = std::collections::HashSet::new();
            projects.len() <= *maximum_results
                && (!truncated || projects.len() == *maximum_results)
                && projects.iter().all(|p| {
                    let valid_hits = match (hit_options, &p.hits, p.hits_truncated) {
                        (None, None, None) => true,
                        (Some((maximum, characters)), Some(hits), Some(truncated)) => {
                            let count = p.matching_documents.unwrap_or(0);
                            let mut ids = std::collections::HashSet::new();
                            hits.len() == count.min(maximum)
                                && truncated == (count > hits.len())
                                && hits.iter().all(|hit| {
                                    hit.document.project.as_ref() == Some(&p.project)
                                        && (archived || !hit.document.archived)
                                        && ids.insert(hit.document.metadata.document_id)
                                        && hit.excerpts.iter().all(|excerpt| {
                                            excerpt.text.chars().count() <= characters
                                        })
                                })
                        }
                        _ => false,
                    };
                    valid_hits
                        && p.document_count > 0
                        && seen.insert(&p.project)
                        && p.description.chars().count() <= *description_characters
                        && p.matching_documents.is_some() == documents
                        && p.matching_documents
                            .is_none_or(|n| n > 0 && n <= p.document_count)
                        && p.index.as_ref().map_or_else(
                            || p.description.is_empty() && !p.description_truncated,
                            |d| {
                                d.project.as_ref() == Some(&p.project)
                                    && d.document_type == agent_knowledge_core::DocumentType::Index
                            },
                        )
                })
                && projects.windows(2).all(|pair| {
                    if documents {
                        pair[0].matching_documents > pair[1].matching_documents
                            || (pair[0].matching_documents == pair[1].matching_documents
                                && pair[0].project < pair[1].project)
                    } else {
                        pair[0].project < pair[1].project
                    }
                })
        }
        (
            InspectQuery::SearchExcerpts {
                maximum_results,
                excerpt_characters,
                filter,
                ..
            },
            Inspection::SearchExcerpts { hits, .. },
        ) => {
            hits.len() <= *maximum_results
                && hits.iter().all(|h| {
                    filter
                        .project
                        .as_ref()
                        .is_none_or(|p| h.document.project.as_ref() == Some(p))
                        && filter.projects.as_ref().is_none_or(|projects| {
                            h.document
                                .project
                                .as_ref()
                                .is_some_and(|p| projects.contains(p))
                        })
                        && h.excerpts
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

#[cfg(test)]
mod project_tests {
    use super::*;

    #[test]
    fn discovery_rejects_impossible_counts_order_and_overlong_descriptions() {
        let request: InspectRequest = serde_json::from_value(serde_json::json!({"protocol_version":1,"query":{"operation":"projects","query":"needle","search_in":"documents","maximum_results":2,"description_characters":3,"include_archived":false}})).unwrap_or_else(|e| panic!("request: {e}"));
        let project = serde_json::json!({"project":"fictional-a","index":null,"description":"","description_truncated":false,"document_count":3,"matching_documents":2});
        let wire = serde_json::json!({"protocol_version":1,"result":{"operation":"projects","commit":"a".repeat(40),"projects":[project],"truncated":false}});
        let response: InspectResponse =
            serde_json::from_value(wire.clone()).unwrap_or_else(|e| panic!("response: {e}"));
        assert!(validate(&request, &response).is_ok());
        let mut bad = wire.clone();
        bad["result"]["projects"][0]["matching_documents"] = 4.into();
        let mut order = wire.clone();
        order["result"]["projects"] = serde_json::json!([project, project]);
        let mut description = wire.clone();
        description["result"]["projects"][0]["description"] = "long text".into();
        let mut mode = wire;
        mode["result"]["projects"][0]["matching_documents"] = serde_json::Value::Null;
        for value in [bad, order, description, mode] {
            let response =
                serde_json::from_value(value).unwrap_or_else(|e| panic!("response: {e}"));
            assert!(validate(&request, &response).is_err());
        }
    }
    #[test]
    fn rejects_malformed_project_hits_and_unrequested_samples() {
        let document = serde_json::json!({"path":"projects/fictional-project/index.md","document_type":"index","project":"fictional-project","archived":false,"revision":format!("sha256:{}", "a".repeat(64)),"metadata":{"schema_version":1,"document_id":"01K00000000000000000000001","title":"Fictional project","created":"2026-09-24T00:00:00Z","request_id":"01K00000000000000000000002","status":"active"}});
        let request = InspectRequest {
            protocol_version: 1,
            query: InspectQuery::ProjectsWithHits {
                query: "needle".into(),
                maximum_results: 10,
                description_characters: 30,
                include_archived: false,
                hits_per_project: 2,
                excerpt_characters: 3,
            },
        };
        let mut second = document.clone();
        second["metadata"]["document_id"] = "01K00000000000000000000003".into();
        let hit = serde_json::json!({"document":document,"excerpts":[{"field":"body","text":"針の例","truncated":true}]});
        let project = serde_json::json!({"project":"fictional-project","index":null,"description":"","description_truncated":false,"document_count":3,"matching_documents":3,"hits":[hit,{"document":second,"excerpts":[]}],"hits_truncated":true});
        let decode = |project| {
            serde_json::from_value::<InspectResponse>(serde_json::json!({"protocol_version":1,"result":{"operation":"projects","commit":"a".repeat(40),"projects":[project],"truncated":false}})).unwrap_or_else(|e| panic!("JSON: {e}"))
        };
        assert!(validate(&request, &decode(project.clone())).is_ok());
        for (pointer, value) in [
            (
                "/hits/0/document/project",
                serde_json::json!("fictional-other"),
            ),
            ("/hits/0/document/archived", serde_json::json!(true)),
            ("/hits/0/excerpts/0/text", serde_json::json!("四文字超え")),
            ("/hits_truncated", serde_json::json!(false)),
            ("/hits", serde_json::json!([hit])),
            ("/hits", serde_json::json!([hit, hit])),
            ("/hits", serde_json::Value::Null),
            ("/hits_truncated", serde_json::Value::Null),
        ] {
            let mut invalid = project.clone();
            *invalid
                .pointer_mut(pointer)
                .unwrap_or_else(|| panic!("pointer")) = value;
            assert!(validate(&request, &decode(invalid)).is_err(), "{pointer}");
        }
        let legacy = InspectRequest {
            protocol_version: 1,
            query: InspectQuery::Projects {
                query: Some("needle".into()),
                search_in: agent_knowledge_protocol::ProjectSearchScope::Documents,
                maximum_results: 10,
                description_characters: 30,
                include_archived: false,
            },
        };
        assert!(validate(&legacy, &decode(project)).is_err());
    }
}
