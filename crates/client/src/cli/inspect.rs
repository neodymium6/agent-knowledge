use super::*;
use agent_knowledge_protocol::{CURRENT_GATEWAY_PROTOCOL_VERSION, InspectQuery, InspectRequest};
use std::collections::BTreeMap;

pub(super) fn parse(
    action: &str,
    mut args: impl Iterator<Item = OsString>,
) -> Result<Command, ParseError> {
    let mut values = BTreeMap::new();
    let mut archived = false;
    let mut projects = Vec::new();
    while let Some(flag) = args.next() {
        let flag = flag.into_string().map_err(|_| ParseError)?;
        if flag == "--include-archived"
            && matches!(action, "search-excerpts" | "projects")
            && !archived
        {
            archived = true;
            continue;
        }
        let value = args.next().ok_or(ParseError)?;
        if flag == "--project" && action == "search-excerpts" {
            projects.push(
                value
                    .to_str()
                    .ok_or(ParseError)?
                    .parse()
                    .map_err(|_| ParseError)?,
            );
            continue;
        }
        if values.insert(flag, value).is_some() {
            return Err(ParseError);
        }
    }
    let destination = values.remove("--destination").ok_or(ParseError)?;
    let timeout = values
        .remove("--timeout-seconds")
        .map(|s| parse_bounded_u64(&s, MAXIMUM_TIMEOUT_SECONDS))
        .transpose()?
        .unwrap_or(DEFAULT_TIMEOUT_SECONDS);
    let query = match action {
        "projects" => InspectQuery::Projects {
            query: optional(&mut values, "--query")?,
            search_in: match optional::<String>(&mut values, "--search-in")?
                .as_deref()
                .unwrap_or("project")
            {
                "project" => agent_knowledge_protocol::ProjectSearchScope::Project,
                "documents" => agent_knowledge_protocol::ProjectSearchScope::Documents,
                _ => return Err(ParseError),
            },
            maximum_results: number(&mut values, "--maximum-results", 100, MAXIMUM_READ_RESULTS)?,
            description_characters: number(&mut values, "--description-characters", 300, 2000)?,
            include_archived: archived,
        },
        "search-excerpts" => InspectQuery::SearchExcerpts {
            query: required(&mut values, "--query")?,
            filter: ReadFilterRequest {
                tag: optional(&mut values, "--tag")?,
                session: optional(&mut values, "--session")?,
                include_archived: archived,
                ..project_filter(projects)?
            },
            maximum_results: number(&mut values, "--maximum-results", 10, MAXIMUM_READ_RESULTS)?,
            excerpt_characters: number(&mut values, "--excerpt-characters", 300, 2000)?,
        },
        "context" => {
            let project = required(&mut values, "--project")?;
            let query = optional(&mut values, "--query")?;
            let maximum_documents =
                number(&mut values, "--maximum-documents", 10, MAXIMUM_READ_RESULTS)?;
            let maximum_characters = number(&mut values, "--maximum-characters", 20000, 100000)?;
            match optional::<String>(&mut values, "--selection")?
                .as_deref()
                .unwrap_or("relevance")
            {
                "relevance" => InspectQuery::Context {
                    project,
                    query,
                    maximum_documents,
                    maximum_characters,
                },
                "balanced" => {
                    let maximum_recent = maximum_documents.saturating_sub(2);
                    let recent_documents = zero_number(
                        &mut values,
                        "--recent-documents",
                        2.min(maximum_recent),
                        maximum_recent,
                    )?;
                    InspectQuery::ContextBalanced {
                        project,
                        query,
                        maximum_documents,
                        maximum_characters,
                        recent_documents,
                    }
                }
                _ => return Err(ParseError),
            }
        }
        "history" => InspectQuery::History {
            document_id: required(&mut values, "--document-id")?,
            anchor_commit: optional(&mut values, "--anchor-commit")?,
            cursor: optional(&mut values, "--cursor")?,
            maximum_results: number(&mut values, "--maximum-results", 20, MAXIMUM_READ_RESULTS)?,
        },
        "get-at" => InspectQuery::GetAt {
            document_id: required(&mut values, "--document-id")?,
            commit: required(&mut values, "--commit")?,
        },
        "diff" => {
            let document_id = required(&mut values, "--document-id")?;
            let from_commit = required(&mut values, "--from-commit")?;
            let to_commit = required(&mut values, "--to-commit")?;
            match optional::<String>(&mut values, "--format")?
                .as_deref()
                .unwrap_or("contiguous")
            {
                "contiguous" => InspectQuery::Diff {
                    document_id,
                    from_commit,
                    to_commit,
                },
                "hunks" => {
                    let context_lines = zero_number(&mut values, "--context-lines", 3, 20)?;
                    let maximum_hunks = number(&mut values, "--maximum-hunks", 20, 100)?;
                    let maximum_diff_bytes =
                        number(&mut values, "--maximum-diff-bytes", 64000, 1_000_000)?;
                    if maximum_diff_bytes < 256 {
                        return Err(ParseError);
                    }
                    InspectQuery::DiffHunks {
                        document_id,
                        from_commit,
                        to_commit,
                        context_lines,
                        maximum_hunks,
                        maximum_diff_bytes,
                    }
                }
                _ => return Err(ParseError),
            }
        }
        _ => return Err(ParseError),
    };
    if matches!(
        &query,
        InspectQuery::Projects {
            query: None,
            search_in: agent_knowledge_protocol::ProjectSearchScope::Documents,
            ..
        }
    ) {
        return Err(ParseError);
    }
    let query = if action == "projects" {
        let hits_per_project = zero_number(&mut values, "--hits-per-project", 0, 5)?;
        if hits_per_project > 0 {
            let excerpt_characters = number(&mut values, "--excerpt-characters", 300, 2000)?;
            match query {
                InspectQuery::Projects {
                    query: Some(query),
                    search_in: agent_knowledge_protocol::ProjectSearchScope::Documents,
                    maximum_results,
                    description_characters,
                    include_archived,
                } => InspectQuery::ProjectsWithHits {
                    query,
                    maximum_results,
                    description_characters,
                    include_archived,
                    hits_per_project,
                    excerpt_characters,
                },
                _ => return Err(ParseError),
            }
        } else {
            query
        }
    } else {
        query
    };
    if !values.is_empty() {
        return Err(ParseError);
    }
    Ok(Command::Inspect {
        destination,
        request: InspectRequest {
            protocol_version: CURRENT_GATEWAY_PROTOCOL_VERSION,
            query,
        },
        timeout: Duration::from_secs(timeout),
    })
}
fn optional<T: std::str::FromStr>(
    values: &mut BTreeMap<String, OsString>,
    flag: &str,
) -> Result<Option<T>, ParseError> {
    values
        .remove(flag)
        .map(|s| {
            s.to_str()
                .ok_or(ParseError)?
                .parse()
                .map_err(|_| ParseError)
        })
        .transpose()
}
fn required<T: std::str::FromStr>(
    values: &mut BTreeMap<String, OsString>,
    flag: &str,
) -> Result<T, ParseError> {
    optional(values, flag)?.ok_or(ParseError)
}
fn number(
    values: &mut BTreeMap<String, OsString>,
    flag: &str,
    default: usize,
    maximum: usize,
) -> Result<usize, ParseError> {
    values
        .remove(flag)
        .map(|s| parse_bounded_usize(&s, maximum))
        .transpose()
        .map(|v| v.unwrap_or(default))
}

fn zero_number(
    values: &mut BTreeMap<String, OsString>,
    flag: &str,
    default: usize,
    maximum: usize,
) -> Result<usize, ParseError> {
    let value = optional::<usize>(values, flag)?.unwrap_or(default);
    if value > maximum {
        Err(ParseError)
    } else {
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parses_inspection_commands_and_rejects_irrelevant_or_duplicate_flags() {
        let cases = [
            ("search-excerpts", vec!["--query", "fictional service"]),
            ("context", vec!["--project", "fictional-project"]),
            (
                "history",
                vec!["--document-id", "01K00000000000000000000001"],
            ),
            (
                "get-at",
                vec![
                    "--document-id",
                    "01K00000000000000000000001",
                    "--commit",
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                ],
            ),
            (
                "diff",
                vec![
                    "--document-id",
                    "01K00000000000000000000001",
                    "--from-commit",
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "--to-commit",
                    "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                ],
            ),
        ];
        for (action, flags) in cases {
            let mut args = vec!["--destination", "fictional-knowledge"];
            args.extend(flags);
            assert!(
                parse(action, args.iter().map(OsString::from)).is_ok(),
                "{action}"
            );
            args.extend(["--unknown", "value"]);
            assert!(parse(action, args.iter().map(OsString::from)).is_err());
        }
        for flags in [
            vec![
                "--project",
                "fictional-project",
                "--maximum-characters",
                "0",
            ],
            vec![
                "--project",
                "fictional-project",
                "--maximum-characters",
                "100001",
            ],
            vec![
                "--project",
                "fictional-project",
                "--project",
                "fictional-other",
            ],
            vec!["--project", "fictional-project", "--include-archived"],
        ] {
            let args = [vec!["--destination", "fictional-knowledge"], flags].concat();
            assert!(parse("context", args.iter().map(OsString::from)).is_err());
        }
    }
    #[test]
    fn parses_balanced_context_and_hunks_and_rejects_mixed_modes() {
        let base = [
            "--destination",
            "fictional-knowledge",
            "--project",
            "fictional-project",
        ];
        let args = [
            base.to_vec(),
            vec!["--selection", "balanced", "--recent-documents", "0"],
        ]
        .concat();
        let command =
            parse("context", args.iter().map(OsString::from)).unwrap_or_else(|_| panic!("parse"));
        assert!(matches!(
            command,
            Command::Inspect {
                request: InspectRequest {
                    query: InspectQuery::ContextBalanced {
                        recent_documents: 0,
                        ..
                    },
                    ..
                },
                ..
            }
        ));
        for flags in [
            vec!["--recent-documents", "1"],
            vec![
                "--selection",
                "balanced",
                "--maximum-documents",
                "3",
                "--recent-documents",
                "2",
            ],
            vec!["--selection", "unknown"],
        ] {
            assert!(
                parse(
                    "context",
                    [base.to_vec(), flags].concat().iter().map(OsString::from)
                )
                .is_err()
            );
        }
        let base = [
            "--destination",
            "fictional-knowledge",
            "--document-id",
            "01K00000000000000000000001",
            "--from-commit",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "--to-commit",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        ];
        let args = [
            base.to_vec(),
            vec!["--format", "hunks", "--context-lines", "0"],
        ]
        .concat();
        let command =
            parse("diff", args.iter().map(OsString::from)).unwrap_or_else(|_| panic!("parse"));
        assert!(matches!(
            command,
            Command::Inspect {
                request: InspectRequest {
                    query: InspectQuery::DiffHunks {
                        context_lines: 0,
                        ..
                    },
                    ..
                },
                ..
            }
        ));
        for flags in [
            vec!["--context-lines", "3"],
            vec!["--format", "hunks", "--maximum-diff-bytes", "255"],
            vec!["--format", "hunks", "--maximum-hunks", "0"],
            vec!["--format", "hunks", "--context-lines", "21"],
        ] {
            assert!(
                parse(
                    "diff",
                    [base.to_vec(), flags].concat().iter().map(OsString::from)
                )
                .is_err()
            );
        }
    }
}

#[cfg(test)]
mod project_hits_tests {
    use super::*;
    #[test]
    fn rejects_invalid_hit_options_and_keeps_zero_on_legacy_wire() {
        let base = [
            "--destination",
            "fictional-knowledge",
            "--query",
            "needle",
            "--search-in",
            "documents",
        ];
        for flags in [
            vec!["--hits-per-project", "6"],
            vec!["--hits-per-project", "-1"],
            vec!["--excerpt-characters", "30"],
            vec!["--hits-per-project", "0", "--excerpt-characters", "30"],
            vec!["--hits-per-project", "1", "--excerpt-characters", "0"],
            vec!["--hits-per-project", "1", "--excerpt-characters", "2001"],
        ] {
            assert!(
                parse(
                    "projects",
                    [base.to_vec(), flags].concat().iter().map(OsString::from)
                )
                .is_err()
            );
        }
        assert!(
            parse(
                "projects",
                [
                    "--destination",
                    "fictional-knowledge",
                    "--query",
                    "needle",
                    "--hits-per-project",
                    "1"
                ]
                .iter()
                .map(OsString::from)
            )
            .is_err()
        );
        let command = parse(
            "projects",
            [base.to_vec(), vec!["--hits-per-project", "0"]]
                .concat()
                .iter()
                .map(OsString::from),
        )
        .unwrap_or_else(|_| panic!("parse"));
        assert!(matches!(
            command,
            Command::Inspect {
                request: InspectRequest {
                    query: InspectQuery::Projects { .. },
                    ..
                },
                ..
            }
        ));
    }
}
