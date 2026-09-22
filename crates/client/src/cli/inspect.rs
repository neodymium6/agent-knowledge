use super::*;
use agent_knowledge_protocol::{CURRENT_GATEWAY_PROTOCOL_VERSION, InspectQuery, InspectRequest};
use std::collections::BTreeMap;

pub(super) fn parse(
    action: &str,
    mut args: impl Iterator<Item = OsString>,
) -> Result<Command, ParseError> {
    let mut values = BTreeMap::new();
    let mut archived = false;
    while let Some(flag) = args.next() {
        let flag = flag.into_string().map_err(|_| ParseError)?;
        if flag == "--include-archived" && action == "search-excerpts" && !archived {
            archived = true;
            continue;
        }
        let value = args.next().ok_or(ParseError)?;
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
        "search-excerpts" => InspectQuery::SearchExcerpts {
            query: required(&mut values, "--query")?,
            filter: ReadFilterRequest {
                project: optional(&mut values, "--project")?,
                tag: optional(&mut values, "--tag")?,
                session: optional(&mut values, "--session")?,
                include_archived: archived,
            },
            maximum_results: number(&mut values, "--maximum-results", 10, MAXIMUM_READ_RESULTS)?,
            excerpt_characters: number(&mut values, "--excerpt-characters", 300, 2000)?,
        },
        "context" => InspectQuery::Context {
            project: required(&mut values, "--project")?,
            query: optional(&mut values, "--query")?,
            maximum_documents: number(
                &mut values,
                "--maximum-documents",
                10,
                MAXIMUM_READ_RESULTS,
            )?,
            maximum_characters: number(&mut values, "--maximum-characters", 20000, 100000)?,
        },
        _ => return Err(ParseError),
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn parses_inspection_commands_and_rejects_irrelevant_or_duplicate_flags() {
        let cases = [
            ("search-excerpts", vec!["--query", "fictional service"]),
            ("context", vec!["--project", "fictional-project"]),
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
}
