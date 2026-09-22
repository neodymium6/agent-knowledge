//! Read-only inspection of a committed snapshot or official ancestry.
use super::*;
use agent_knowledge_core::{DocumentId, DocumentStatus, DocumentType, markdown_body};
use agent_knowledge_protocol::{
    BodyDiff, ContextDocument, Excerpt, HistoryEntry, InspectQuery, InspectRequest,
    InspectResponse, Inspection, SearchHit,
};
use agent_knowledge_repository::{
    CommittedSnapshot, HistoricalDocument, HistoryReader, TantivySearchIndex, linear_excerpts,
};
use std::collections::HashSet;

const HISTORY_SCAN_COMMITS: usize = 100;

pub(crate) fn inspect_until(
    settings: &GatewaySettings,
    store: &CommittedStore,
    search_indexes: Option<PathAttestation>,
    request: &InspectRequest,
    deadline: Instant,
) -> Result<PreparedResponse<InspectResponse>, GatewayError> {
    validate_version(request.protocol_version)?;
    let result = match &request.query {
        InspectQuery::SearchExcerpts {
            query,
            filter,
            maximum_results,
            excerpt_characters,
        } => {
            validate_filter(filter)?;
            validate_result_limit(settings, *maximum_results)?;
            bounded(*excerpt_characters, 2_000)?;
            let snapshot = snapshot(settings, store, deadline)?;
            let settings = settings.clone();
            let query = query.clone();
            let filter = filter.clone();
            let maximum = *maximum_results;
            let characters = *excerpt_characters;
            run_until(
                move || {
                    let index = open_index(search_indexes, snapshot.commit())?;
                    let records = ranked(
                        &settings,
                        &snapshot,
                        index.as_ref(),
                        &query,
                        &filter,
                        maximum,
                        deadline,
                    )?;
                    let mut bytes_read = 0;
                    let hits = records
                        .into_iter()
                        .map(|record| {
                            check_deadline(deadline)?;
                            let document =
                                read_document(&settings, &snapshot, record, &mut bytes_read)?;
                            let body = body(&document.markdown, record.metadata().document_id)?;
                            let excerpts = excerpts(
                                &settings,
                                index.as_ref(),
                                record,
                                body,
                                &query,
                                characters,
                            )?;
                            Ok(SearchHit {
                                document: document.summary,
                                excerpts,
                            })
                        })
                        .collect::<Result<Vec<_>, GatewayError>>()?;
                    Ok(Inspection::SearchExcerpts {
                        commit: snapshot.commit().into(),
                        hits,
                    })
                },
                deadline,
            )?
        }
        InspectQuery::Context {
            project,
            query,
            maximum_documents,
            maximum_characters,
        } => {
            validate_result_limit(settings, *maximum_documents)?;
            bounded(*maximum_characters, 100_000)?;
            let snapshot = snapshot(settings, store, deadline)?;
            let settings = settings.clone();
            let filter = ReadFilterRequest {
                project: Some(project.clone()),
                ..ReadFilterRequest::default()
            };
            let query = query.clone();
            let maximum = *maximum_documents;
            let characters = *maximum_characters;
            run_until(
                move || {
                    context(
                        &settings,
                        snapshot,
                        search_indexes,
                        &filter,
                        query.as_deref(),
                        maximum,
                        characters,
                        deadline,
                    )
                },
                deadline,
            )?
        }
        InspectQuery::History {
            document_id,
            anchor_commit,
            cursor,
            maximum_results,
        } => {
            validate_result_limit(settings, *maximum_results)?;
            let mut reader = history_reader(settings, store, deadline)?;
            if let Some(anchor) = anchor_commit {
                reader.restrict_to(anchor).map_err(committed)?;
            }
            if cursor.is_some() && anchor_commit.is_none() {
                return Err(invalid());
            }
            history(
                &mut reader,
                *document_id,
                cursor.as_deref(),
                *maximum_results,
            )?
        }
        InspectQuery::GetAt {
            document_id,
            commit,
        } => {
            let mut reader = history_reader(settings, store, deadline)?;
            Inspection::GetAt {
                commit: commit.clone(),
                document: Box::new(historical(&mut reader, commit, *document_id)?),
            }
        }
        InspectQuery::Diff {
            document_id,
            from_commit,
            to_commit,
        } => {
            let mut reader = history_reader(settings, store, deadline)?;
            let before = historical(&mut reader, from_commit, *document_id)?;
            let after = historical(&mut reader, to_commit, *document_id)?;
            let diff = body_diff(
                body(&before.markdown, *document_id)?,
                body(&after.markdown, *document_id)?,
            );
            Inspection::Diff {
                from_commit: from_commit.clone(),
                to_commit: to_commit.clone(),
                before: Box::new(before.summary),
                after: Box::new(after.summary),
                body: diff,
            }
        }
    };
    prepare_response(
        settings,
        InspectResponse {
            protocol_version: CURRENT_GATEWAY_PROTOCOL_VERSION,
            result,
        },
        deadline,
    )
}
fn invalid() -> GatewayError {
    GatewayError::ReadRequest(ReadRequestError::InvalidInspection)
}
fn bounded(value: usize, maximum: usize) -> Result<(), GatewayError> {
    if value == 0 || value > maximum {
        Err(GatewayError::ReadRequest(
            ReadRequestError::InvalidResultLimit {
                maximum,
                actual: value,
            },
        ))
    } else {
        Ok(())
    }
}
fn check_deadline(deadline: Instant) -> Result<(), GatewayError> {
    if Instant::now() >= deadline {
        Err(GatewayError::OperationDeadlineExceeded)
    } else {
        Ok(())
    }
}
fn metadata_fields(settings: &GatewaySettings) -> SearchMetadataFields {
    let f = settings.search_metadata_fields();
    SearchMetadataFields::new(f[0], f[1], f[2], f[3])
}
fn body(markdown: &str, id: DocumentId) -> Result<&str, GatewayError> {
    markdown_body(markdown.as_bytes())
        .map_err(|_| committed(CommittedReadError::InvalidMarkdownEncoding { document_id: id }))
}
fn open_index(
    root: Option<PathAttestation>,
    commit: &str,
) -> Result<Option<TantivySearchIndex>, GatewayError> {
    root.map(|root| {
        let index = SearchIndexStore::open_active_read_only(root.stable_path())
            .map_err(|_| GatewayError::SearchIndexUnavailable)?
            .ok_or(GatewayError::SearchIndexUnavailable)?;
        if index.commit() != commit {
            return Err(GatewayError::SearchIndexUnavailable);
        }
        Ok(index)
    })
    .transpose()
}
fn search_error(error: TantivySearchError) -> GatewayError {
    match error {
        TantivySearchError::Query(_) => {
            GatewayError::ReadRequest(ReadRequestError::InvalidSearchQuery)
        }
        TantivySearchError::EmptyQuery => committed(CommittedReadError::EmptyQuery),
        TantivySearchError::QueryTooLong { maximum, actual } => {
            committed(CommittedReadError::QueryTooLong { maximum, actual })
        }
        TantivySearchError::DeadlineExceeded => GatewayError::OperationDeadlineExceeded,
        _ => GatewayError::SearchIndexUnavailable,
    }
}
#[allow(clippy::too_many_arguments)]
fn ranked<'a>(
    settings: &GatewaySettings,
    snapshot: &'a CommittedSnapshot,
    index: Option<&TantivySearchIndex>,
    query: &str,
    filter: &ReadFilterRequest,
    maximum: usize,
    deadline: Instant,
) -> Result<Vec<&'a DocumentRecord>, GatewayError> {
    if let Some(index) = index {
        index
            .search_with_metadata(
                snapshot,
                query,
                &repository_filter(filter),
                metadata_fields(settings),
                TantivySearchPolicy::new(settings.maximum_search_query_characters(), maximum)
                    .with_deadline(deadline),
            )
            .map_err(search_error)
    } else {
        LinearSearch::new(metadata_fields(settings))
            .search(
                snapshot,
                query,
                &repository_filter(filter),
                SearchPolicy {
                    maximum_query_characters: settings.maximum_search_query_characters(),
                    maximum_results: maximum,
                    maximum_scanned_documents: settings.maximum_search_documents(),
                    maximum_scanned_markdown_bytes: settings.maximum_search_markdown_bytes(),
                    deadline: Some(deadline),
                },
            )
            .map_err(committed)
    }
}
fn excerpts(
    settings: &GatewaySettings,
    index: Option<&TantivySearchIndex>,
    record: &DocumentRecord,
    body: &str,
    query: &str,
    maximum: usize,
) -> Result<Vec<Excerpt>, GatewayError> {
    let fields = metadata_fields(settings);
    let excerpts = if let Some(index) = index {
        index
            .excerpts(record, body, query, fields, maximum)
            .map_err(search_error)?
    } else {
        linear_excerpts(record, body, query, fields, maximum)
    };
    Ok(excerpts
        .into_iter()
        .map(|e| Excerpt {
            field: e.field,
            text: e.text,
            truncated: e.truncated,
        })
        .collect())
}
fn read_document(
    settings: &GatewaySettings,
    snapshot: &CommittedSnapshot,
    record: &DocumentRecord,
    bytes: &mut u64,
) -> Result<DocumentContent, GatewayError> {
    *bytes = bytes.saturating_add(record.byte_length());
    if *bytes > settings.maximum_search_markdown_bytes() {
        return Err(committed(
            CommittedReadError::SearchMarkdownByteLimitExceeded {
                maximum: settings.maximum_search_markdown_bytes(),
            },
        ));
    }
    let doc = snapshot
        .get(record.metadata().document_id)
        .map_err(committed)?;
    let markdown = std::str::from_utf8(doc.markdown())
        .map_err(|_| invalid())?
        .to_owned();
    Ok(DocumentContent {
        summary: document_summary(record)?,
        markdown,
    })
}
#[allow(clippy::too_many_arguments)]
fn context(
    settings: &GatewaySettings,
    snapshot: CommittedSnapshot,
    search_indexes: Option<PathAttestation>,
    filter: &ReadFilterRequest,
    query: Option<&str>,
    maximum: usize,
    characters: usize,
    deadline: Instant,
) -> Result<Inspection, GatewayError> {
    let eligible = |record: &&DocumentRecord| {
        !matches!(
            record.metadata().status,
            DocumentStatus::Deprecated | DocumentStatus::Archived
        ) && record.metadata().superseded_by.is_none()
    };
    let all = snapshot
        .list(&repository_filter(filter), settings.maximum_index_entries())
        .map_err(committed)?;
    let mut candidates = Vec::new();
    for record in all
        .iter()
        .copied()
        .filter(eligible)
        .filter(|r| r.location().document_type() == DocumentType::Index)
    {
        candidates.push((record, "project_index"));
    }
    let index = if query.is_some() {
        open_index(search_indexes, snapshot.commit())?
    } else {
        None
    };
    if let Some(query) = query {
        let ranked = ranked(
            settings,
            &snapshot,
            index.as_ref(),
            query,
            filter,
            settings.maximum_index_entries(),
            deadline,
        )?;
        for kind in [true, false] {
            for record in ranked.iter().copied().filter(eligible).filter(|r| {
                matches!(
                    r.location().document_type(),
                    DocumentType::Decision | DocumentType::Runbook
                ) == kind
            }) {
                candidates.push((
                    record,
                    if kind {
                        "related_guidance"
                    } else {
                        "query_match"
                    },
                ));
            }
        }
    } else {
        for record in all.iter().copied().filter(eligible).filter(|r| {
            matches!(
                r.location().document_type(),
                DocumentType::Decision | DocumentType::Runbook
            )
        }) {
            candidates.push((record, "project_guidance"));
        }
    }
    let recent = snapshot
        .recent(&repository_filter(filter), settings.maximum_index_entries())
        .map_err(committed)?;
    candidates.extend(
        recent
            .into_iter()
            .filter(eligible)
            .take(3)
            .map(|r| (r, "recent")),
    );
    let mut seen = HashSet::new();
    candidates.retain(|(r, _)| seen.insert(r.metadata().document_id));
    let mut documents = Vec::new();
    let mut additional = Vec::new();
    let mut remaining = characters;
    let mut bytes = 0;
    let mut truncated = false;
    for (record, reason) in candidates {
        check_deadline(deadline)?;
        if documents.len() >= maximum || remaining == 0 {
            truncated = true;
            if additional.len() < maximum {
                additional.push(document_summary(record)?);
            }
            continue;
        }
        let doc = read_document(settings, &snapshot, record, &mut bytes)?;
        let raw = body(&doc.markdown, record.metadata().document_id)?;
        let text = if raw.chars().count() <= remaining {
            raw.to_owned()
        } else if let Some(query) = query {
            excerpts(settings, index.as_ref(), record, raw, query, remaining)?
                .into_iter()
                .find(|e| e.field == "body")
                .map(|e| e.text)
                .unwrap_or_else(|| raw.chars().take(remaining).collect())
        } else {
            raw.chars().take(remaining).collect()
        };
        let shortened = text != raw;
        remaining = remaining.saturating_sub(text.chars().count());
        truncated |= shortened;
        documents.push(ContextDocument {
            document: doc.summary,
            reason: reason.into(),
            body: text,
            truncated: shortened,
        });
    }
    Ok(Inspection::Context {
        commit: snapshot.commit().into(),
        documents,
        additional,
        truncated,
    })
}
fn history_reader(
    settings: &GatewaySettings,
    store: &CommittedStore,
    deadline: Instant,
) -> Result<HistoryReader, GatewayError> {
    store
        .history_reader(ContentPolicy {
            maximum_entry_count: settings.maximum_index_entries(),
            maximum_total_markdown_bytes: settings.maximum_index_markdown_bytes(),
            scan_deadline: Some(deadline),
            ..ContentPolicy::default()
        })
        .map_err(committed)
}
fn historical(
    reader: &mut HistoryReader,
    commit: &str,
    id: DocumentId,
) -> Result<DocumentContent, GatewayError> {
    let doc = reader
        .document(commit, id)
        .map_err(committed)?
        .ok_or_else(|| committed(CommittedReadError::DocumentNotFound { document_id: id }))?;
    Ok(DocumentContent {
        summary: document_summary(&doc.record)?,
        markdown: doc.markdown,
    })
}
fn history(
    reader: &mut HistoryReader,
    id: DocumentId,
    cursor: Option<&str>,
    maximum: usize,
) -> Result<Inspection, GatewayError> {
    let anchor = reader.anchor().to_owned();
    if reader.document(&anchor, id).map_err(committed)?.is_none() {
        return Err(committed(CommittedReadError::DocumentNotFound {
            document_id: id,
        }));
    }
    let commits = reader
        .commits(cursor, HISTORY_SCAN_COMMITS + 1)
        .map_err(committed)?;
    let mut current = if let Some(commit) = commits.first() {
        reader.document(commit, id).map_err(committed)?
    } else {
        None
    };
    let mut entries = Vec::new();
    let mut next_cursor = None;
    for (i, commit) in commits.iter().take(HISTORY_SCAN_COMMITS).enumerate() {
        let previous = commits
            .get(i + 1)
            .map(|parent| reader.document(parent, id).map_err(committed))
            .transpose()?
            .flatten();
        if let Some(doc) = &current {
            let changed = previous.as_ref().is_none_or(|p: &HistoricalDocument| {
                p.record.revision() != doc.record.revision()
                    || p.record.relative_path() != doc.record.relative_path()
            });
            if changed {
                entries.push(HistoryEntry {
                    commit: commit.clone(),
                    document: document_summary(&doc.record)?,
                    previous_revision: previous.as_ref().map(|p| p.record.revision()),
                });
            }
        }
        next_cursor = commits.get(i + 1).cloned();
        if entries.len() >= maximum {
            break;
        }
        current = previous;
    }
    Ok(Inspection::History {
        anchor_commit: anchor,
        entries,
        next_cursor,
    })
}
fn body_diff(before: &str, after: &str) -> BodyDiff {
    let old = before.split_inclusive('\n').collect::<Vec<_>>();
    let new = after.split_inclusive('\n').collect::<Vec<_>>();
    let prefix = old.iter().zip(&new).take_while(|(a, b)| a == b).count();
    let suffix = old[prefix..]
        .iter()
        .rev()
        .zip(new[prefix..].iter().rev())
        .take_while(|(a, b)| a == b)
        .count();
    BodyDiff {
        from_line: prefix + 1,
        to_line: prefix + 1,
        removed: old[prefix..old.len() - suffix].concat(),
        added: new[prefix..new.len() - suffix].concat(),
    }
}

#[cfg(test)]
mod tests {
    use super::body_diff;
    #[test]
    fn body_ranges_preserve_unicode_newlines_insertions_and_removals() {
        for (old, new, line, removed, added) in [
            ("same\n", "same\n", 2, "", ""),
            ("first\nlast\n", "first\n追加\nlast\n", 2, "", "追加\n"),
            ("first\n削除\nlast", "first\nlast", 2, "削除\n", ""),
            ("last\n", "last", 1, "last\n", "last"),
            ("", "new", 1, "", "new"),
        ] {
            let diff = body_diff(old, new);
            assert_eq!(diff.from_line, line);
            assert_eq!(diff.to_line, line);
            assert_eq!(diff.removed, removed);
            assert_eq!(diff.added, added);
        }
    }
}
