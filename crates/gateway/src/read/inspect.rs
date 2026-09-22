//! Read-only inspection of a committed snapshot or official ancestry.
use super::*;
use agent_knowledge_core::{DocumentId, markdown_body};
use agent_knowledge_protocol::{
    Excerpt, InspectQuery, InspectRequest, InspectResponse, Inspection, SearchHit,
};
use agent_knowledge_repository::{CommittedSnapshot, TantivySearchIndex, linear_excerpts};

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
        _ => return Err(invalid()),
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
