use std::fmt;
use std::time::Instant;

use agent_knowledge_core::{DocumentId, markdown_body};
use tantivy::collector::TopDocs;
use tantivy::query::{BooleanQuery, Occur, Query, QueryParser, QueryParserError, TermQuery};
use tantivy::schema::{
    Field, IndexRecordOption, STORED, STRING, Schema, TEXT, TantivyDocument, Term, Value,
};
use tantivy::{Index, IndexReader};

use crate::{
    CommittedReadError, CommittedSnapshot, DocumentRecord, ReadFilter, SearchMetadataFields,
    SearchPolicy,
};

// Tantivy 0.26 requires at least 15 MB per indexing thread. Keep the initial
// primitive single-threaded and at that minimum until deployment measurements
// justify a configurable larger budget.
const INDEX_WRITER_MEMORY_BYTES: usize = 15_000_000;

/// An in-memory Tantivy index built from one exact committed snapshot.
///
/// This is the repository-layer primitive used to validate schema and query
/// semantics before a later storage layer publishes persistent derived
/// indexes. Markdown remains the canonical data source.
pub struct TantivySearchIndex {
    commit: String,
    index: Index,
    reader: IndexReader,
    fields: SearchFields,
}

impl TantivySearchIndex {
    /// Builds a new in-memory index from every validated Markdown document in
    /// one committed snapshot.
    ///
    /// The standard Tantivy tokenizer is used. It lowercases text and splits
    /// on punctuation and whitespace; no Japanese tokenizer is registered.
    ///
    /// # Errors
    ///
    /// Returns an error when committed Markdown changes during the build, is
    /// not UTF-8, or Tantivy cannot construct and commit the index.
    pub fn build_in_memory(
        snapshot: &CommittedSnapshot,
        metadata_fields: SearchMetadataFields,
    ) -> Result<Self, TantivySearchError> {
        let (schema, fields) = SearchFields::schema(metadata_fields);
        let index = Index::create_in_ram(schema);
        let mut writer = index
            .writer_with_num_threads(1, INDEX_WRITER_MEMORY_BYTES)
            .map_err(TantivySearchError::engine)?;
        let mut records = snapshot.documents().collect::<Vec<_>>();
        records.sort_by(|left, right| left.relative_path().cmp(right.relative_path()));
        for record in records {
            let markdown = snapshot
                .read_markdown(record)
                .map_err(TantivySearchError::committed)?;
            let body = markdown_body(&markdown).map_err(|_| {
                TantivySearchError::committed(CommittedReadError::InvalidMarkdownEncoding {
                    document_id: record.metadata().document_id,
                })
            })?;
            writer
                .add_document(fields.document(record, body, metadata_fields))
                .map_err(TantivySearchError::engine)?;
        }
        writer.commit().map_err(TantivySearchError::engine)?;
        writer
            .wait_merging_threads()
            .map_err(TantivySearchError::engine)?;
        let reader = index.reader().map_err(TantivySearchError::engine)?;
        Ok(Self {
            commit: snapshot.commit().to_owned(),
            index,
            reader,
            fields,
        })
    }

    /// Returns the exact committed revision represented by this index.
    #[must_use]
    pub fn commit(&self) -> &str {
        &self.commit
    }

    /// Searches the index and resolves ranked hits against the same committed
    /// snapshot used to build it.
    ///
    /// Terms are combined as a conjunction by default. Exact project, tag,
    /// session, and archive filters are applied inside the Tantivy query.
    /// Equal BM25 scores are ordered by canonical path.
    ///
    /// # Errors
    ///
    /// Returns an error for a mismatched snapshot, invalid bounds or query
    /// syntax, deadline expiry, corrupt stored identities, or Tantivy failure.
    pub fn search<'a>(
        &self,
        snapshot: &'a CommittedSnapshot,
        query: &str,
        filter: &ReadFilter,
        policy: SearchPolicy,
    ) -> Result<Vec<&'a DocumentRecord>, TantivySearchError> {
        check_deadline(policy.deadline)?;
        if snapshot.commit() != self.commit {
            return Err(TantivySearchError::SnapshotCommitMismatch {
                index: self.commit.clone(),
                snapshot: snapshot.commit().to_owned(),
            });
        }
        if policy.maximum_results == 0 {
            return Err(TantivySearchError::InvalidResultLimit);
        }
        let query = query.trim();
        if query.is_empty() {
            return Err(TantivySearchError::EmptyQuery);
        }
        let actual = query.chars().count();
        if actual > policy.maximum_query_characters {
            return Err(TantivySearchError::QueryTooLong {
                maximum: policy.maximum_query_characters,
                actual,
            });
        }

        let mut parser = QueryParser::for_index(&self.index, self.fields.searchable.clone());
        parser.set_conjunction_by_default();
        parser.set_field_boost(self.fields.title, 4.0);
        parser.set_field_boost(self.fields.tags, 3.0);
        parser.set_field_boost(self.fields.path, 2.0);
        let parsed = parser
            .parse_query(query)
            .map_err(TantivySearchError::Query)?;
        let filtered = self.fields.filtered_query(parsed, filter);
        check_deadline(policy.deadline)?;

        let searcher = self.reader.searcher();
        let hits = searcher
            .search(
                &filtered,
                &TopDocs::with_limit(policy.maximum_results).order_by_score(),
            )
            .map_err(TantivySearchError::engine)?;
        check_deadline(policy.deadline)?;

        let mut records = Vec::with_capacity(hits.len());
        for (score, address) in hits {
            check_deadline(policy.deadline)?;
            let stored = searcher
                .doc::<TantivyDocument>(address)
                .map_err(TantivySearchError::engine)?;
            let document_id = stored
                .get_first(self.fields.document_id)
                .and_then(|value| value.as_str())
                .ok_or(TantivySearchError::MissingDocumentId)?
                .parse::<DocumentId>()
                .map_err(|_| TantivySearchError::InvalidDocumentId)?;
            let record = snapshot
                .document(document_id)
                .ok_or(TantivySearchError::UnknownDocumentId { document_id })?;
            records.push((score, record));
        }
        records.sort_by(|(left_score, left), (right_score, right)| {
            right_score
                .total_cmp(left_score)
                .then_with(|| left.relative_path().cmp(right.relative_path()))
        });
        Ok(records.into_iter().map(|(_, record)| record).collect())
    }
}

impl fmt::Debug for TantivySearchIndex {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TantivySearchIndex")
            .field("commit", &self.commit)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
struct SearchFields {
    document_id: Field,
    title: Field,
    body: Field,
    path: Field,
    tags: Field,
    exact_tags: Field,
    node: Field,
    agent: Field,
    session: Field,
    exact_session: Field,
    request_id: Field,
    project: Field,
    archived: Field,
    searchable: Vec<Field>,
}

impl SearchFields {
    fn schema(metadata_fields: SearchMetadataFields) -> (Schema, Self) {
        let mut schema = Schema::builder();
        let document_id = schema.add_text_field("document_id", STRING | STORED);
        let title = schema.add_text_field("title", TEXT);
        let body = schema.add_text_field("body", TEXT);
        let path = schema.add_text_field("path", TEXT);
        let tags = schema.add_text_field("tags", TEXT);
        let exact_tags = schema.add_text_field("exact_tags", STRING);
        let node = schema.add_text_field("node", TEXT);
        let agent = schema.add_text_field("agent", TEXT);
        let session = schema.add_text_field("session", TEXT);
        let exact_session = schema.add_text_field("exact_session", STRING);
        let request_id = schema.add_text_field("request_id", TEXT);
        let project = schema.add_text_field("project", STRING);
        let archived = schema.add_bool_field("archived", tantivy::schema::INDEXED);
        let schema = schema.build();
        let mut searchable = vec![title, body, path, tags];
        if metadata_fields.node() {
            searchable.push(node);
        }
        if metadata_fields.agent() {
            searchable.push(agent);
        }
        if metadata_fields.session() {
            searchable.push(session);
        }
        if metadata_fields.request_id() {
            searchable.push(request_id);
        }
        (
            schema,
            Self {
                document_id,
                title,
                body,
                path,
                tags,
                exact_tags,
                node,
                agent,
                session,
                exact_session,
                request_id,
                project,
                archived,
                searchable,
            },
        )
    }

    fn document(
        &self,
        record: &DocumentRecord,
        body: &str,
        metadata_fields: SearchMetadataFields,
    ) -> TantivyDocument {
        let metadata = record.metadata();
        let mut document = TantivyDocument::default();
        document.add_text(self.document_id, metadata.document_id.to_string());
        document.add_text(self.title, &metadata.title);
        document.add_text(self.body, body);
        document.add_text(self.path, record.relative_path().to_string_lossy());
        for tag in &metadata.tags {
            document.add_text(self.tags, tag);
            document.add_text(self.exact_tags, tag);
        }
        if metadata_fields.node()
            && let Some(node) = &metadata.node
        {
            document.add_text(self.node, node);
        }
        if metadata_fields.agent()
            && let Some(agent) = &metadata.agent
        {
            document.add_text(self.agent, agent);
        }
        if let Some(session) = metadata.session {
            let session = session.to_string();
            if metadata_fields.session() {
                document.add_text(self.session, &session);
            }
            document.add_text(self.exact_session, session);
        }
        if metadata_fields.request_id() {
            document.add_text(self.request_id, metadata.request_id.to_string());
        }
        if let Some(project) = record.location().project() {
            document.add_text(self.project, project.as_str());
        }
        document.add_bool(self.archived, record.location().is_archived());
        document
    }

    fn filtered_query(&self, query: Box<dyn Query>, filter: &ReadFilter) -> BooleanQuery {
        let mut clauses = vec![(Occur::Must, query)];
        if let Some(project) = filter.project() {
            clauses.push((Occur::Must, self.term_query(self.project, project.as_str())));
        }
        if let Some(tag) = filter.tag() {
            clauses.push((Occur::Must, self.term_query(self.exact_tags, tag)));
        }
        if let Some(session) = filter.session() {
            clauses.push((
                Occur::Must,
                self.term_query(self.exact_session, &session.to_string()),
            ));
        }
        if !filter.include_archived() {
            clauses.push((
                Occur::MustNot,
                Box::new(TermQuery::new(
                    Term::from_field_bool(self.archived, true),
                    IndexRecordOption::Basic,
                )),
            ));
        }
        BooleanQuery::new(clauses)
    }

    fn term_query(&self, field: Field, value: &str) -> Box<dyn Query> {
        Box::new(TermQuery::new(
            Term::from_field_text(field, value),
            IndexRecordOption::Basic,
        ))
    }
}

fn check_deadline(deadline: Option<Instant>) -> Result<(), TantivySearchError> {
    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        Err(TantivySearchError::OperationDeadlineExceeded)
    } else {
        Ok(())
    }
}

/// Failure while building or querying a Tantivy-derived search index.
#[derive(Debug)]
pub enum TantivySearchError {
    /// Reading the exact committed Markdown snapshot failed.
    Committed(Box<CommittedReadError>),
    /// Tantivy failed to build, commit, read, or query its index.
    Engine(Box<tantivy::TantivyError>),
    /// The user-facing Tantivy query syntax was invalid.
    Query(QueryParserError),
    /// The query was empty after trimming.
    EmptyQuery,
    /// The query exceeded the configured character bound.
    QueryTooLong {
        /// Maximum accepted Unicode scalar values.
        maximum: usize,
        /// Supplied Unicode scalar values.
        actual: usize,
    },
    /// The requested result limit was zero.
    InvalidResultLimit,
    /// The absolute operation deadline expired.
    OperationDeadlineExceeded,
    /// The index and supplied snapshot represent different commits.
    SnapshotCommitMismatch {
        /// Commit represented by the index.
        index: String,
        /// Commit represented by the supplied snapshot.
        snapshot: String,
    },
    /// One hit did not contain its required stored document identity.
    MissingDocumentId,
    /// One hit contained a malformed stored document identity.
    InvalidDocumentId,
    /// One valid stored identity was absent from the matching snapshot.
    UnknownDocumentId {
        /// Identity missing from the committed snapshot.
        document_id: DocumentId,
    },
}

impl TantivySearchError {
    fn committed(error: CommittedReadError) -> Self {
        Self::Committed(Box::new(error))
    }

    fn engine(error: tantivy::TantivyError) -> Self {
        Self::Engine(Box::new(error))
    }
}

impl fmt::Display for TantivySearchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Committed(error) => write!(formatter, "committed search input failed: {error}"),
            Self::Engine(error) => write!(formatter, "Tantivy search index failed: {error}"),
            Self::Query(error) => write!(formatter, "search query is invalid: {error}"),
            Self::EmptyQuery => formatter.write_str("search query must not be empty"),
            Self::QueryTooLong { maximum, actual } => write!(
                formatter,
                "search query has {actual} characters; maximum is {maximum}"
            ),
            Self::InvalidResultLimit => formatter.write_str("maximum results must be positive"),
            Self::OperationDeadlineExceeded => formatter.write_str("search deadline expired"),
            Self::SnapshotCommitMismatch { index, snapshot } => write!(
                formatter,
                "search index commit `{index}` does not match snapshot commit `{snapshot}`"
            ),
            Self::MissingDocumentId => {
                formatter.write_str("search hit has no stored document identity")
            }
            Self::InvalidDocumentId => {
                formatter.write_str("search hit has an invalid stored document identity")
            }
            Self::UnknownDocumentId { document_id } => write!(
                formatter,
                "search hit document `{document_id}` is absent from the committed snapshot"
            ),
        }
    }
}

impl std::error::Error for TantivySearchError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Committed(error) => Some(error),
            Self::Engine(error) => Some(error),
            Self::Query(error) => Some(error),
            _ => None,
        }
    }
}
