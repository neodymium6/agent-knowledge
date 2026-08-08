use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use agent_knowledge_core::read_bounded_regular_file;
use serde::{Deserialize, Serialize};
use tantivy::Index;

use super::{SearchFields, TantivySearchError, TantivySearchIndex};
use crate::{CommittedSnapshot, SearchMetadataFields};

const FORMAT_VERSION: u16 = 2;
const INDEX_DIRECTORY: &str = "tantivy";
const MANIFEST_FILE: &str = ".agent-knowledge-search-index.json";
const MAXIMUM_MANIFEST_BYTES: u64 = 16 * 1024;
#[cfg(unix)]
const INDEX_FILE_MODE: u32 = 0o640;

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DiskManifest {
    format_version: u16,
    commit: String,
    document_count: u64,
    metadata_fields: DiskMetadataFields,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DiskMetadataFields {
    node: bool,
    agent: bool,
    session: bool,
    request_id: bool,
}

impl TantivySearchIndex {
    /// Builds a durable index in a newly created directory.
    ///
    /// The parent directory must already exist and `directory` itself must not.
    /// A format manifest is written only after Tantivy has committed and synced
    /// the complete index. A failed build may leave an incomplete directory for
    /// its staging owner to discard or inspect. On Unix, generated regular files
    /// are made group-readable before the staging directory can be promoted.
    ///
    /// # Errors
    ///
    /// Returns an error when the destination cannot be created, committed
    /// Markdown changes while indexing, or Tantivy cannot persist the index.
    pub fn build_in_directory(
        snapshot: &CommittedSnapshot,
        metadata_fields: SearchMetadataFields,
        directory: impl AsRef<Path>,
    ) -> Result<Self, TantivySearchError> {
        let directory = directory.as_ref();
        fs::create_dir(directory).map_err(TantivySearchError::io)?;
        sync_parent(directory)?;
        let index_directory = directory.join(INDEX_DIRECTORY);
        fs::create_dir(&index_directory).map_err(TantivySearchError::io)?;

        let (schema, query_schema, fields) = SearchFields::schemas(metadata_fields);
        let index =
            Index::create_in_dir(&index_directory, schema).map_err(TantivySearchError::engine)?;
        let built = Self::build(snapshot, metadata_fields, index, query_schema, fields)?;
        sync_directory(&index_directory)?;
        let document_count = u64::try_from(built.document_count)
            .map_err(|_| TantivySearchError::InvalidDiskManifest)?;
        let manifest = DiskManifest {
            format_version: FORMAT_VERSION,
            commit: built.commit.clone(),
            document_count,
            metadata_fields: metadata_fields.into(),
        };
        write_manifest(directory, &manifest)?;
        normalize_index_file_permissions(directory)?;
        sync_parent(directory)?;
        Ok(built)
    }

    /// Opens and validates a completed durable index.
    ///
    /// # Errors
    ///
    /// Returns an error when the manifest or Tantivy files are missing,
    /// malformed, incompatible, or disagree about the indexed document count.
    pub fn open_directory(directory: impl AsRef<Path>) -> Result<Self, TantivySearchError> {
        Self::open_directory_with(directory.as_ref(), || {}, None)
    }

    fn open_directory_with(
        directory: &Path,
        after_resolve: impl FnOnce(),
        directory_anchor: Option<Arc<std::fs::File>>,
    ) -> Result<Self, TantivySearchError> {
        let directory = if directory_anchor.is_some() {
            directory.to_owned()
        } else {
            fs::canonicalize(directory).map_err(TantivySearchError::io)?
        };
        after_resolve();
        let manifest = read_manifest(&directory)?;
        let metadata_fields = SearchMetadataFields::from(manifest.metadata_fields);
        let (expected_schema, query_schema, fields) = SearchFields::schemas(metadata_fields);
        let index = Index::open_in_dir(directory.join(INDEX_DIRECTORY))
            .map_err(TantivySearchError::engine)?;
        if index
            .load_metas()
            .map_err(TantivySearchError::engine)?
            .payload
            .as_deref()
            != Some(manifest.commit.as_str())
        {
            return Err(TantivySearchError::DiskCommitMismatch);
        }
        if index.schema() != expected_schema {
            return Err(TantivySearchError::DiskSchemaMismatch);
        }
        let reader = index.reader().map_err(TantivySearchError::engine)?;
        let indexed_documents = reader.searcher().num_docs();
        if indexed_documents != manifest.document_count {
            return Err(TantivySearchError::DiskDocumentCountMismatch {
                manifest: manifest.document_count,
                index: indexed_documents,
            });
        }
        let document_count = usize::try_from(manifest.document_count)
            .map_err(|_| TantivySearchError::InvalidDiskManifest)?;
        Ok(Self {
            commit: manifest.commit,
            document_count,
            index,
            query_schema,
            reader,
            fields,
            _directory_anchor: directory_anchor,
        })
    }

    pub(super) fn open_pinned_directory(
        directory: &Path,
        directory_anchor: Arc<std::fs::File>,
    ) -> Result<Self, TantivySearchError> {
        Self::open_directory_with(directory, || {}, Some(directory_anchor))
    }

    #[cfg(test)]
    pub(crate) fn open_directory_after_resolve(
        directory: impl AsRef<Path>,
        after_resolve: impl FnOnce(),
    ) -> Result<Self, TantivySearchError> {
        Self::open_directory_with(directory.as_ref(), after_resolve, None)
    }
}

impl From<SearchMetadataFields> for DiskMetadataFields {
    fn from(fields: SearchMetadataFields) -> Self {
        Self {
            node: fields.node(),
            agent: fields.agent(),
            session: fields.session(),
            request_id: fields.request_id(),
        }
    }
}

impl From<DiskMetadataFields> for SearchMetadataFields {
    fn from(fields: DiskMetadataFields) -> Self {
        Self::new(fields.node, fields.agent, fields.session, fields.request_id)
    }
}

fn read_manifest(directory: &Path) -> Result<DiskManifest, TantivySearchError> {
    let bytes = read_bounded_regular_file(directory.join(MANIFEST_FILE), MAXIMUM_MANIFEST_BYTES)
        .map_err(|error| match error {
            agent_knowledge_core::BoundedFileError::Io(error) => TantivySearchError::io(error),
            agent_knowledge_core::BoundedFileError::InvalidFileType
            | agent_knowledge_core::BoundedFileError::FileTooLarge { .. } => {
                TantivySearchError::InvalidDiskManifest
            }
        })?;
    let manifest = serde_json::from_slice::<DiskManifest>(&bytes)
        .map_err(|_| TantivySearchError::InvalidDiskManifest)?;
    if manifest.format_version != FORMAT_VERSION || !valid_commit(&manifest.commit) {
        return Err(TantivySearchError::InvalidDiskManifest);
    }
    Ok(manifest)
}

fn write_manifest(directory: &Path, manifest: &DiskManifest) -> Result<(), TantivySearchError> {
    let mut bytes =
        serde_json::to_vec(manifest).map_err(|_| TantivySearchError::InvalidDiskManifest)?;
    bytes.push(b'\n');
    if bytes.len() as u64 > MAXIMUM_MANIFEST_BYTES {
        return Err(TantivySearchError::InvalidDiskManifest);
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(directory.join(MANIFEST_FILE))
        .map_err(TantivySearchError::io)?;
    file.write_all(&bytes).map_err(TantivySearchError::io)?;
    file.sync_all().map_err(TantivySearchError::io)?;
    File::open(directory)
        .and_then(|directory| directory.sync_all())
        .map_err(TantivySearchError::io)
}

fn sync_parent(path: &Path) -> Result<(), TantivySearchError> {
    let parent = path
        .parent()
        .ok_or(TantivySearchError::InvalidDiskManifest)?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(TantivySearchError::io)
}

fn sync_directory(path: &Path) -> Result<(), TantivySearchError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(TantivySearchError::io)
}

#[cfg(unix)]
fn normalize_index_file_permissions(path: &Path) -> Result<(), TantivySearchError> {
    let metadata = fs::symlink_metadata(path).map_err(TantivySearchError::io)?;
    if metadata.file_type().is_dir() {
        for entry in fs::read_dir(path).map_err(TantivySearchError::io)? {
            let entry = entry.map_err(TantivySearchError::io)?;
            normalize_index_file_permissions(&entry.path())?;
        }
        sync_directory(path)
    } else if metadata.file_type().is_file() {
        fs::set_permissions(path, fs::Permissions::from_mode(INDEX_FILE_MODE))
            .map_err(TantivySearchError::io)?;
        File::open(path)
            .and_then(|file| file.sync_all())
            .map_err(TantivySearchError::io)
    } else {
        Err(TantivySearchError::InvalidDiskManifest)
    }
}

#[cfg(not(unix))]
fn normalize_index_file_permissions(_path: &Path) -> Result<(), TantivySearchError> {
    Ok(())
}

fn valid_commit(commit: &str) -> bool {
    matches!(commit.len(), 40 | 64)
        && commit
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}
