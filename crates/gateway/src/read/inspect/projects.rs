//! Project discovery derived from one committed snapshot.
use super::*;
use agent_knowledge_core::ProjectId;
use agent_knowledge_protocol::{ProjectSearchScope, ProjectSummary};
use std::collections::BTreeMap;

#[allow(clippy::too_many_arguments)]
pub(super) fn list(
    settings: &GatewaySettings,
    snapshot: &CommittedSnapshot,
    search_indexes: Option<PathAttestation>,
    query: Option<&str>,
    search_in: ProjectSearchScope,
    maximum: usize,
    characters: usize,
    include_archived: bool,
    hit_options: Option<(usize, usize)>,
    deadline: Instant,
) -> Result<Inspection, GatewayError> {
    let filter = ReadFilter::new(None, None, None, include_archived);
    let records = snapshot
        .list(&filter, settings.maximum_index_entries())
        .map_err(committed)?;
    let mut groups: BTreeMap<ProjectId, (usize, Option<&DocumentRecord>)> = BTreeMap::new();
    for record in records {
        check_deadline(deadline)?;
        if let Some(project) = record.location().project() {
            let entry = groups.entry(project.clone()).or_default();
            entry.0 += 1;
            if record.location().document_type() == DocumentType::Index {
                entry.1 = Some(record);
            }
        }
    }
    let mut counts: BTreeMap<ProjectId, usize> = BTreeMap::new();
    let mut top_hits: BTreeMap<ProjectId, Vec<&DocumentRecord>> = BTreeMap::new();
    let index = if search_in == ProjectSearchScope::Documents {
        open_index(search_indexes, snapshot.commit())?
    } else {
        None
    };
    if search_in == ProjectSearchScope::Documents {
        let filter = ReadFilterRequest {
            include_archived,
            ..ReadFilterRequest::default()
        };
        // Collect all bounded matches before ranking projects, never per-project top hits.
        for record in ranked(
            settings,
            snapshot,
            index.as_ref(),
            query.ok_or_else(invalid)?,
            &filter,
            settings.maximum_index_entries(),
            deadline,
        )? {
            check_deadline(deadline)?;
            if let Some(project) = record.location().project() {
                *counts.entry(project.clone()).or_default() += 1;
                if let Some((maximum, _)) = hit_options {
                    let hits = top_hits.entry(project.clone()).or_default();
                    if hits.len() < maximum {
                        hits.push(record);
                    }
                }
            }
        }
    }
    let mut groups = groups.into_iter().collect::<Vec<_>>();
    if search_in == ProjectSearchScope::Documents {
        groups.retain(|(project, _)| counts.contains_key(project));
        groups.sort_by(|(a, _), (b, _)| counts[b].cmp(&counts[a]).then_with(|| a.cmp(b)));
    }
    let search_query = query;
    let query = query
        .filter(|_| search_in == ProjectSearchScope::Project)
        .map(|q| q.trim().to_lowercase());
    let mut projects = Vec::new();
    let mut bytes = 0;
    let mut truncated = false;
    for (project, (document_count, index_document)) in groups {
        check_deadline(deadline)?;
        if query.is_none() && projects.len() == maximum {
            truncated = true;
            break;
        }
        let document = index_document
            .map(|r| read_document(settings, snapshot, r, &mut bytes))
            .transpose()?;
        let raw = document
            .as_ref()
            .map(|d| body(&d.markdown, d.summary.metadata.document_id))
            .transpose()?
            .unwrap_or("");
        let matches = query.as_ref().is_none_or(|q| {
            project.as_str().contains(q)
                || document
                    .as_ref()
                    .is_some_and(|d| d.summary.metadata.title.to_lowercase().contains(q))
                || raw.to_lowercase().contains(q)
        });
        check_deadline(deadline)?;
        if !matches {
            continue;
        }
        if projects.len() == maximum {
            truncated = true;
            break;
        }
        let description = raw.chars().take(characters).collect::<String>();
        let description_truncated = description != raw;
        let matching_documents = counts.get(&project).copied();
        let hits = hit_options
            .map(|(_, characters)| {
                top_hits
                    .remove(&project)
                    .unwrap_or_default()
                    .into_iter()
                    .map(|record| {
                        check_deadline(deadline)?;
                        let document = read_document(settings, snapshot, record, &mut bytes)?;
                        let raw = body(&document.markdown, record.metadata().document_id)?;
                        let excerpts = excerpts(
                            settings,
                            index.as_ref(),
                            record,
                            raw,
                            search_query.ok_or_else(invalid)?,
                            characters,
                        )?;
                        Ok(SearchHit {
                            document: document.summary,
                            excerpts,
                        })
                    })
                    .collect::<Result<Vec<_>, GatewayError>>()
            })
            .transpose()?;
        let hits_truncated = hits
            .as_ref()
            .map(|hits| matching_documents.unwrap_or(0) > hits.len());
        projects.push(ProjectSummary {
            project,
            index: document.map(|d| d.summary),
            description,
            description_truncated,
            document_count,
            matching_documents,
            hits,
            hits_truncated,
        });
    }
    Ok(Inspection::Projects {
        commit: snapshot.commit().into(),
        projects,
        truncated,
    })
}
