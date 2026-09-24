# Knowledge read operations

These additive operations are available in the CLI and the local STDIO or
loopback HTTP MCP server. They use the existing restricted SSH transport and
require an updated server. Older servers reject the new `akp-v1 inspect`
command; clients do not silently substitute a different operation. Existing
list, recent, get, export, search, and status wire formats remain unchanged.

| CLI command | MCP tool | Purpose |
| --- | --- | --- |
| `projects` | `knowledge_projects` | Discover projects by overview or document hit counts |
| `search-excerpts` | `knowledge_search_excerpts` | Ranked hits with raw field excerpts |
| `context` | `knowledge_context` | Selected project documents from one snapshot |
| `history` | `knowledge_history` | Committed Markdown and path changes |
| `get-at` | `knowledge_get_at` | Exact Markdown at an official commit |
| `diff` | `knowledge_diff` | Metadata and body changes between two commits |

Every CLI command accepts `--destination` and the existing optional
`--timeout-seconds`. The full package exposes them under `agent-knowledge client`.
MCP uses the server's configured destination and timeout.

## Project discovery

Project discovery reads one committed snapshot and derives projects from their
stored documents; no registry or new metadata is required. Projects without an
index are included, with a null `index` and empty description. Unclassified
inbox documents and the root index do not form projects. Archived documents are
excluded unless `--include-archived` is set; archive-only projects therefore
appear only with that option.

```sh
# List projects in slug order.
agent-knowledge-client projects --destination fictional-knowledge

# Find project slugs, index titles, or index bodies containing this text.
agent-knowledge-client projects --destination fictional-knowledge --query astronomy

# Search documents and rank their projects by the number of matching documents.
agent-knowledge-client projects --destination fictional-knowledge \
  --query backup --search-in documents --maximum-results 10
```

The default `--search-in project` mode uses case-insensitive substring matching
on the project slug, index title, and complete index body. A query is optional;
results are ordered by project slug. It does not search other documents or
require a Tantivy index.

`--search-in documents` requires a query and uses the same backend, query syntax,
metadata allowlist, and archive policy as ordinary document search. All matches
within the enforced search bounds are counted by project **before** limiting the
number of returned projects. A matching index document counts as one document.
Projects with zero matches are omitted. Results are ordered by matching document
count descending, then slug ascending. Repeated terms in one document still
count as one matching document; this ranking measures hit count, not normalized
relevance or a percentage of the project's documents.

The response includes the exact `commit`, `projects`, and a `truncated` flag for
omitted projects. Each project includes:

- `project`: the project slug;
- `index`: index document metadata, including its title and permanent ID, or null;
- `description`: a raw prefix of the index body, never a generated summary;
- `description_truncated`: whether that prefix omits index text;
- `document_count`: all project documents within the requested archive scope;
- `matching_documents`: the exact hit count in documents mode, or null otherwise.

`--maximum-results` defaults to 100, subject to the Gateway's result limit.
`--description-characters` defaults to 300 Unicode scalar values, with a range
of 1..2000. Read bytes, response bytes, scan sizes, and the operation deadline
remain bounded. Exhausting a scan or computation bound is an error rather than
a successful response with partial hit counts. Document search requires the
configured index to match the same commit; a stale or missing index is an error.

MCP `knowledge_projects` uses `query`, `search_in`, `maximum_results`,
`description_characters`, and `include_archived` with the same defaults.
Discovery uses the additive `projects` inspection query advertised by Gateway
version reports and requires a supporting Gateway.

## Multiple project scopes

Repeat `--project` to select the union of several projects in `search`,
`search-excerpts`, `list`, or `recent`:

```sh
agent-knowledge-client search --destination fictional-knowledge \
  --query backup --project fictional-lab --project fictional-home \
  --maximum-results 20
```

MCP uses `projects: ["fictional-lab", "fictional-home"]` on the corresponding
read tools. At most one of `project` and `projects` may be supplied. A `projects`
array must contain 1..32 distinct valid slugs; empty arrays, duplicates, and
ambiguous single/multiple selections are rejected. An unknown slug matches
nothing and never widens the scope. Tag and session filters are ANDed with the
project union, and archive exclusions still apply.

The union is filtered before ranking and limiting results, inside one snapshot
and one search operation. `maximum_results` limits the combined result, not each
project separately. Search retains its configured backend order, list uses path
order, and recent uses recorded update/creation time.

One CLI `--project` retains the original single-project wire shape. Omitting
project filters retains cross-project reads. The additive `projects` filter
requires an updated Gateway; older Gateways reject it rather than silently
searching all projects. Context selection remains a single-project operation.

## Search excerpts

```sh
agent-knowledge-client search-excerpts \
  --destination fictional-knowledge \
  --project fictional-project \
  --query '"fictional service"' \
  --maximum-results 10 \
  --excerpt-characters 300
```

Project, tag, session, and archive filters have the same meaning as `search`.
The default is 10 hits and 300 Unicode scalar values per field excerpt; the
excerpt limit must be between 1 and 2000. Results retain document metadata,
revision, ranking, and the exact searched commit. Each excerpt identifies its
field and whether it omits part of that field. At most one excerpt is returned
per field. Text is copied from the source without HTML highlighting.

The configured Tantivy backend extracts query-term fragments using its query
parser and tokenizer. A fragment illustrates matching terms; it is not a proof
that a whole Boolean expression matches within that field. Some expressions
(such as match-all or term-free queries) can return hits without excerpts.
The unindexed backend uses its existing case-insensitive substring semantics.
A title- or tag-only match does not invent a matching body passage. Optional
metadata fields obey the deployment's search allowlist.

The new operation retains a committed snapshot until extraction completes.
Ranking and source text cannot straddle publication. Selected Markdown reads,
response bytes, and the entire operation have enforced bounds. A configured
missing or stale index remains a retryable failure.

## Project context

```sh
agent-knowledge-client context \
  --destination fictional-knowledge \
  --project fictional-project \
  --query 'fictional recovery' \
  --maximum-documents 10 \
  --maximum-characters 20000
```

Selection is deterministic and requires no model or external service. The
`--selection relevance` default retains the original candidate order:

1. Include the project's index document.
2. With a query, select matching decisions and runbooks in search order, then
   other matching documents. Without a query, select decisions and runbooks
   in canonical path order.
3. Include up to three recent eligible records.
4. Deduplicate by permanent document ID and apply the budgets.

Archived, deprecated, and superseded records are excluded from context.
Completed records remain eligible. Every selected document has a reason,
metadata/revision, a raw Markdown body, and a truncation flag. Bodies exclude
YAML front matter. All results come from the same commit.

The default budget is 10 documents and 20000 Unicode scalar values across
returned bodies, with a hard character maximum of 100000. Metadata is outside
that body budget but remains subject to the encoded response-byte limit. The
budget is not a model-specific token count. A large document may consume the
remaining budget. A shortened query-related body prefers a matching excerpt;
otherwise it uses the body prefix. If a query fragment is shorter than the
minimum usable excerpt despite sufficient allowance, the body prefix is used.
Partial Markdown can end inside a block.
A shortened body must contain at least 80 Unicode scalar values; a smaller
fragment is omitted instead of returned, leaving the allowance available to
later candidates. Complete short bodies remain eligible, including when they
fit into a remaining allowance of only a few characters.

`additional` contains up to `maximum_documents` omitted candidate summaries;
`truncated` indicates omitted or shortened selected content. It is a bounded
reading list, not a complete project export or an automatically maintained
project summary. A project with no eligible records returns empty lists.

### Balanced selection

Use an explicit selection mode when historical query-matching logs crowd out
durable guidance or recent observations:

```sh
agent-knowledge-client context \
  --destination fictional-knowledge --project fictional-project \
  --query backup --selection balanced --recent-documents 2 \
  --maximum-documents 6 --maximum-characters 12000
```

Balanced selection uses the same snapshot, eligibility rules, and hard budgets:

1. Select the project index.
2. Reserve up to `recent_documents` eligible logs, newest first by `updated`
   (or `created` when absent), breaking ties by permanent document ID. This is
   recorded time, not an inferred observation date from prose. Logs need not
   match the query; their selection reason is `recent_observation`.
3. Select matching decisions, runbooks, and references in search order, then
   other query matches. Without a query, select those durable document types
   in canonical path order.
4. Append the original recent-record candidates and deduplicate by document ID.

Each selected body gets at most `floor(maximum_characters / maximum_documents)`
Unicode scalar values. Unused shares are not redistributed. A long index or
log therefore cannot consume another document's share. The same 80-character
minimum for shortened bodies applies; complete shorter bodies still fit.

`recent_documents` defaults to `min(2, maximum_documents - 2)`, with a floor of
zero. Its maximum is `maximum_documents - 2` (floored at zero), leaving space
for index and guidance.
For example, three slots with one reserved log and at least 240 characters can
include an index, one recent log, and relevant guidance even when each body is
long. The guarantee requires eligible candidates and enough per-document budget
for usable excerpts; missing logs or omitted bodies are not synthesized.

The MCP `knowledge_context` parameters are `selection: "balanced"` and
`recent_documents`. Supplying recent-document controls in relevance mode is an
error. Balanced requests use the advertised `context_balanced` inspection query
and require a supporting Gateway. Default/relevance requests retain their
original wire shape and work with older Gateways.

## History, historical reads, and differences

```sh
agent-knowledge-client history \
  --destination fictional-knowledge \
  --document-id 01K00000000000000000000001 \
  --maximum-results 20
```

History identifies documents by front matter, so a move, reclassification, or
archive does not break their identity. Entries report the changing commit,
its document metadata/path/revision, and the previous Markdown revision.
Unrelated commits and attachment-only changes are omitted.

A page scans at most 100 first-parent commits and returns at most the requested
number of changes (20 by default). Continue with both returned `anchor_commit`
and `next_cursor`, including when an intermediate page has no entries. The
anchor keeps subsequent pages stable when new commits are published. A null
cursor ends the history. Tree-entry counts, cumulative Markdown bytes, Git
output, and the operation deadline bound historical inspection; a budget
failure is an error, not a successful incomplete history. The initial reader
scans committed trees directly, so large repositories may require smaller pages
or a future derived history index.

Use full lowercase commit IDs returned by reads/history for historical reads
and comparisons. The following hashes are fictional placeholders:

```sh
agent-knowledge-client get-at \
  --destination fictional-knowledge \
  --document-id 01K00000000000000000000001 \
  --commit aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa

agent-knowledge-client diff \
  --destination fictional-knowledge \
  --document-id 01K00000000000000000000001 \
  --from-commit aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --to-commit bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
```

Both commits must be reachable from the official commit pinned at operation
start. Abbreviations, revision expressions, and unpublished transaction commits
are rejected. If the document does not exist at a requested endpoint, the
operation returns `DOCUMENT_NOT_FOUND`.

`get-at` returns exact Markdown including front matter. `diff` returns complete
before/after metadata and a body-only changed line range, trimming identical
prefix and suffix lines. The range uses one-based body line numbers and
preserves line endings. Multiple separated edits appear in one contiguous
replacement range; this is not a minimal multi-hunk patch. An unchanged body
has empty added/removed strings, even when metadata or the path changed.

### Concise multi-hunk differences

```sh
agent-knowledge-client diff \
  --destination fictional-knowledge \
  --document-id 01K00000000000000000000001 \
  --from-commit aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --to-commit bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb \
  --format hunks --context-lines 3 --maximum-hunks 20 --maximum-diff-bytes 64000
```

The default `--format contiguous` retains the existing request and response.
The opt-in `hunks` format uses the advertised `diff_hunks` inspection query and
requires a supporting Gateway. MCP `knowledge_diff` accepts the corresponding
`format`, `context_lines`, `maximum_hunks`, and `maximum_diff_bytes` parameters.
Hunk controls with the contiguous format are rejected.

The `diff_hunks` result retains exact `from_commit`, `to_commit`, and complete
`before`/`after` metadata. Its `body` contains `changed`, `hunks`, `truncated`,
and `truncation_reason`. Each hunk has one-based `from_line`/`to_line` and raw
`removed`/`added` strings, including only the local context. Overlapping context
ranges are merged. Line endings and a missing final newline are preserved;
Unicode is never split. Applying all untruncated ranges to the old body produces
the new body. Metadata-only changes have `changed: false` and no hunks.

Context defaults to three lines and accepts 0..20. The hunk limit defaults to
20 and accepts 1..100. The encoded body-diff JSON budget defaults to 64000 bytes
and accepts 256..1000000, including JSON escaping and truncation information.
Metadata remains subject to the Gateway's separate total response-byte limit.
Only complete hunks are returned; if a hunk cannot fit, it and all later hunks
are omitted. Reducing context or increasing the byte budget can expose it.

Inputs accept up to 20000 lines each. A bounded Myers search skips common runs,
with at most 1000000 search steps, 1000000 stored frontier cells, and edit
distance 1024 after common prefix/suffix trimming. The operation deadline also
applies. Large unchanged middle sections do not require a quadratic line-pair
matrix, while heavily rewritten documents may exceed the computation bounds.
An omitted calculation or range sets `truncated: true` with `input_line_limit`,
`computation_limit`, `hunk_limit`, or `byte_limit`. `changed` always describes
the complete input bodies: an empty truncated hunk list does **not** mean the
bodies were identical. Deadline or overall response-limit failures remain
errors, consistent with other inspection operations.

History is commit-granular. Multiple accepted requests can share one commit;
intermediate states within that batch are not exposed as separate revisions.
These tools provide reads, not rollback. Archived historical content remains
readable under the same existing access model.
