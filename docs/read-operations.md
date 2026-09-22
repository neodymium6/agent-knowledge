# Knowledge read operations

These additive operations are available in the CLI and the local STDIO or
loopback HTTP MCP server. They use the existing restricted SSH transport and
require an updated server. Older servers reject the new `akp-v1 inspect`
command; clients do not silently substitute a different operation. Existing
list, recent, get, export, search, and status wire formats remain unchanged.

| CLI command | MCP tool | Purpose |
| --- | --- | --- |
| `search-excerpts` | `knowledge_search_excerpts` | Ranked hits with raw field excerpts |
| `context` | `knowledge_context` | Selected project documents from one snapshot |

Every CLI command accepts `--destination` and the existing optional
`--timeout-seconds`. The full package exposes them under `agent-knowledge client`.
MCP uses the server's configured destination and timeout.

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

Selection is deterministic and requires no model or external service:

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
otherwise it uses the body prefix. Partial Markdown can end inside a block.

`additional` contains up to `maximum_documents` omitted candidate summaries;
`truncated` indicates omitted or shortened selected content. It is a bounded
reading list, not a complete project export or an automatically maintained
project summary. A project with no eligible records returns empty lists.
