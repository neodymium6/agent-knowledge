# Knowledge read operations

These additive operations are available in the CLI and the local STDIO or
loopback HTTP MCP server. They use the existing restricted SSH transport and
require an updated server. Older servers reject the new `akp-v1 inspect`
command; clients do not silently substitute a different operation. Existing
list, recent, get, export, search, and status wire formats remain unchanged.

| CLI command | MCP tool | Purpose |
| --- | --- | --- |
| `search-excerpts` | `knowledge_search_excerpts` | Ranked hits with raw field excerpts |

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
