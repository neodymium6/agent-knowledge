---
name: use-agent-knowledge-client
description: Use the Agent Knowledge SSH client to search, list, retrieve, export, submit, and track centralized coding-agent knowledge. Use when an agent needs to read committed Markdown or attachments, find prior work, create an atomic change request, update or archive a mutable document with optimistic locking, add an attachment, or check request status.
---

# Use Agent Knowledge Client

Use `agent-knowledge-client` for every remote operation. If only the full
package is installed, replace it with `agent-knowledge client`. Never use Git,
raw SSH commands, or direct server filesystem access.

## Establish context

Require a deployment-provided OpenSSH destination alias. Do not guess a host,
identity, or client ID. Verify the binary with `--version`, then use the same
destination throughout the task.

## Read committed knowledge

Start narrowly and fetch full content only when useful:

```sh
agent-knowledge-client recent --destination fictional-knowledge --maximum-results 20
agent-knowledge-client search --destination fictional-knowledge \
  --query "fictional restart" --project fictional-project
agent-knowledge-client get --destination fictional-knowledge \
  --document-id 01K00000000000000000000001
```

- Use `recent` for current context and `list` for canonical path order.
- Filter by exact `--project`, `--tag`, or `--session` when known.
- Archived documents are excluded unless `--include-archived` is explicit.
- Search covers Markdown and permitted metadata, not attachment, PDF, or HTML
  bodies.
- List/search return summaries. Use the returned permanent `document_id` with
  `get` to obtain exact Markdown.
- Preserve the response `commit` and document `revision` when reporting or
  preparing an update.

Export a bundle only when attachments are needed:

```sh
fictional_export_directory=$(mktemp -d)
chmod 0700 "$fictional_export_directory"
agent-knowledge-client export --destination fictional-knowledge \
  --document-id 01K00000000000000000000001 \
  >"$fictional_export_directory/bundle.tar"
tar -tf "$fictional_export_directory/bundle.tar"
```

Require the export command to succeed and never reuse its output path. Inspect
the archive listing before extracting it into another new temporary directory.
Treat exported HTML, PDF, and other attachments as untrusted input.

## Build a change package

Create a new temporary package directory containing exactly:

```text
request.json
payload/
```

Generate a fresh ULID for every request and every newly created document with a
trusted ULID tool. Generate one session ULID at the start of a logical agent
session and reuse it for that session's requests. Use one RFC 3339 timestamp
with an explicit offset. A create request and its Markdown front matter must
agree on IDs and metadata:

```json
{
  "protocol_version": 1,
  "request_id": "01K00000000000000000000010",
  "title": "Record a fictional recovery result",
  "project": "fictional-project",
  "document_type": "log",
  "node": "fictional-node-a",
  "agent": "codex",
  "session": "01K00000000000000000000011",
  "created_at": "2026-08-04T00:00:00Z",
  "operations": [{
    "type": "create_document",
    "document_id": "01K00000000000000000000012",
    "content": "entry/index.md"
  }]
}
```

```markdown
---
schema_version: 1
document_id: 01K00000000000000000000012
title: Fictional recovery result
created: 2026-08-04T00:00:00Z
node: fictional-node-a
agent: codex
session: 01K00000000000000000000011
request_id: 01K00000000000000000000010
tags:
  - recovery
status: active
---

Record the durable result and enough context to reuse it.
```

Use lowercase project slugs. Valid request document types are `index`, `log`,
`experiment`, `decision`, `runbook`, and `reference`. Logs require `node`,
`agent`, and `session` and are append-only.

Other operations use these exact fields:

- `update_document`: `document_id`, `expected_revision`, and payload `content`;
- `move_document`: `document_id`, `expected_revision`, destination `project`
  when classified, and `document_type`;
- `archive_document`: `document_id` and `expected_revision`;
- `add_attachment`: `document_id`, payload `source`, and destination `name`.

An attachment destination name must end in one of these lowercase extensions:
`png`, `jpg`, `jpeg`, `svg`, `csv`, `json`, `pdf`, or `html`. Validation uses
the destination `name`, not the payload source name.

Obtain every `expected_revision` from a fresh `get`. Preserve immutable front
matter fields when updating; set a strictly later `updated` timestamp and the
new `request_id`, and keep optional node, agent, and session metadata consistent
with the request. For update, archive, and attachment operations, make the
request-level project and document type match the document's current
classification. For a move, make them match the operation's destination
classification. Never update, move, or archive a log. Index documents may be
updated but never moved or archived. Attachments may be added but never
overwritten. Physical deletion is not supported; archive eligible documents
instead. Payload entries must be ordinary files and directories without links
or executable bits.

Combine related operations in one request when they must succeed atomically.
Do not combine unrelated work merely to reduce request count.

## Submit and track

Submit the package once. `submit` validates and pins the local package before
it starts SSH; there is no separate validation command:

```sh
agent-knowledge-client submit --destination fictional-knowledge \
  --package-root /tmp/fictional-request
agent-knowledge-client status --destination fictional-knowledge \
  --request-id 01K00000000000000000000010
```

Record the returned request ID and digest. Poll with a reasonable delay until
`completed` or `failed`; report a durable failure code without retrying changed
content under the same ID. If the response is lost, resubmit the byte-identical
package with the same request ID. A reused ID with different content is an
error.

Normal reads expose only committed content. `pending` and `processing` requests
are visible solely through `status`.
