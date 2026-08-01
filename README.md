# Agent Knowledge

A centralized, file-based knowledge-management system for coding agents running
across multiple machines.

The intended source of truth is a hierarchy of Markdown documents and ordinary
attachment files. Client machines submit and retrieve information through a
restricted gateway; they do not synchronize the repository with Git.

## Status

The architecture is defined, delivery increments 1 through 7 are implemented,
and the request-status and Git-replication portions of increment 8 are complete. The current
executable can accept requests locally or through an
OpenSSH forced command, process them through the single Writer, and publish
immutable Quartz releases. Coding agents can list, retrieve, and search an
exact committed content snapshot and inspect durable request state through the
same Gateway. Git remote replication runs asynchronously with durable retry
state. Bundle export, operational status, retention, and packaging remain future
work.

- Rust is the implementation language.
- OpenSSH forced commands provide the client transport and authentication
  boundary.
- A durable file queue separates request acceptance from repository changes.
- A single Repository Worker applies atomic changes and commits them with Git.
- A bounded Quartz runner and pinned release store publish immutable static
  releases through an atomically replaced `current` symlink.
- A conventional Linux host is the initial target. The design remains
  compatible with a future single-replica Kubernetes deployment.

See [DESIGN.md](DESIGN.md) for the complete architecture, invariants,
protocol, persistence, recovery, and delivery plan.

## Development

Enter the pinned development environment:

```sh
direnv allow
```

Initialize the local Git hooks and run the repository checks:

```sh
just init
just check
```

Run the Repository Worker with a validated deployment configuration:

```sh
agent-knowledge worker run --config /srv/agent-knowledge/worker.yaml
```

The Worker accepts a strict configuration such as:

```yaml
schema_version: 1
storage:
  queue_root: /srv/fictional-knowledge/queue
  repository_root: /srv/fictional-knowledge/repository
  content_root: /srv/fictional-knowledge/content
  work_root: /srv/fictional-knowledge/work
  release_root: /srv/fictional-knowledge/releases
repository:
  official_branch: main
  author_name: Fictional Knowledge Worker
  author_email: worker@example.invalid
  replication:
    remote: fictional-backup
    branch: main
    timeout_seconds: 30
    initial_backoff_seconds: 10
    maximum_backoff_seconds: 3600
quartz:
  program: /opt/fictional-quartz/bin/build-site
  integration_root: /opt/fictional-quartz
  timeout_seconds: 300
batch:
  debounce_seconds: 30
  maximum_age_seconds: 300
  maximum_scan_entries: 1024
  maximum_requests: 100
  maximum_recovery_requests: 10000
```

`repository.replication` is optional. When present, the named remote must
already exist in the bare repository. Authentication belongs in the service
account's Git/SSH deployment configuration; credentials are not accepted in
the Worker configuration. A push failure never rolls back the local commit,
canonical content, active Quartz release, or completed request. The Worker
persists the last confirmed commit and an exponential retry deadline under the
bare repository, caps delay at `maximum_backoff_seconds`, disables interactive
credential prompts, and applies `timeout_seconds` to every attempt.

Submit a validated request package through an SSH host alias:

```sh
agent-knowledge client submit \
  --destination fictional-knowledge \
  --package-root ./fictional-request \
  --timeout-seconds 300
```

Inspect the durable state of an accepted request:

```sh
agent-knowledge client status \
  --destination fictional-knowledge \
  --request-id 01K00000000000000000000000
```

Search committed Markdown and configured metadata through that alias:

```sh
agent-knowledge client search \
  --destination fictional-knowledge \
  --query "fictional restart" \
  --project fictional-project \
  --maximum-results 25
```

The `list`, `recent`, `get`, `search`, and `status` commands return strict
versioned JSON. Every successful committed-content response identifies the
exact official Git commit used for the operation. List and search operations
support exact project, tag, and session filters; archived documents are
excluded unless `--include-archived` is supplied. Status returns `pending`,
`processing`, `completed`, or `failed`; failed responses include the durable
error code and failure time. An unknown request ID returns
`REQUEST_NOT_FOUND`. After a complete control request is received, one
read-operation deadline covers initialization, lookup or query work, response
encoding, and delivery to the SSH channel. The response-byte limit includes
the JSON Lines framing newline. Committed-content read processes open only the
repository and content checkout. Status and submit processes open the durable
queue, while status takes no queue locks and does not run maintenance.

The client validates and snapshots at most 64 MiB of package data before
network output. It then invokes the system `ssh` executable directly, uses
non-interactive authentication, disables TTY allocation, forwarding, and SSH
backgrounding/stdin overrides, and streams an uncompressed tar archive to the
exact remote command `akp-v1 submit`. SSH identity, host-key, proxy, and
destination settings belong in the user's SSH configuration. The timeout is an
absolute transfer deadline; it defaults to 300 seconds and is bounded to 3,600
seconds. The client resolves payloads beneath a pinned package directory
without following symbolic-link components. It runs SSH in a dedicated process
group and terminates that group when the deadline or a stream-size limit is
reached, including when a proxy descendant retains an output pipe.

The forced command requires a strict Gateway configuration such as:

```yaml
schema_version: 2
storage:
  queue_root: /srv/fictional-knowledge/queue
  git_directory: /srv/fictional-knowledge/repository
  content_root: /srv/fictional-knowledge/content
repository:
  official_branch: main
reads:
  maximum_results: 100
  maximum_query_characters: 512
  maximum_index_entries: 100000
  maximum_index_markdown_bytes: 536870912
  maximum_search_documents: 10000
  maximum_search_markdown_bytes: 536870912
  operation_timeout_seconds: 30
  maximum_response_bytes: 268435456
  search_metadata:
    node: true
    agent: true
    session: true
    request_id: true
transport:
  submit_timeout_seconds: 300
```

```text
restrict,command="/usr/local/bin/agent-knowledge gateway --config /etc/agent-knowledge/gateway.yaml --client-id fictional-node-a" ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIFictionalKeyMaterialOnly
```

The Worker emits JSON Lines operational events. Every record includes
`timestamp`, `severity`, `component`, and `event`. Terminal batch records also
include `outcome`, `successful_requests`, and `failed_requests`; committed
batches include `commit`. Failure counts include both queue validation and
repository application. Queue-validation counts are retained in the durable
repository transaction journal, so resumed batch events preserve them.
Terminal process failures use a stable `error_code` and include any requests
already rejected during the interrupted cycle.
Remote replication emits `remote_replication_succeeded` after a new commit is
confirmed and `remote_replication_failed` with `commit`,
`consecutive_failures`, and `retry_at` after a failed attempt. Durable-state
validation failures emit `remote_replication_state_error` once until the state
becomes readable again. Up-to-date and deferred polls are intentionally quiet.

`SIGINT` and `SIGTERM` request graceful shutdown before a new durable
transaction or after the current transaction completes. A supervisor must
signal only the main Worker process initially and reserve group-wide `SIGKILL`
for its hard-stop timeout.
