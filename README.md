# Agent Knowledge

A centralized, file-based knowledge-management system for coding agents running
across multiple machines.

The intended source of truth is a hierarchy of Markdown documents and ordinary
attachment files. Client machines submit and retrieve information through a
restricted gateway; they do not synchronize the repository with Git.

## Status

The architecture is defined, and delivery increments 1 through 6 are
implemented. The current executable can accept requests locally or through an
OpenSSH forced command, process them through the single Writer, and publish
immutable Quartz releases. Committed reads, search, remote-push retry, and
packaging remain future increments.

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

Submit a validated request package through an SSH host alias:

```sh
agent-knowledge client submit \
  --destination fictional-knowledge \
  --package-root ./fictional-request \
  --timeout-seconds 300
```

The client validates and snapshots at most 64 MiB of package data before
network output. It then invokes the system `ssh` executable directly, uses
non-interactive authentication, disables TTY allocation, forwarding, and SSH
backgrounding/stdin overrides, and streams an uncompressed tar archive to the
exact remote command `akp-v1 submit`. SSH identity, host-key, proxy, and
destination settings belong in the user's SSH configuration. The timeout is an
absolute transfer deadline; it defaults to 300 seconds and is bounded to 3,600
seconds.

The forced command requires a strict Gateway configuration such as:

```yaml
schema_version: 1
storage:
  queue_root: /srv/fictional-knowledge/queue
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

`SIGINT` and `SIGTERM` request graceful shutdown before a new durable
transaction or after the current transaction completes. A supervisor must
signal only the main Worker process initially and reserve group-wide `SIGKILL`
for its hard-stop timeout.
