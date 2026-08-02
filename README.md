# Agent Knowledge

A centralized, file-based knowledge-management system for coding agents running
across multiple machines.

The intended source of truth is a hierarchy of Markdown documents and ordinary
attachment files. Client machines submit and retrieve information through a
restricted gateway; they do not synchronize the repository with Git.

## Status

The architecture is defined, delivery increments 1 through 8 and the Linux
portion of increment 9 are implemented. The current executable can accept
requests locally or through an OpenSSH forced command, process them through the
single Writer, and publish immutable Quartz releases. Coding agents can list,
retrieve, and search an exact committed content snapshot and inspect durable
request state through the same Gateway. Git remote replication runs
asynchronously with durable retry state. Derived-release retention and
document-bundle export are implemented. The flake provides reproducible Linux
packaging, a conventional systemd Worker service, and a systemd-activated local
queue-ingress broker that isolates the forced-command Gateway from durable queue
access under distinct service identities. Reproducible Worker and Queue
Ingress container packaging is implemented; the Gateway container and
single-replica Kubernetes packaging remain future work.

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

Build the production Linux package or run it directly through the flake:

```sh
nix build .#agent-knowledge
nix run .#agent-knowledge -- client list --destination fictional-knowledge
```

The package is available for `x86_64-linux` and `aarch64-linux`. Its runtime
wrapper provides the pinned Git and OpenSSH executables used by Worker,
Gateway, and client operations. Quartz remains a deployment-supplied absolute
program path and integration directory. The package does not contain
deployment-specific Worker or Gateway configuration, credentials, host keys,
client keys, or Quartz content.

### Container image

Build the Docker-compatible Worker image archive without a container daemon:

```sh
nix build .#worker-container-image
docker load < result
```

Build the independently role-locked Queue Ingress image in the same format:

```sh
nix build .#queue-ingress-container-image
docker load < result
```

The flake builds the image natively for both `x86_64-linux` (`amd64`) and
`aarch64-linux` (`arm64`). Its entrypoint fixes the wrapped executable and
`worker run` role; the configuration path is supplied as an argument by the
deployment. The image resolves the non-root `agent-knowledge` account to
`10003:10003` and its queue supplementary group to `10002`, and includes the CA
bundle and `SSL_CERT_FILE` setting needed for HTTPS Git replication. It exposes
no conventional shell path and contains no deployment configuration,
credentials, keys, or Quartz content.

The Queue Ingress image fixes the non-root `agent-knowledge-queue` identity
(`10002:10002`) and `queue-ingress listen` entrypoint. It contains the raw Rust
executable closure rather than the Worker package's Git/OpenSSH wrapper, and it
does not include a CA bundle. The deployment supplies the queue root, shared
runtime directory, and listener arguments. A dedicated ingress-socket group
(`10004`) grants the Gateway access to the `0660` socket without granting the
broker the Gateway reader group or granting Gateway access to the queue.

`just check-package` validates both image archives, architectures,
deterministic timestamps, role-locked entrypoints, non-root metadata, identity
database, role-specific environment, and required filesystem entries without
Docker or Podman. It also verifies the Worker's CA bundle. Runtime storage,
configuration, secrets, runtime socket directory, and writable paths are
deployment-supplied mounts.

### systemd service

The Linux package contains hardened Worker and socket-activated queue-ingress
units plus `sysusers.d` and `tmpfiles.d` definitions. Install them from the
immutable package output:

```sh
sudo nix profile add --profile /nix/var/nix/profiles/agent-knowledge \
  .#agent-knowledge
package_path=/nix/var/nix/profiles/agent-knowledge
sudo systemd-sysusers "$package_path/lib/sysusers.d/agent-knowledge.conf"
sudo ln -sfn \
  "$package_path/lib/tmpfiles.d/agent-knowledge.conf" \
  /etc/tmpfiles.d/agent-knowledge.conf
sudo systemd-tmpfiles --create agent-knowledge.conf
sudo install -d -m 0755 -o root -g root /etc/agent-knowledge
sudo install -m 0640 -o root -g agent-knowledge \
  ./fictional-worker.yaml /etc/agent-knowledge/worker.yaml
sudo systemctl link "$package_path/lib/systemd/system/agent-knowledge-worker.service"
sudo systemctl link \
  "$package_path/lib/systemd/system/agent-knowledge-queue-ingress.socket"
sudo systemctl link \
  "$package_path/lib/systemd/system/agent-knowledge-queue-ingress@.service"
```

The `/etc/tmpfiles.d` link points through the stable system profile, so boot
recreates the volatile runtime directory and profile upgrades select the new
packaged definition.

The supplied storage layout uses sibling roots below
`/var/lib/agent-knowledge/`. A matching Worker configuration uses `queue`,
`repository`, `content`, `work`, and `releases` below that directory. The
operator must initialize `repository` as a bare Git repository with the
configured official branch, attach `content` as that branch's canonical
worktree, and deploy the configured Quartz launcher and integration tree before
starting the service. The Worker intentionally does not invent these
deployment inputs. After validating them, enable and start the Worker:

```sh
sudo systemctl enable --now agent-knowledge-queue-ingress.socket
sudo systemctl enable --now agent-knowledge-worker.service
```

The unit does not start without `/etc/agent-knowledge/worker.yaml`, and repeated
startup failures are limited to five attempts in five minutes. Inspect a failed
start with `systemctl status` and `journalctl -u agent-knowledge-worker` rather
than repeatedly restarting an incompletely provisioned deployment.

The packaged `agent-knowledge` and `agent-knowledge-queue` accounts are locked,
non-login accounts for the Worker and queue ingress broker respectively. Never
use either for OpenSSH. Create the deployment-specific SSH account according to
the host's authentication policy and add only that account to the
`agent-knowledge-gateway` and `agent-knowledge-ingress` groups. The first group
can read committed repository/content storage; the second can connect to the
local ingress socket. Neither can open the durable queue. The broker owns the
queue but cannot open Worker-owned storage.
The Worker receives the queue group as a supplementary group so it can perform
state transitions without sharing either service UID. The durable storage root
is `0751 root:agent-knowledge-queue`: the broker and Worker can open it for
directory durability syncs, while the Gateway can only traverse to its known
read-only repository and content paths.

The dedicated system profile keeps the package output live across Nix garbage
collection. The unit allows writes only below `/var/lib/agent-knowledge`, uses
`KillMode=mixed`, and grants 15 minutes for transaction-boundary shutdown. A
deployment using other durable roots must add them with a systemd drop-in. It
must also increase `TimeoutStopSec` when its maximum expected Git or Quartz
transaction can exceed 15 minutes.

The first upgrade from the Worker-only queue layout is an offline migration.
Disable new forced-command SSH sessions, wait for existing Gateway processes,
stop the Worker and socket, and take a storage backup before running it. The
migration takes both queue locks, requires empty `queue/incoming` and
`queue/quarantine` directories, rejects links, special files, hard links,
cross-mount traversal, and concurrent tree changes, changes existing queue data
to the queue group, and grants the Gateway group read-only access to existing
repository/content descendants. Before changing permissions, it preflights all
three roots and rejects stores exceeding 1,000,000 filesystem objects or 512
MiB of cumulative relative-path bytes. For the default storage root, upgrade
and reload with:

```sh
sudo systemctl stop agent-knowledge-worker.service
sudo systemctl stop agent-knowledge-queue-ingress.socket 2>/dev/null || true
sudo nix profile upgrade \
  --profile /nix/var/nix/profiles/agent-knowledge agent-knowledge
package_path=/nix/var/nix/profiles/agent-knowledge
sudo systemd-sysusers "$package_path/lib/sysusers.d/agent-knowledge.conf"
# Replace this fictional name with the existing forced-command SSH account.
gateway_account=fictional-agent-knowledge-gateway
sudo usermod --append --groups agent-knowledge-ingress "$gateway_account"
sudo "$package_path/bin/agent-knowledge" admin migrate-v1-storage \
  --queue-root /var/lib/agent-knowledge/queue \
  --git-directory /var/lib/agent-knowledge/repository \
  --content-root /var/lib/agent-knowledge/content
sudo ln -sfn \
  "$package_path/lib/tmpfiles.d/agent-knowledge.conf" \
  /etc/tmpfiles.d/agent-knowledge.conf
sudo systemd-tmpfiles --create agent-knowledge.conf
sudo systemctl link --force \
  "$package_path/lib/systemd/system/agent-knowledge-worker.service"
sudo systemctl link --force \
  "$package_path/lib/systemd/system/agent-knowledge-queue-ingress.socket"
sudo systemctl link --force \
  "$package_path/lib/systemd/system/agent-knowledge-queue-ingress@.service"
# Before restarting, change Gateway schema_version to 3 and replace
# storage.queue_root with:
#   queue_socket: /run/agent-knowledge/queue-ingress.sock
sudo systemctl daemon-reload
sudo systemctl enable --now agent-knowledge-queue-ingress.socket
sudo systemctl start agent-knowledge-worker.service
```

Gateway schema v2 is intentionally not accepted after this upgrade. Update
`/etc/agent-knowledge/gateway.yaml` to schema v3 while access is disabled;
replace `storage.queue_root` with `storage.queue_socket` as shown in the Gateway
configuration example below. A deployment using non-default durable roots
passes the three configured roots independently to the migration command and
updates `ReadWritePaths`, `WorkingDirectory`, and the queue-ingress service's
`ExecStart` queue root with systemd drop-ins.

Configuration, SSH keys, Git credentials, Quartz, and service enablement remain
deployment inputs.

Supervisors that do not provide systemd-style socket activation can run the
broker as one long-lived process:

```sh
agent-knowledge queue-ingress listen \
  --queue-root /srv/fictional-knowledge/queue \
  --socket-path /run/fictional-knowledge/queue-ingress.sock \
  --maximum-connections 64 \
  --connection-timeout-seconds 3900
```

The runtime directory must already exist, be owned and writable by the
queue-ingress identity, use the setgid `agent-knowledge-ingress` group, and be
writable by neither group nor other; mode `2750` is recommended. The container
identity database assigns this group GID `10004`, and the Gateway joins it.
Its configured path must already be canonical, must not traverse symbolic
links, and must leave room within Linux `sun_path` for the listener's 30-byte
`.ak-<ULID>` temporary socket name; this is checked before listener state is
changed.
The listener publishes the socket as `0660`, refuses to overwrite live,
non-socket, or unowned stale paths, recovers a stale socket recorded by its own
locked state file after a crash, and rejects a socket basename change while a
prior recorded socket still exists. It bounds concurrent connections and
handler shutdown, keeps connection diagnostics outside the listener control
loop, and stops accepting and cancels active queue lock waits on `SIGINT` or
`SIGTERM`. A handler that ignores cancellation past the grace period makes the
listener exit with failure so its supervisor can replace the process without
accumulating detached threads.
`queue-ingress serve` remains the one-connection entrypoint used by the
packaged systemd units.

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
retention:
  retained_releases: 10
  maximum_scan_entries: 10000
  maximum_removals: 10
```

`repository.replication` is optional. When present, the named non-mirror remote
must already exist in the bare repository and resolve to exactly one push URL.
Authentication belongs in the service account's Git/SSH deployment
configuration; credentials are not
accepted in the Worker configuration. A push failure never rolls back the local
commit, canonical content, active Quartz release, or completed request. The
Worker performs pushes on an independent background thread, so a slow remote
does not delay queue processing. Local publication wakes that thread; otherwise
it sleeps until a retry is due, with a bounded low-frequency verification poll.
It persists the last confirmed commit, a fingerprint of the configured push
URL, and an exponential retry deadline under the bare repository. It caps delay
at `maximum_backoff_seconds`, disables interactive Git and SSH credential
prompts, and applies one `timeout_seconds` deadline to every Git subprocess in
an attempt, including local Git inspection. Local replication-state filesystem
I/O is outside that subprocess deadline. Each push uses an isolated temporary
Git directory and the exact validated URL snapshot, so later changes to the
main repository's local Git configuration cannot change that attempt's
destination or behavior.

Inspect the initialized local deployment through the same trusted Worker
configuration:

```sh
agent-knowledge admin status \
  --config /srv/agent-knowledge/worker.yaml \
  --maximum-queue-entries 100000 \
  --timeout-seconds 30
```

This local administrative command emits one versioned JSON object containing
queue counts, the oldest pending timestamp, Worker-lock activity, the official
commit, the active Quartz release, and remote-replication progress. It is
read-only: it neither initializes nor repairs storage nor contacts the Git
remote. The queue scan has an explicit entry bound and does not take the
accepted-state lock, so it does not block submissions or Worker transitions.
Queue fields are best-effort observations and `snapshot_exact` is currently
always `false`. Its deadline covers bounded queue work and Git subprocesses;
local filesystem calls remain subject to the host filesystem's I/O behavior.
The command verifies the official commit again after inspecting release and
replication state; a concurrent publication causes a transient failure instead
of a mixed committed-content snapshot.

Preview and apply bounded retention of old derived Quartz releases:

```sh
agent-knowledge admin prune-releases \
  --config /srv/agent-knowledge/worker.yaml \
  --dry-run

agent-knowledge admin prune-releases \
  --config /srv/agent-knowledge/worker.yaml
```

The optional `retention` configuration defaults to the values shown above.
Each pass preserves the newest `retained_releases` and always protects the
active release, even when it is older. `maximum_scan_entries` bounds all
release-store directory entries inspected, and `maximum_removals` bounds the
release trees selected per invocation. The command takes the release-store
maintenance lock, does not initialize missing storage, and emits versioned
JSON. Dry-run output identifies both newly selected releases and existing
cleanup-pending tombstones. On Unix, a non-dry-run pass records an inode-bound
durable intent and atomically moves each selected derived tree into a private
tombstone before descriptor-relative deletion. Large trees may report
`cleanup_pending_release_ids` and complete on a later invocation. Canonical
content, Git history, accepted requests, and the active release are never
removed. Interrupted intents are reconciled during the next bounded pass, and
the scan retains only lightweight manifest and filesystem identity metadata
rather than one open descriptor per release. A selected directory must match
that recorded identity when it is reopened for mutation.

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

Export a committed document and its colocated attachments as an uncompressed
tar archive:

```sh
agent-knowledge client export \
  --destination fictional-knowledge \
  --document-id 01K00000000000000000000001 \
  > bundle.tar
```

The archive contains `index.md` followed by attachment files in deterministic
name order. Project-shared `assets/` are not part of a document bundle.

The `list`, `recent`, `get`, `search`, and `status` commands return strict
versioned JSON; `export` returns an uncompressed tar stream. Every successful
JSON committed-content response identifies the
exact official Git commit used for the operation. List and search operations
support exact project, tag, and session filters; archived documents are
excluded unless `--include-archived` is supplied. Status returns `pending`,
`processing`, `completed`, or `failed`; failed responses include the durable
error code and failure time. An unknown request ID returns
`REQUEST_NOT_FOUND`. After a complete control request is received, one
read-operation deadline covers initialization, lookup or query work, response
encoding, and delivery to the SSH channel. The response-byte limit includes
the JSON Lines framing newline. Committed-content read processes open only the
repository and content checkout. Gateway status and submit processes open only
the local ingress socket. The queue ingress broker alone opens the durable
queue; per-request status takes no queue locks and does not run maintenance.

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

The forced-command account needs read access to this root-controlled
configuration and membership in `agent-knowledge-gateway`, but no queue or
Worker-account membership. It also joins `agent-knowledge-ingress` to connect
to the local broker socket.

The forced command requires a strict Gateway configuration such as:

```yaml
schema_version: 3
storage:
  queue_socket: /run/agent-knowledge/queue-ingress.sock
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
restrict,command="/nix/var/nix/profiles/agent-knowledge/bin/agent-knowledge gateway --config /etc/agent-knowledge/gateway.yaml --client-id fictional-node-a" ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIFictionalKeyMaterialOnly
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
Reportable events use a bounded in-process queue; when that queue is full,
replication pauses instead of overwriting an unread event.
An unexpected background-thread exit emits
`remote_replication_thread_stopped` instead of appearing as an idle poll.

`SIGINT` and `SIGTERM` request graceful shutdown before a new durable
transaction or after the current transaction completes. An in-flight remote
push is cancelled when the Worker runtime shuts down. A supervisor must signal
only the main Worker process initially and reserve group-wide `SIGKILL` for its
hard-stop timeout.
