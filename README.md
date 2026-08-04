# Agent Knowledge

Agent Knowledge is a centralized, file-based knowledge store for coding agents
running on multiple machines. Markdown and ordinary attachments are the source
of truth; clients use a restricted SSH interface and never synchronize the Git
repository directly.

The initial implementation is complete. It provides durable request intake, a
single repository writer, optimistic locking, Git history and replication,
Quartz releases, committed reads and search, systemd packaging, and a
single-replica Kubernetes deployment.

## Install

The complete server and administrative CLI is distributed as a tagged Nix
flake for `x86_64-linux` and `aarch64-linux`:

```sh
nix profile install \
  github:neodymium6/agent-knowledge/v0.1.0#agent-knowledge
agent-knowledge --version
```

A static client-only binary is attached to each GitHub Release. Select the
archive for `x86_64-unknown-linux-musl` or
`aarch64-unknown-linux-musl`, then verify it with the published
`SHA256SUMS` file. The client requires a system OpenSSH `ssh` executable.

Server containers are published to GHCR for `linux/amd64` and `linux/arm64`:

```text
ghcr.io/neodymium6/agent-knowledge-worker:0.1.0
ghcr.io/neodymium6/agent-knowledge-queue-ingress:0.1.0
ghcr.io/neodymium6/agent-knowledge-gateway:0.1.0
ghcr.io/neodymium6/agent-knowledge-openssh-gateway:0.1.0
ghcr.io/neodymium6/agent-knowledge-storage-bootstrap:0.1.0
```

Images contain no deployment configuration, SSH keys, Git credentials, or
Quartz content. Quartz remains an external, immutable deployment input.

## Architecture

```text
Coding agent
    │ restricted SSH
    ▼
Gateway ── Unix socket ── Queue Ingress ── durable queue
                                               │
                                               ▼
                                      Repository Worker
                                      ├─ Git history/remote
                                      ├─ canonical content
                                      └─ Quartz releases
```

The Gateway cannot write content or open the durable queue. Queue Ingress can
write the queue but cannot read repository content. Only the Worker changes the
canonical content, Git repository, and release store. One request either
commits completely or has no content effect.

See [DESIGN.md](DESIGN.md) for protocols, schemas, storage invariants, recovery,
and the complete security model.

## Client

The full package uses the `agent-knowledge client` namespace. The downloadable
client-only binary exposes the same operations directly as
`agent-knowledge-client`.

```sh
# Full package
agent-knowledge client submit \
  --destination fictional-knowledge \
  --package-root ./fictional-request

# Client-only release binary
agent-knowledge-client status \
  --destination fictional-knowledge \
  --request-id 01K00000000000000000000000

agent-knowledge-client search \
  --destination fictional-knowledge \
  --query "fictional restart" \
  --project fictional-project

agent-knowledge-client get \
  --destination fictional-knowledge \
  --document-id 01K00000000000000000000001

agent-knowledge-client export \
  --destination fictional-knowledge \
  --document-id 01K00000000000000000000001 \
  >bundle.tar
```

SSH host aliases, identities, host-key policy, and proxies belong in the
client's OpenSSH configuration. The client disables interactive prompts, TTYs,
and forwarding and enforces bounded request, response, and transfer sizes.

Request packages contain `request.json` and a `payload/` tree. Their exact
format and path rules are defined in [DESIGN.md](DESIGN.md#14-change-requests).

Server installation, client installation, and client usage skills are bundled
in the skills-only plugin at
[`plugins/agent-knowledge/`](plugins/agent-knowledge/). Resolve the desired
release tag to its exact approved commit, then install that immutable Git
revision through the repository marketplace:

```sh
codex plugin marketplace add neodymium6/agent-knowledge \
  --ref 0123456789abcdef0123456789abcdef01234567
codex plugin add agent-knowledge@agent-knowledge
```

Replace the fictional SHA with a release commit that contains the plugin. A
semantic tag selects a version but is not itself an immutable pin.

## Linux systemd deployment

The Nix package contains hardened Worker and socket-activated Queue Ingress
units plus `sysusers.d` and `tmpfiles.d` definitions. A normal deployment:

1. installs the tagged package into a stable system profile;
2. creates the packaged Worker and Queue Ingress accounts;
3. provisions one dedicated forced-command Gateway account;
4. installs root-controlled Worker and Gateway configuration;
5. runs `admin bootstrap-storage` once as root;
6. links the packaged units and tmpfiles definition; and
7. enables the socket and Worker service.

The example Kubernetes configuration files are also valid schema references
for a conventional host. Replace their fictional identities and paths before
installing them:

```text
deploy/kubernetes/config/worker.yaml
deploy/kubernetes/config/gateway.yaml
```

The Gateway account must use `agent-knowledge-gateway` as its primary group,
`agent-knowledge-ingress` as its only supplementary group, and
`agent-knowledge-ssh-shell` as its login shell. Keep password authentication
disabled and attach a forced command to every authorized key:

```text
restrict,command="akg-v1 /etc/agent-knowledge/gateway.yaml fictional-node-a" ssh-ed25519 <public-key>
```

Initialize and start the default storage layout with:

```sh
sudo agent-knowledge admin bootstrap-storage \
  --config /etc/agent-knowledge/worker.yaml \
  --gateway-owner fictional-agent-knowledge-gateway
sudo systemctl enable --now agent-knowledge-queue-ingress.socket
sudo systemctl enable --now agent-knowledge-worker.service
```

Deploy the configured Quartz launcher before starting the Worker. Sites using
paths outside `/var/lib/agent-knowledge` must provide matching systemd
drop-ins. Never use the Worker or Queue Ingress accounts for SSH.

## Kubernetes deployment

`deploy/kubernetes` is a security-hardened, single-Pod StatefulSet base. It is
not directly deployable because it intentionally omits site-specific storage,
Quartz content, SSH keys, host keys, and credentials.

Create an overlay that supplies:

- a `ReadWriteOncePod` volume with the required POSIX, rename, sync, inode, and
  `flock` semantics;
- immutable Quartz and SSH resources;
- the Gateway's forced-command authorized keys;
- required scheduling/cgroup isolation; and
- deployment-specific configuration and credentials.

Review the rendered overlay before applying it:

```sh
kubectl kustomize deploy/kubernetes
kubectl apply -k /path/to/reviewed-overlay
```

The supported Kubernetes baseline is 1.33 or newer with
`supplementalGroupsPolicy`. Horizontal scaling and multiple Repository Workers
are not supported.

## Operations

Inspect a deployment without changing it:

```sh
agent-knowledge admin status \
  --config /etc/agent-knowledge/worker.yaml \
  --maximum-queue-entries 100000 \
  --timeout-seconds 30
```

Preview and prune inactive derived releases:

```sh
agent-knowledge admin prune-releases \
  --config /etc/agent-knowledge/worker.yaml \
  --dry-run
agent-knowledge admin prune-releases \
  --config /etc/agent-knowledge/worker.yaml
```

Cold backups must be taken while the Worker, Queue Ingress socket, and existing
Gateway sessions are stopped. Back up all five durable sibling roots together
and preserve owners, modes, and links. A restored copy has new filesystem
identities and must be validated and rebound before services restart:

```sh
sudo agent-knowledge admin rebind-restored-storage \
  --config /etc/agent-knowledge/worker.yaml \
  --gateway-owner fictional-agent-knowledge-gateway
```

Configuration, Quartz, SSH material, and Git credentials are separate backup
inputs. Detailed migration and recovery rules are in
[DESIGN.md](DESIGN.md#16-durable-queue).

## Development

Enter the pinned environment and install repository hooks:

```sh
direnv allow
just init
```

Common checks:

```sh
just check                 # source, package, and image checks
just test-systemd-e2e      # NixOS VM and reboot
just test-kubernetes-e2e   # disposable kind cluster
just test-recovery-e2e     # cold backup and restore
```

The CI matrix covers native `x86_64-linux` and `aarch64-linux` packages,
restricted OpenSSH, privilege separation, real Quartz, systemd, Kubernetes,
and cold recovery.

## Release process

Releases use semantic tags such as `v0.1.0`. The tag must match the workspace
version and point to a commit on `main`. Tag CI rebuilds the client archives and
server images, verifies checksums and both architectures, publishes versioned
GHCR manifests, and prepares a draft GitHub Release. The draft is published
only after every tag check succeeds.

New GHCR packages start private. Set each package to public once before
publishing the first release; subsequent versioned images retain that setting.

Pre-1.0 releases may change command and configuration interfaces. Durable
format changes remain explicit and fail closed; release notes identify any
required migration. Downgrades are unsupported unless a release says
otherwise.

Changes are recorded in [CHANGELOG.md](CHANGELOG.md). Security reports follow
[SECURITY.md](.github/SECURITY.md).

## License

Licensed under the [Apache License 2.0](LICENSE).
