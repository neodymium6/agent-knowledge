# Changelog

Notable changes are recorded here. This project follows Semantic Versioning.

## Unreleased

- Add committed project discovery through CLI and MCP, with index descriptions,
  total document counts, and optional ranking by exact document-search hit counts.
- Add bounded multi-project scopes to list, recent, search, and search excerpts,
  preserving single-project wire shapes and globally ranked/limited results.

## 0.5.0

- Omit nearly empty shortened context bodies while preserving complete short
  documents. Add opt-in balanced context with per-document shares, reserved
  recent logs, and durable guidance prioritized over repeated operational logs.
- Add opt-in bounded multi-hunk body differences through CLI and MCP, preserving
  the default contiguous-range wire contract and exact official commit metadata.
- Advertise the new read modes as additive Gateway capabilities. Upgrade the
  client and Gateway to use them; no durable storage migration is required.

## 0.4.0

- Add CLI and MCP search excerpts with source fields and truncation markers,
  preserving the configured search semantics and exact committed snapshot.
- Add bounded project context assembled from its index, related guidance,
  and recent records, with selection reasons and omitted-document references.
- Add document history, exact historical Markdown reads, and body/metadata
  comparisons over official Git history, retaining identity across moves and
  archival. Existing v1 read command and response shapes remain unchanged.
- Add read-only client and Gateway version and capability reports through CLI
  commands and the MCP `knowledge_version` tool. Older Gateways and protocol
  differences are reported explicitly, independently of release version skew.
- Add client-side stable release checks with a shared 24-hour persistent cache,
  bounded background requests, and one stderr notice per release during ordinary
  interactive CLI use. Explicit checks are available through CLI and MCP;
  disabling checks prevents upstream requests without disabling version reports.
- Keep update checks independent of knowledge operations and MCP framing,
  including offline, rate-limited, malformed-response, and cache-failure cases.
  Only public release metadata is requested; updates are never installed
  automatically. No durable storage migration is required for this release.

## 0.3.0

- Add a durable, file-based SSH client registry and local administration CLI
  for enrollment, suspension, re-enablement, key rotation, and migration from
  the documented static `authorized_keys` format.
- Add an optional Unix-socket-only, server-rendered client administration Web
  UI with CSRF protection and no built-in authentication or TLS.
- Add a read-only OpenSSH adapter that emits active registry keys with their
  restricted Gateway command.
- Add opt-in systemd/OpenSSH registry lookup through a root-managed command,
  with a dedicated read-only account and end-to-end enrollment, login, and
  suspension coverage.

## 0.2.0

- Serve exact-commit BM25 search from Worker-published Tantivy indexes. Missing,
  stale, or unreadable configured indexes fail retryably; deployments without
  indexed search retain the bounded committed-Markdown backend.

## 0.1.8

- Add structured MCP document archival over STDIO or Streamable HTTP without a
  shared filesystem.

## 0.1.7

- Add structured MCP document creation over STDIO or Streamable HTTP without a
  shared filesystem, while retaining local package submission for advanced
  operations.

## 0.1.6

- Add an optional loopback-only Streamable HTTP MCP transport for same-Pod
  sidecars while retaining the default STDIO transport.
- Add a non-root MCP client sidecar image containing the static client and
  OpenSSH, published for `linux/amd64` and `linux/arm64`.

## 0.1.5

- Add a local STDIO MCP mode to the client for committed reads, search,
  request status, and immutable package submission over the node's existing
  restricted SSH configuration.

## 0.1.4

- Migrate legacy queue bindings from the privileged storage bootstrap so
  retained deployments with separate queue and Worker identities can upgrade
  without granting `CAP_CHOWN` to the Worker.

## 0.1.3

- Allow reciprocal repository bindings to survive reattaching the same
  persistent filesystem with a different Linux device ID. Version 2 bindings
  migrate automatically to filesystem-ID-based version 3 bindings on the next
  writable open while retaining path, inode, official-branch, and live storage
  replacement checks.

## 0.1.2

- Allow a durable queue binding to survive remounting the same persistent
  filesystem with a different Linux device ID. Legacy bindings migrate
  automatically on the next writable open; exact block-level clones still
  require exclusive attachment.
- Accept Release Store output with safe read-only modes `0440` and `0444`
  during storage bootstrap while retaining ownership, ACL, link, type, mount,
  and fingerprint validation.
- Verify published client archives, the Codex plugin, the Nix package, and
  multi-architecture GHCR images with post-release smoke tests.

## 0.1.1

- Add a Codex skills-only plugin for server installation, client installation,
  and client read/write workflows.

## 0.1.0

Initial release of the durable SSH gateway, queue ingress, single repository
writer, Git and Quartz publication pipeline, committed reads and search,
systemd package, and single-replica Kubernetes deployment.

Pre-1.0 command and configuration interfaces may change. Durable format
changes will be called out here with any required migration steps.
