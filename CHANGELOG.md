# Changelog

Notable changes are recorded here. This project follows Semantic Versioning.

## Unreleased

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
