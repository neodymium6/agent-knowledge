---
name: install-agent-knowledge-server
description: Install or prepare a fresh Agent Knowledge server on a conventional Linux systemd host or a single-replica Kubernetes deployment. Use when an agent must plan, configure, validate, or install the server-side Gateway, queue ingress, Repository Worker, storage, Quartz integration, and restricted SSH entry point.
---

# Install Agent Knowledge Server

Install a commit-pinned release without weakening single-writer, privilege, or
storage boundaries. Prefer systemd; use Kubernetes only when its prerequisites
already exist. Stop on an existing deployment: this fresh-install skill does
not provide the release-specific upgrade and rollback plan it requires.

## Gather deployment inputs

Before changing the target, obtain:

- release version, exact approved commit, and `systemd` or `kubernetes`;
- target host or cluster and supported architecture;
- durable storage location, capacity, backup plan, and filesystem semantics;
- immutable Quartz program/root, release-site path, and external Web/TLS plan;
- SSH endpoint, host-key plan, and one public key/client ID per client; and
- optional Git remote with separate Worker-only credentials.

Do not invent values or put secrets in repositories, logs, ConfigMaps, or
examples. Ask before changing a live target.

## Preflight

1. Resolve the approved version tag to an exact approved commit; a tag is not
   an immutable pin. Stop if cryptographic Nix-source provenance is required:
   releases attest client archives and container digests, not that source.
2. Confirm Linux `x86_64`/`amd64` or `aarch64`/`arm64`.
3. Check its release notes for migrations and ensure no Worker uses the target.
   Require operator review for a nonempty, unmarked storage root.
4. Preserve the five sibling durable roots together: `queue`, `repository`,
   `content`, `work`, and `releases`.
   Keep a configured derived `search-indexes` root as a sibling on the same
   mount; it may be rebuilt instead of backed up.

## Install on systemd Linux

1. Install the exact approved commit into a dedicated root-owned profile. Use
   the same explicit profile path for installation, unit links, and
   administrative commands:

   ```sh
   sudo nix profile add \
     --profile /nix/var/nix/profiles/agent-knowledge \
     github:neodymium6/agent-knowledge/0123456789abcdef0123456789abcdef01234567#agent-knowledge
   /nix/var/nix/profiles/agent-knowledge/bin/agent-knowledge --version
   ```

   Replace the fictional commit with the approved release commit.

2. As root, install the packaged `sysusers.d`, `tmpfiles.d`, Worker unit,
   queue-ingress socket, and service from that same package; do not recreate
   their hardening. Apply sysusers and verify all packaged users and groups.
3. Install the pinned Quartz launcher at the configured immutable path before
   starting the Worker. Quartz content is not included in the server package.
4. Provision a no-password Gateway account with primary group
   `agent-knowledge-gateway`, only supplementary group
   `agent-knowledge-ingress`, and the packaged `agent-knowledge-ssh-shell`.
   Verify PAM/`UsePAM` permits key login; never use Worker or queue accounts.
   Record its numeric UID.
5. Create root-owned `/etc/agent-knowledge/worker.yaml` and `gateway.yaml` from
   the approved commit. Set `identity.gateway_uid` to the actual Gateway
   account UID; the Kubernetes sample value is not valid for a conventional
   host. Keep the default `/var/lib/agent-knowledge` layout unless there is a
   reviewed systemd sandbox override.
6. Configure OpenSSH public-key-only access. Give every key a root-controlled
   forced command:

   ```text
   restrict,command="akg-v1 /etc/agent-knowledge/gateway.yaml fictional-node-a" ssh-ed25519 <public-key>
   ```

   Keep authorized keys and its parents root-controlled and Gateway read-only.
   Disable alternate key sources, password/interactive authentication,
   forwarding, PTY, and user startup features. Enforce finite SSH connection
   and OS process/resource limits for forced commands.

   Run `sshd -t`, then inspect the effective Match configuration with a
   deployment-specific command shaped like:

   ```sh
   sudo sshd -T -C \
     user=fictional-agent-knowledge-gateway,host=knowledge.example.invalid,addr=192.0.2.10
   ```

   Before reload, verify those controls and the expected keys path and limits.
   Audit every key for `restrict`, the forced command, and its client ID.
7. Apply packaged tmpfiles, then initialize fresh storage once:

   ```sh
   sudo /nix/var/nix/profiles/agent-knowledge/bin/agent-knowledge \
     admin bootstrap-storage \
     --config /etc/agent-knowledge/worker.yaml \
     --gateway-owner fictional-agent-knowledge-gateway
   ```

8. Enable the queue-ingress socket and Worker only after configuration,
   Quartz, storage, and SSH validation succeed.

## Install on Kubernetes

Create an overlay from the matching `deploy/kubernetes`; never apply the base.

1. Require Kubernetes 1.33 or newer with `supplementalGroupsPolicy`, a
   single replica, and the documented PID/cgroup isolation.
2. Resolve and attest the released multi-platform digest for
   `storage-bootstrap`, `worker`, `queue-ingress`, and `openssh-gateway`.
   Through the overlay, replace every sample image with its immutable
   `ghcr.io/neodymium6/agent-knowledge-<name>@sha256:<digest>` reference. The
   standalone `gateway` image is not part of this Kubernetes base.
3. Supply one `ReadWriteOncePod` volume with POSIX rename, sync, inode, and
   `flock` semantics. Retain it when the StatefulSet is removed.
4. Supply immutable Quartz, configuration, SSH host keys, and forced-command
   keys. SSH material belongs in Secrets, not ConfigMaps/images. Rotate SSH or
   Quartz with new versioned objects referenced by the Pod template, forcing a
   complete replacement; in-place Secret or claim updates are unsupported.
5. Preserve the provided UIDs, GIDs, security contexts, NetworkPolicy,
   read-only roots, and storage-bootstrap init container.
6. Run `kubectl kustomize` and review the complete render before an explicitly
   approved `kubectl apply -k`.

## Verify and report

- Confirm the queue socket and Worker are healthy and remain distinct users.
- On systemd, run the profiled `agent-knowledge admin status` against the
  installed Worker config. On Kubernetes, run the same read-only check in the
  Worker container through its exact running executable and mounted config:

  ```sh
  kubectl exec agent-knowledge-0 -c worker -- \
    /proc/1/exe admin status --config /etc/agent-knowledge/worker.yaml
  ```

- Test one restricted SSH client destination with `recent`; arbitrary shell
  access must fail.
- Confirm a test request reaches `completed`, commits, and points `current` to
  that complete release. Only project recovery tests establish atomicity.
- Report the version, exact commit, deployment type, validation performed, and
  any site-owned follow-up. Do not print credentials or private infrastructure.
