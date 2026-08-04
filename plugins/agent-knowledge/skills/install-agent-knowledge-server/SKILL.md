---
name: install-agent-knowledge-server
description: Install or prepare an Agent Knowledge server on a conventional Linux systemd host or a single-replica Kubernetes deployment. Use when an agent must plan, configure, validate, install, or upgrade the server-side Gateway, queue ingress, Repository Worker, storage, Quartz integration, and restricted SSH entry point.
---

# Install Agent Knowledge Server

Install a pinned release without weakening its single-writer, privilege, or
storage boundaries. Prefer conventional Linux with systemd; use Kubernetes
when the deployment already has the required cluster facilities.

## Gather deployment inputs

Obtain these values before changing the target:

- release version and deployment type (`systemd` or `kubernetes`);
- target host or cluster and supported architecture;
- durable storage location, capacity, backup plan, and filesystem semantics;
- immutable Quartz program and integration root;
- SSH endpoint, one public key and client ID per client, and host-key plan;
- optional Git remote and a separate Worker-only credential plan; and
- release-site serving path. Web authentication and TLS are external.

Do not invent infrastructure values. Never copy private keys, tokens, or Git
credentials into the repository, logs, ConfigMaps, or generated examples. Ask
before installing packages, changing accounts, applying manifests, or starting
services on a live target.

## Preflight

1. Pin one semantic version. Do not deploy mutable tags such as `latest`.
2. Confirm Linux `x86_64`/`amd64` or `aarch64`/`arm64`.
3. Inspect the matching tagged source and release notes for migrations.
4. Verify that no Worker is already using the selected storage. Treat a
   nonempty unmarked storage root as an operator review condition.
5. Preserve the five sibling durable roots together: `queue`, `repository`,
   `content`, `work`, and `releases`.

## Install on systemd Linux

1. Install the tagged Nix package into a dedicated root-owned profile. Use the
   same explicit profile path for installation, upgrades, unit links, and
   administrative commands:

   ```sh
   sudo nix profile add \
     --profile /nix/var/nix/profiles/agent-knowledge \
     github:neodymium6/agent-knowledge/v0.1.0#agent-knowledge
   /nix/var/nix/profiles/agent-knowledge/bin/agent-knowledge --version
   ```

   Replace `0.1.0` with the selected version everywhere.

2. Install the packaged `sysusers.d`, `tmpfiles.d`, Worker unit, queue-ingress
   socket, and instantiated queue-ingress service as root. Use the files from
   the same package version; do not reconstruct their hardening manually.
3. Create root-owned `/etc/agent-knowledge/worker.yaml` and `gateway.yaml` from
   the matching tagged schemas. Keep the default `/var/lib/agent-knowledge`
   layout unless there is a reviewed systemd sandbox override.
4. Install the pinned Quartz launcher at the configured immutable path before
   starting the Worker. Quartz content is not included in the server package.
5. Provision one dedicated Gateway account. Its primary group is
   `agent-knowledge-gateway`, its only supplementary group is
   `agent-knowledge-ingress`, and its shell is the packaged
   `agent-knowledge-ssh-shell`. Give it no usable password, but verify that the
   selected PAM and `UsePAM` policy still permits public-key login. Never use
   the Worker or queue accounts for SSH.
6. Configure OpenSSH public-key-only access. Give every key a root-controlled
   forced command:

   ```text
   restrict,command="akg-v1 /etc/agent-knowledge/gateway.yaml fictional-node-a" ssh-ed25519 <public-key>
   ```

   Keep the authorized-keys file and every parent root-controlled and
   non-writable by the Gateway account. Disable `AuthorizedKeysCommand`,
   `TrustedUserCAKeys`, password and keyboard-interactive authentication,
   forwarding, PTY, and user startup features for that account. Apply finite
   SSH connection limits and an OS-enforced process/resource limit covering
   its forced-command processes.

   Run `sshd -t`, then inspect the effective Match configuration with a
   deployment-specific command shaped like:

   ```sh
   sudo sshd -T -C \
     user=fictional-agent-knowledge-gateway,host=knowledge.example.invalid,addr=192.0.2.10
   ```

   Require public-key-only authentication, disabled alternate key sources and
   forwarding, the expected authorized-keys path, and the reviewed connection
   limits before reload. Audit every non-comment authorized-key entry for
   `restrict` and the expected root-controlled `command` and client ID.
7. Apply sysusers/tmpfiles, then initialize fresh storage once:

   ```sh
   sudo /nix/var/nix/profiles/agent-knowledge/bin/agent-knowledge \
     admin bootstrap-storage \
     --config /etc/agent-knowledge/worker.yaml \
     --gateway-owner fictional-agent-knowledge-gateway
   ```

8. Enable the queue-ingress socket and Worker only after configuration,
   Quartz, storage, and SSH validation succeed.

## Install on Kubernetes

Use the matching `deploy/kubernetes` directory as a base and create an overlay.
Do not apply the base directly.

1. Require Kubernetes 1.33 or newer with `supplementalGroupsPolicy`, a
   single replica, and the documented PID/cgroup isolation.
2. Resolve the released multi-platform digest and verify the GitHub attestation
   for each of these four images: `storage-bootstrap`, `worker`,
   `queue-ingress`, and `openssh-gateway`. Replace every `example.invalid`
   image through the overlay with the corresponding immutable
   `ghcr.io/neodymium6/agent-knowledge-<name>@sha256:<digest>` reference. The
   standalone `gateway` image is not part of this Kubernetes base.
3. Supply one `ReadWriteOncePod` volume with POSIX rename, sync, inode, and
   `flock` semantics. Retain it when the StatefulSet is removed.
4. Supply immutable Quartz content, Worker and Gateway configuration, SSH host
   keys, and forced-command authorized keys. Put SSH material in Secrets, not
   ConfigMaps or image layers. Rotate SSH or Quartz inputs by creating new
   versioned objects and changing their names in the Pod template so Kubernetes
   replaces the complete Pod; in-place Secret or claim updates are unsupported.
5. Preserve the provided UIDs, GIDs, security contexts, NetworkPolicy,
   read-only roots, and storage-bootstrap init container.
6. Run `kubectl kustomize` and review the complete render before an explicitly
   approved `kubectl apply -k`.

## Verify and report

- Confirm the queue socket and Worker are healthy and remain distinct users.
- Run the profiled `agent-knowledge admin status` against the installed Worker
  config.
- Test one restricted SSH client destination with `recent`; arbitrary shell
  access must fail.
- Confirm a test request reaches `completed`, creates a Git commit, and leaves
  `current` pointing to the complete release for that commit. Use the project's
  recovery tests, not this smoke test, as evidence of atomic switching.
- Report the pinned version, deployment type, validation performed, and any
  site-owned follow-up. Do not print credentials or private infrastructure.
