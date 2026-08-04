---
name: install-agent-knowledge-client
description: Install or upgrade the static Agent Knowledge client on a Linux coding-agent node, configure its restricted OpenSSH destination, and verify the artifact, host identity, public-key authentication, and connection. Use when an agent needs a complete client setup before reading or writing through an Agent Knowledge Gateway.
---

# Install Agent Knowledge Client

Install one pinned, verified static binary into a user-owned executable
directory, then configure and test one restricted SSH destination.

## Select the artifact

1. Obtain the desired semantic version. Prefer an explicit deployment-approved
   version over an implicit latest release.
2. Require Linux and map the architecture exactly:

   - `x86_64` -> `x86_64-unknown-linux-musl`
   - `aarch64` or `arm64` -> `aarch64-unknown-linux-musl`

3. Stop on another OS or architecture instead of selecting a near match.
4. Require `ssh`, `tar`, and `sha256sum`; use `curl` or `gh` for download.

The artifact name is:

```text
agent-knowledge-client-v<version>-<target>.tar.gz
```

Download that archive and `SHA256SUMS` from the exact GitHub tag:

```text
https://github.com/neodymium6/agent-knowledge/releases/download/v<version>/
```

Use a new temporary directory. Do not use mutable URLs or accept a checksum
from a different release.

## Verify and install

1. Verify the selected archive with the downloaded checksum file. On GNU
   systems, `sha256sum -c SHA256SUMS --ignore-missing` checks only downloaded
   entries. Require an `OK` result.
2. When GitHub CLI is available, also verify provenance:

   ```sh
   gh attestation verify <archive> --repo neodymium6/agent-knowledge
   ```

3. List the archive before extraction. It must contain one directory with
   `agent-knowledge-client`, `LICENSE`, and `README.md`, and no unexpected links
   or absolute paths.
4. Extract only into the temporary directory. Confirm the binary is a static
   executable for the selected architecture.
5. Install it as `agent-knowledge-client` in a selected user-owned directory,
   normally `$HOME/.local/bin`. If another binary exists, report its version
   and obtain approval before replacement unless the user explicitly requested
   an upgrade.
6. Run:

   ```sh
   agent-knowledge-client --version
   command -v ssh
   ```

7. Ensure the binary directory is on `PATH`.

## Configure OpenSSH

Obtain the following deployment-specific values instead of guessing them:

- a local destination alias, server hostname, port, and Gateway user;
- the authenticated client ID that the server will associate with this node;
- an existing private key or approval and policy for a new dedicated key;
- the server host-key fingerprint from a trusted channel; and
- an optional `ProxyJump` or proxy command when the deployment requires one.

Inspect the existing `~/.ssh/config` before editing it. Preserve existing
entries and permissions, reject a conflicting alias, and add one specific Host
block. Require mode `0700` on `~/.ssh`, `0600` on the private key and
`~/.ssh/config`, and no broader than `0644` on public keys and `known_hosts`.
An example shape is:

```sshconfig
Host fictional-knowledge
    HostName knowledge.example.invalid
    Port 22
    User agent-knowledge-gateway
    IdentityFile ~/.ssh/fictional-agent-knowledge
    IdentitiesOnly yes
    PreferredAuthentications publickey
    PasswordAuthentication no
    KbdInteractiveAuthentication no
    BatchMode yes
    StrictHostKeyChecking yes
    ForwardAgent no
    ForwardX11 no
    ClearAllForwardings yes
    RequestTTY no
```

Use an existing deployment-approved key when available. Generate a dedicated
Ed25519 key only with explicit approval, using the user's passphrase or agent
policy. Keep the private key mode `0600`, never print or transmit it, and send
only the `.pub` file to the server operator. The operator must install that
public key with a forced command bound to the intended client ID before the
connection can succeed.

`BatchMode yes` forbids passphrase prompts. When the approved private key is
encrypted, load it into an approved `ssh-agent` before unattended use and
confirm the intended key is present. Do not remove its passphrase merely to
make the smoke test pass.

Obtain the SSH host key or fingerprint independently. If `ssh-keyscan` is used,
write its output to a temporary file and compare it with the trusted fingerprint
using `ssh-keygen -lf` before appending it to `known_hosts`; keyscan output alone
is not proof of identity. Never use `StrictHostKeyChecking no`,
`UserKnownHostsFile /dev/null`, or an unverified `accept-new` shortcut.

## Verify the complete setup

1. Run `ssh -G <alias>` and inspect the resolved hostname, port, user,
   identity file, host-key policy, and forwarding settings. This does not
   connect.
2. After the public key is registered and `known_hosts` is verified, test the
   restricted protocol rather than requesting a shell:

   ```sh
   agent-knowledge-client recent \
     --destination fictional-knowledge \
     --maximum-results 1
   ```

3. Treat arbitrary shell access as a deployment failure; the account must run
   only the forced Gateway command. Run a harmless command such as
   `ssh fictional-knowledge true` and require it to fail without returning
   shell command output.
4. Report the installed client version and path, SSH alias, authenticated
   client ID, and successful protocol check without exposing private material.
