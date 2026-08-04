---
name: install-agent-knowledge-client
description: Install or upgrade the static Agent Knowledge client on a Linux coding-agent node, configure its restricted OpenSSH destination, and verify the artifact, host identity, public-key authentication, and connection. Use when an agent needs a complete client setup before reading or writing through an Agent Knowledge Gateway.
---

# Install Agent Knowledge Client

Install a pinned, verified static binary, then configure and test a restricted
SSH destination.

## Select the artifact

1. Obtain an explicit, deployment-approved semantic version; avoid `latest`.
2. Require Linux and map the architecture exactly:

   - `x86_64` -> `x86_64-unknown-linux-musl`
   - `aarch64` or `arm64` -> `aarch64-unknown-linux-musl`

3. Stop on any other platform rather than selecting a near match.
4. Require `ssh`, `tar`, `sha256sum`, `gh`, `file`, and `readelf`; use `curl`
   or `gh` for download.

The artifact name is:

```text
agent-knowledge-client-v<version>-<target>.tar.gz
```

Download that archive and `SHA256SUMS` from the exact GitHub tag:

```text
https://github.com/neodymium6/agent-knowledge/releases/download/v<version>/
```

Use a new temporary directory and only that release's immutable URL/checksums.

## Verify and install

1. Require `sha256sum -c SHA256SUMS --ignore-missing` to report the selected
   archive `OK` (this option checks only downloaded entries).
2. Verify provenance with the GitHub artifact attestation:

   ```sh
   gh attestation verify <archive> --repo neodymium6/agent-knowledge
   ```

   Require success. Otherwise stop unless an independently trusted channel
   supplied a pinned checksum/signature; the adjacent checksum is not one.

3. Before extraction, require one directory containing only the expected
   `agent-knowledge-client`, `LICENSE`, and `README.md`, with no links or
   absolute paths.
4. Extract only there. With `file`, `readelf -h`, and `readelf -l`, require the
   selected ELF machine and no `INTERP` segment/program interpreter.
5. Install as `agent-knowledge-client` in a selected user-owned directory,
   normally `$HOME/.local/bin`. Before unrequested replacement, report the old
   version and obtain approval.
6. Run the newly installed binary by its absolute path, not through `PATH`:

   ```sh
   /home/fictional-agent/.local/bin/agent-knowledge-client --version
   command -v ssh
   ```

7. Put that directory on `PATH`; require `command -v agent-knowledge-client`
   to resolve to the installed file.

## Configure OpenSSH

Obtain the following deployment-specific values instead of guessing them:

- a local destination alias, server hostname, port, and Gateway user;
- the authenticated client ID that the server will associate with this node;
- an existing private key or approval and policy for a new dedicated key;
- the server host-key fingerprint from a trusted channel; and
- an optional `ProxyJump` or proxy command when the deployment requires one.

Preserve the existing SSH config and permissions, reject a conflicting alias,
and add one specific Host block. Require `0700` on `~/.ssh`, `0600` on private
keys/config, and at most `0644` on public keys/`known_hosts`:

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

Prefer an approved key. Generate a dedicated Ed25519 key only with explicit
approval and the user's passphrase/agent policy. Never expose the `0600`
private key; send only `.pub`. The server must bind it by forced command to the
intended client ID.

Because `BatchMode yes` forbids prompts, preload an encrypted key into an
approved `ssh-agent` and confirm it is present. Do not remove its passphrase.

Obtain the host key/fingerprint independently. If using `ssh-keyscan`, compare
its temporary output with the trusted fingerprint via `ssh-keygen -lf` before
adding it; keyscan alone proves nothing. Never disable checking or use
`/dev/null` or unverified `accept-new`.

## Verify the complete setup

1. Use `ssh -G <alias>` to inspect the resolved endpoint, identity, host-key
   policy, and forwarding without connecting.
2. After the public key is registered and `known_hosts` is verified, test the
   restricted protocol rather than requesting a shell:

   ```sh
   agent-knowledge-client recent \
     --destination fictional-knowledge \
     --maximum-results 1
   ```

3. Require `ssh fictional-knowledge true` to fail without shell output;
   arbitrary shell access is a deployment failure.
4. Report the installed client version and path, SSH alias, authenticated
   client ID, and successful protocol check without exposing private material.
