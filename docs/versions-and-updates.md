# Client and Gateway versions and release awareness

`agent-knowledge-client --version` prints the local binary version without network
or cache access. `agent-knowledge-client version` returns a JSON report containing
`client_version`, `client_protocol_version`, `server`, and cached `upstream` status.

Add `--destination fictional-knowledge` to query the connected Gateway over its
existing restricted SSH connection. The report includes the Gateway binary's
version, its independent wire protocol version, exact supported commands, and
supported inspection queries. It does not infer the versions of other deployed
components. The SSH observation is bounded to five seconds.

`server.status` is `available`, `not_requested`, `unsupported` (the Gateway rejected
the new command), or `unavailable` (transport or response failure). Older Gateways
need not implement `akp-v1 version`; the reporting command still succeeds and
reports that uncertainty. Different software release numbers do not by themselves
mean a protocol incompatibility. `protocol_matches` and advertised capabilities
are reported separately.

## Checking stable releases

```sh
agent-knowledge-client version --destination fictional-knowledge
agent-knowledge-client update-check --destination fictional-knowledge
```

`version` reads cached upstream information only. `update-check` checks the public
GitHub release API when the persistent cache is due. The destination is optional
for both commands. `client_update_available` and `server_update_available` compare
the latest known stable release independently against the installed client and
Gateway versions. `null` means that a comparison could not be made. Semantic
version precedence is used; build metadata does not make an installed version
older. No command installs updates, changes server configuration, or deploys code.

`upstream` includes:

- `status`: `available`, `unknown`, `unavailable`, or `disabled`.
- `policy`: the effective `auto`, `on`, `off`, or `invalid` configuration.
- `latest`: the stable version and public release page URL, or `null`.
- `checked_at`: Unix UTC seconds of the last successful check, or `null`.
- `last_attempt_at`: Unix UTC seconds of the last attempt, or `null`.
- `cached`: whether this invocation used persisted release metadata.
- `stale`: whether that metadata is at least 24 hours old or a later check failed.
- `error`: a bounded error classification, or `null`.

Failures preserve earlier successful metadata with `stale: true`; a previously
observed release is not presented as a fresh successful check. The failure remains
explicit in `status` and `error`. Offline operation, rate limiting, malformed
responses, and unavailable/corrupt cache files do not turn a successful knowledge
operation into a failure. Invalid CLI arguments or a broken stdout can still fail
the reporting command normally.

## Automatic checks and notices

`AGENT_KNOWLEDGE_UPDATE_CHECK` controls both CLI and MCP release checks:

| Value | Ordinary CLI commands | Explicit update-check or MCP check |
| --- | --- | --- |
| `auto` (default) | Background check and notice only when stderr is a terminal | Check when cache is due |
| `on` | Enable background checks/notices, including redirected stderr | Check when cache is due |
| `off` | No upstream access or notices | No upstream access; local/Gateway reports still work |
| Any other value | Disabled | Disabled; reported as `policy: invalid` |

Ordinary commands start a detached helper when needed and never wait for a network
check. The helper has null stdin/stdout/stderr. A notice uses fresh cached metadata
and appears on stderr after a successful operation. On the first invocation, the
check may finish after the command, so a later invocation displays the notice.
Each newly observed release is announced once per cache. JSON, exported archive
bytes, and successful exit status remain unchanged. Default noninteractive CLI
use is silent; long-running MCP servers never start automatic checks or notices.

Checks are attempted at most once every 24 hours per cache, including failed or
interrupted attempts. Explicit checks obey the same limit. Concurrent processes
use a nonblocking lock; a busy cache is reported as `cache_busy`. A reserved attempt
that has not completed (or was interrupted) is marked `incomplete`, independently
of a confirmed network failure. A backwards wall
clock does not cause repeated requests. Missing cache storage prevents a network
check rather than allowing unthrottled requests. Malformed or unknown cache
schemas are reported as `cache_invalid`; remove the cache file to recover.

The cache is `stable-release-v1.json` under `$XDG_CACHE_HOME/agent-knowledge`, or
`$HOME/.cache/agent-knowledge` when no absolute XDG cache home is available.
`AGENT_KNOWLEDGE_CACHE_DIR` overrides the directory and must be absolute. The cache
contains only public release metadata, attempt/check timestamps, error classes,
and the last announced version. It stores no knowledge or SSH identity details.
For MCP containers, mount a writable persistent cache directory owned by the client
UID (10005 in the provided image) and set `AGENT_KNOWLEDGE_CACHE_DIR` to that mount.
A read-only or unwritable home without an override reports `cache_unavailable`.

## MCP and network boundary

The read-only MCP tool `knowledge_version` returns the same structured report over
STDIO or streamable HTTP. Its optional `check_updates` boolean defaults to `false`
(cache only). Set it to `true` to request a check using the shared 24-hour policy.
`AGENT_KNOWLEDGE_UPDATE_CHECK=off` overrides even this explicit request. MCP tool
results remain normal protocol responses; there are no unsolicited update notices.
The full binary accepts CLI commands under `agent-knowledge client` as well.

Only the client contacts
`https://api.github.com/repos/neodymium6/agent-knowledge/releases/latest`, following
the [GitHub latest-release API](https://docs.github.com/en/rest/releases/releases#get-the-latest-release).
Requests need no GitHub authentication and send no queries, document content, SSH
destinations, private keys, or credentials. Drafts and prereleases are excluded;
release tags must be stable semantic versions and URLs must belong to this public
repository. Redirects and proxies are disabled. TLS certificate verification uses
platform trust; the background helper retains only cache configuration and public
`SSL_CERT_FILE`/`SSL_CERT_DIR` trust settings. The Nix MCP wrapper supplies a CA
bundle when `SSL_CERT_FILE` is not already configured. The request has a three-second budget
and a 256 KiB response limit. The Gateway has no GitHub network dependency.
