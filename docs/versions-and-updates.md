# Client and Gateway versions

`agent-knowledge-client --version` prints the local binary version without any
network access. `agent-knowledge-client version` returns a JSON report containing
`client_version`, `client_protocol_version`, and `server`.

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

The read-only MCP tool `knowledge_version` exposes the same information over STDIO
and streamable HTTP. It never writes notices into protocol output. The full binary
also accepts these commands under `agent-knowledge client`.
