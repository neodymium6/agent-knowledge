#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 || $1 != /* || $(id -u) -ne 0 ]]; then
  echo "usage: sudo $0 <absolute-agent-knowledge-binary>" >&2
  exit 2
fi

source_binary=$1
test_root=$(mktemp -d)
chmod 0711 "$test_root"
activation_pid=
cleanup() {
  if [[ -n $activation_pid ]]; then
    kill "$activation_pid" 2>/dev/null || true
    wait "$activation_pid" 2>/dev/null || true
  fi
  rm -rf -- "$test_root"
}
trap cleanup EXIT

gateway_uid=61100
gateway_gid=61100
queue_uid=61101
queue_gid=61101
worker_uid=61102
worker_gid=61102

install -m 0755 "$source_binary" "$test_root/agent-knowledge"
install -d -m 0711 "$test_root/storage"
install -d -m 2770 -o "$queue_uid" -g "$queue_gid" \
  "$test_root/storage/queue" "$test_root/storage/queue/.locks" \
  "$test_root/storage/queue/incoming" "$test_root/storage/queue/quarantine" \
  "$test_root/storage/queue/worker-tmp" "$test_root/storage/queue/pending" \
  "$test_root/storage/queue/processing" "$test_root/storage/queue/completed" \
  "$test_root/storage/queue/failed"
install -m 0660 -o "$queue_uid" -g "$queue_gid" /dev/null \
  "$test_root/storage/queue/.locks/queue.lock"
install -m 0660 -o "$queue_uid" -g "$queue_gid" /dev/null \
  "$test_root/storage/queue/.locks/repository-writer.lock"
install -d -m 2750 -o "$worker_uid" -g "$gateway_gid" \
  "$test_root/storage/repository" "$test_root/storage/content"
install -d -m 0750 -o "$worker_uid" -g "$worker_gid" \
  "$test_root/storage/work" "$test_root/storage/releases"
install -d -m 0750 -o "$queue_uid" -g "$gateway_gid" "$test_root/run"
install -d -m 0755 "$test_root/package/payload/run"

cat >"$test_root/package/request.json" <<'EOF'
{
  "protocol_version": 1,
  "request_id": "01K00000000000000000000000",
  "title": "Record a fictional privilege-boundary test",
  "project": "fictional-project",
  "document_type": "experiment",
  "created_at": "2026-07-31T03:50:00Z",
  "operations": [{
    "type": "create_document",
    "document_id": "01K00000000000000000000001",
    "content": "run/index.md"
  }]
}
EOF
cat >"$test_root/package/payload/run/index.md" <<'EOF'
---
schema_version: 1
document_id: 01K00000000000000000000001
title: Fictional privilege-boundary test
created: 2026-07-31T03:50:00Z
request_id: 01K00000000000000000000000
status: active
---
Fictional privilege-boundary body.
EOF
tar --format=gnu --sort=name --owner=0 --group=0 --numeric-owner --mtime=@0 \
  -C "$test_root/package" -cf "$test_root/request.tar" request.json payload

cat >"$test_root/gateway.yaml" <<EOF
schema_version: 3
storage:
  queue_socket: $test_root/run/queue-ingress.sock
  git_directory: $test_root/storage/repository
  content_root: $test_root/storage/content
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
  submit_timeout_seconds: 30
EOF
chown root:"$gateway_gid" "$test_root/gateway.yaml" "$test_root/request.tar"
chmod 0640 "$test_root/gateway.yaml" "$test_root/request.tar"

umask 0007
systemd-socket-activate --accept --inetd \
  --listen="$test_root/run/queue-ingress.sock" \
  setpriv --reuid="$queue_uid" --regid="$queue_gid" --clear-groups \
  "$test_root/agent-knowledge" queue-ingress serve \
  --queue-root "$test_root/storage/queue" \
  >"$test_root/activation.log" 2>&1 &
activation_pid=$!
for _ in $(seq 1 100); do
  [[ -S $test_root/run/queue-ingress.sock ]] && break
  sleep 0.05
done
test -S "$test_root/run/queue-ingress.sock"
chown "$queue_uid":"$gateway_gid" "$test_root/run/queue-ingress.sock"
chmod 0660 "$test_root/run/queue-ingress.sock"

submit_response=$(
  setpriv --reuid="$gateway_uid" --regid="$gateway_gid" --clear-groups \
    env SSH_ORIGINAL_COMMAND='akp-v1 submit' \
    "$test_root/agent-knowledge" gateway --config "$test_root/gateway.yaml" \
    --client-id fictional-node-a <"$test_root/request.tar"
)
grep -Fq '"status":"accepted"' <<<"$submit_response"

status_response=$(
  printf '%s\n' '{"protocol_version":1,"request_id":"01K00000000000000000000000"}' |
    setpriv --reuid="$gateway_uid" --regid="$gateway_gid" --clear-groups \
      env SSH_ORIGINAL_COMMAND='akp-v1 status' \
      "$test_root/agent-knowledge" gateway --config "$test_root/gateway.yaml" \
      --client-id fictional-node-a
)
grep -Fq '"status":"pending"' <<<"$status_response"

if setpriv --reuid="$gateway_uid" --regid="$gateway_gid" --clear-groups \
  test -r "$test_root/storage/queue/queue-id"; then
  echo "Gateway identity can read the durable queue" >&2
  exit 1
fi
if setpriv --reuid="$queue_uid" --regid="$queue_gid" --clear-groups \
  test -r "$test_root/storage/repository"; then
  echo "queue ingress identity can read the repository" >&2
  exit 1
fi
setpriv --reuid="$gateway_uid" --regid="$gateway_gid" --clear-groups \
  test -r "$test_root/storage/repository"
if setpriv --reuid="$gateway_uid" --regid="$gateway_gid" --clear-groups \
  touch "$test_root/storage/repository/gateway-write.fixture"; then
  echo "Gateway identity can write the repository" >&2
  exit 1
fi
setpriv --reuid="$worker_uid" --regid="$worker_gid" \
  --groups="$queue_gid" touch \
  "$test_root/storage/queue/pending/01K00000000000000000000000/phase.fixture"
test -f "$test_root/storage/queue/pending/01K00000000000000000000000/phase.fixture"
