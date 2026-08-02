#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 || $1 != /* || $2 != /* || $(id -u) -ne 0 ]]; then
  echo "usage: sudo $0 <absolute-agent-knowledge-binary> <absolute-tmpfiles-config>" >&2
  exit 2
fi

source_binary=$1
tmpfiles_config=$2
test_root=$(mktemp -d)
chmod 0711 "$test_root"
activation_pid=
cleanup() {
  cleanup_status=$?
  set +e
  if [[ -n $activation_pid ]]; then
    kill "$activation_pid" 2>/dev/null || true
    wait "$activation_pid" 2>/dev/null || true
  fi
  if ((cleanup_status != 0)) && [[ -s $test_root/activation.log ]]; then
    echo "queue ingress activation log:" >&2
    sed 's/^/  /' "$test_root/activation.log" >&2
  fi
  rm -rf -- "$test_root"
  exit "$cleanup_status"
}
trap cleanup EXIT

gateway_uid=61100
gateway_gid=61100
ingress_gid=61103
queue_uid=61101
queue_gid=61101
worker_uid=61102
worker_gid=61102
document_bundle=2026-07-31-01K00000000000000000000001

install -m 0755 "$source_binary" "$test_root/agent-knowledge"

fresh_root=$test_root/fresh-root
fresh_config=$test_root/fresh-tmpfiles.conf
install -d -m 0755 "$fresh_root"
sed \
  -e "s/ root agent-knowledge-queue / 0 $queue_gid /g" \
  -e "s/ agent-knowledge-queue agent-knowledge-queue / $queue_uid $queue_gid /g" \
  -e "s/ agent-knowledge agent-knowledge-gateway / $worker_uid $gateway_gid /g" \
  -e "s/ agent-knowledge agent-knowledge / $worker_uid $worker_gid /g" \
  -e "s/ agent-knowledge-queue agent-knowledge-ingress / $queue_uid $ingress_gid /g" \
  "$tmpfiles_config" >"$fresh_config"
systemd-tmpfiles --create --root="$fresh_root" "$fresh_config"
test "$(stat -c '%u:%g:%a' "$fresh_root/var/lib/agent-knowledge")" = \
  "0:$queue_gid:751"
test "$(stat -c '%u:%g:%a' "$fresh_root/var/lib/agent-knowledge/queue")" = \
  "$queue_uid:$queue_gid:2770"
test "$(stat -c '%u:%g:%a' "$fresh_root/var/lib/agent-knowledge/repository")" = \
  "$worker_uid:$gateway_gid:2750"
setpriv --reuid="$worker_uid" --regid="$worker_gid" --groups="$queue_gid" \
  touch "$fresh_root/var/lib/agent-knowledge/queue/pending/fresh-sidecar.fixture"
test "$(stat -c '%g' "$fresh_root/var/lib/agent-knowledge/queue/pending/fresh-sidecar.fixture")" = \
  "$queue_gid"

install -d -m 0750 -o 0 -g "$worker_gid" "$test_root/storage"
install -d -m 0750 -o "$worker_uid" -g "$worker_gid" \
  "$test_root/storage/queue" "$test_root/storage/repository" \
  "$test_root/storage/content"
install -d -m 0750 -o "$worker_uid" -g "$worker_gid" \
  "$test_root/storage/work" "$test_root/storage/releases"
install -d -m 2750 -o "$queue_uid" -g "$ingress_gid" "$test_root/run"
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

(
  umask 0027
  setpriv --reuid="$worker_uid" --regid="$worker_gid" --clear-groups \
    "$test_root/agent-knowledge" admin submit \
    --queue-root "$test_root/storage/queue" \
    --package-root "$test_root/package" >"$test_root/legacy-submit.json"
)
grep -Fq '"status":"accepted"' "$test_root/legacy-submit.json"

install -d -m 0750 -o "$worker_uid" -g "$worker_gid" \
  "$test_root/worker-home" "$test_root/seed"
worker_git=(
  setpriv --reuid="$worker_uid" --regid="$worker_gid" --clear-groups
  env "HOME=$test_root/worker-home"
  git
)
"${worker_git[@]}" init --bare "$test_root/storage/repository"
"${worker_git[@]}" init --initial-branch=main "$test_root/seed"
"${worker_git[@]}" -C "$test_root/seed" config user.name "Fictional Writer"
"${worker_git[@]}" -C "$test_root/seed" config user.email writer@fictional.invalid
install -d -m 0750 -o "$worker_uid" -g "$worker_gid" \
  "$test_root/seed/projects/fictional-project/experiments/$document_bundle"
install -m 0640 -o "$worker_uid" -g "$worker_gid" \
  "$test_root/package/payload/run/index.md" \
  "$test_root/seed/projects/fictional-project/experiments/$document_bundle/index.md"
"${worker_git[@]}" -C "$test_root/seed" add .
"${worker_git[@]}" -C "$test_root/seed" commit -m "Initialize fictional knowledge"
"${worker_git[@]}" -C "$test_root/seed" remote add origin "$test_root/storage/repository"
"${worker_git[@]}" -C "$test_root/seed" push origin main
"${worker_git[@]}" --git-dir="$test_root/storage/repository" symbolic-ref HEAD refs/heads/main
"${worker_git[@]}" --git-dir="$test_root/storage/repository" worktree add \
  "$test_root/storage/content" main

"$test_root/agent-knowledge" admin migrate-v1-storage \
  --queue-root "$test_root/storage/queue" \
  --git-directory "$test_root/storage/repository" \
  --content-root "$test_root/storage/content" \
  --queue-owner "$queue_uid" \
  --queue-group "$queue_gid" \
  --gateway-group "$gateway_gid" >"$test_root/migration.json"
grep -Fq '"status":"completed"' "$test_root/migration.json"
chown 0:"$queue_gid" "$test_root/storage"
chmod 0751 "$test_root/storage"
test "$(stat -c '%u:%g:%a' "$test_root/storage")" = "0:$queue_gid:751"
test "$(stat -c '%u:%g:%a' "$test_root/storage/queue")" = "$queue_uid:$queue_gid:2770"
test "$(stat -c '%g' "$test_root/storage/queue/queue-id")" = "$queue_gid"
test "$(stat -c '%g:%a' "$test_root/storage/repository")" = "$gateway_gid:2750"
test "$(stat -c '%g:%a' "$test_root/storage/content")" = "$gateway_gid:2750"
test "$(stat -c '%g' "$test_root/storage/content/projects/fictional-project/experiments/$document_bundle/index.md")" = "$gateway_gid"
setpriv --reuid="$queue_uid" --regid="$queue_gid" --clear-groups \
  test -r "$test_root/storage"
if setpriv --reuid="$gateway_uid" --regid="$gateway_gid" --clear-groups \
  test -r "$test_root/storage"; then
  echo "Gateway identity can list the durable storage root" >&2
  exit 1
fi

tar --format=gnu --sort=name --owner=0 --group=0 --numeric-owner --mtime=@0 \
  -C "$test_root/package" -cf "$test_root/request.tar" request.json payload
cp -a "$test_root/package" "$test_root/package-two"
sed -i \
  -e 's/01K00000000000000000000000/01K00000000000000000000002/g' \
  -e 's/01K00000000000000000000001/01K00000000000000000000003/g' \
  "$test_root/package-two/request.json" \
  "$test_root/package-two/payload/run/index.md"
tar --format=gnu --sort=name --owner=0 --group=0 --numeric-owner --mtime=@0 \
  -C "$test_root/package-two" -cf "$test_root/request-two.tar" request.json payload

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
chown root:"$gateway_gid" \
  "$test_root/gateway.yaml" "$test_root/request.tar" "$test_root/request-two.tar"
chmod 0640 "$test_root/gateway.yaml" "$test_root/request.tar" "$test_root/request-two.tar"

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
chown "$queue_uid":"$ingress_gid" "$test_root/run/queue-ingress.sock"
chmod 0660 "$test_root/run/queue-ingress.sock"

submit_response=$(
  setpriv --reuid="$gateway_uid" --regid="$gateway_gid" --groups="$ingress_gid" \
    env SSH_ORIGINAL_COMMAND='akp-v1 submit' \
    "$test_root/agent-knowledge" gateway --config "$test_root/gateway.yaml" \
    --client-id fictional-node-a <"$test_root/request.tar"
)
grep -Fq '"status":"existing"' <<<"$submit_response"

new_submit_response=$(
  setpriv --reuid="$gateway_uid" --regid="$gateway_gid" --groups="$ingress_gid" \
    env SSH_ORIGINAL_COMMAND='akp-v1 submit' \
    "$test_root/agent-knowledge" gateway --config "$test_root/gateway.yaml" \
    --client-id fictional-node-a <"$test_root/request-two.tar"
)
grep -Fq '"status":"accepted"' <<<"$new_submit_response"

status_response=$(
  printf '%s\n' '{"protocol_version":1,"request_id":"01K00000000000000000000000"}' |
    setpriv --reuid="$gateway_uid" --regid="$gateway_gid" --groups="$ingress_gid" \
      env SSH_ORIGINAL_COMMAND='akp-v1 status' \
      "$test_root/agent-knowledge" gateway --config "$test_root/gateway.yaml" \
      --client-id fictional-node-a
)
grep -Fq '"status":"pending"' <<<"$status_response"

list_response=$(
  printf '%s\n' '{"protocol_version":1,"maximum_results":10}' |
    setpriv --reuid="$gateway_uid" --regid="$gateway_gid" --groups="$ingress_gid" \
      env SSH_ORIGINAL_COMMAND='akp-v1 list' \
      "$test_root/agent-knowledge" gateway --config "$test_root/gateway.yaml" \
      --client-id fictional-node-a
)
grep -Fq '01K00000000000000000000001' <<<"$list_response"

if setpriv --reuid="$gateway_uid" --regid="$gateway_gid" --groups="$ingress_gid" \
  test -r "$test_root/storage/queue/queue-id"; then
  echo "Gateway identity can read the durable queue" >&2
  exit 1
fi
if setpriv --reuid="$queue_uid" --regid="$queue_gid" --clear-groups \
  test -r "$test_root/storage/repository"; then
  echo "queue ingress identity can read the repository" >&2
  exit 1
fi
setpriv --reuid="$gateway_uid" --regid="$gateway_gid" --groups="$ingress_gid" \
  test -r "$test_root/storage/repository"
if setpriv --reuid="$gateway_uid" --regid="$gateway_gid" --groups="$ingress_gid" \
  touch "$test_root/storage/repository/gateway-write.fixture"; then
  echo "Gateway identity can write the repository" >&2
  exit 1
fi
setpriv --reuid="$worker_uid" --regid="$worker_gid" \
  --groups="$queue_gid" sh -c \
  'temporary=$1/phase.fixture.tmp; final=$1/phase.fixture; : >"$temporary"; mv "$temporary" "$final"' \
  sh "$test_root/storage/queue/pending/01K00000000000000000000002"
test -f "$test_root/storage/queue/pending/01K00000000000000000000002/phase.fixture"
test "$(stat -c '%g' "$test_root/storage/queue/pending/01K00000000000000000000002/phase.fixture")" = "$queue_gid"
