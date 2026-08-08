#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 4 || $1 != /* || $2 != /* || $3 != /* || $4 != /* || $(id -u) -ne 0 ]]; then
  echo "usage: sudo $0 <absolute-agent-knowledge-binary> <absolute-ssh-shell-binary> <absolute-tmpfiles-config> <absolute-openssh-bin-directory>" >&2
  exit 2
fi

source_binary=$1
source_ssh_shell=$2
tmpfiles_config=$3
openssh_bin=$4
test_root=$(mktemp -d /tmp/agent-knowledge-e2e.XXXXXX)
chmod 0711 "$test_root"
activation_pid=
mismatch_activation_pid=
sshd_pid=
gateway_account_created=false
client_account_created=false
privsep_account_created=false
gateway_group_created=false
ingress_group_created=false
client_group_created=false
privsep_group_created=false
var_empty_created=false
cleanup() {
  cleanup_status=$?
  set +e
  if [[ -n $sshd_pid ]]; then
    kill "$sshd_pid" 2>/dev/null || true
    wait "$sshd_pid" 2>/dev/null || true
  fi
  if [[ -n $activation_pid ]]; then
    kill "$activation_pid" 2>/dev/null || true
    wait "$activation_pid" 2>/dev/null || true
  fi
  if [[ -n $mismatch_activation_pid ]]; then
    kill "$mismatch_activation_pid" 2>/dev/null || true
    wait "$mismatch_activation_pid" 2>/dev/null || true
  fi
  if [[ $client_account_created == true ]]; then
    userdel fictional-ak-client 2>/dev/null || true
  fi
  if [[ $gateway_account_created == true ]]; then
    userdel fictional-ak-gateway 2>/dev/null || true
  fi
  if [[ $privsep_account_created == true ]]; then
    userdel sshd 2>/dev/null || true
  fi
  if [[ $client_group_created == true ]]; then
    groupdel fictional-ak-client 2>/dev/null || true
  fi
  if [[ $ingress_group_created == true ]]; then
    groupdel fictional-ak-ingress 2>/dev/null || true
  fi
  if [[ $gateway_group_created == true ]]; then
    groupdel fictional-ak-gateway 2>/dev/null || true
  fi
  if [[ $privsep_group_created == true ]]; then
    groupdel sshd 2>/dev/null || true
  fi
  if [[ $var_empty_created == true ]]; then
    rmdir /var/empty 2>/dev/null || true
  fi
  if ((cleanup_status != 0)) && [[ -s $test_root/activation.log ]]; then
    echo "queue ingress activation log:" >&2
    sed 's/^/  /' "$test_root/activation.log" >&2
  fi
  if ((cleanup_status != 0)) && [[ -s $test_root/sshd.log ]]; then
    echo "OpenSSH test log:" >&2
    sed 's/^/  /' "$test_root/sshd.log" >&2
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
client_uid=61104
client_gid=61104
privsep_uid=61105
privsep_gid=61105
document_bundle=2026-07-31-01K00000000000000000000001

for program in getent groupadd groupdel timeout useradd userdel; do
  if ! command -v "$program" >/dev/null; then
    echo "OpenSSH privilege-boundary test requires ${program}" >&2
    exit 1
  fi
done
for program in ssh ssh-keygen sshd; do
  if [[ ! -x $openssh_bin/$program ]]; then
    echo "OpenSSH bin directory is missing executable ${program}" >&2
    exit 1
  fi
done

install -m 0755 "$source_binary" "$test_root/agent-knowledge"
install -m 0755 "$source_ssh_shell" "$test_root/agent-knowledge-ssh-shell"

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
test "$(stat -c '%u:%g:%a' "$fresh_root/run/agent-knowledge")" = \
  "$queue_uid:$ingress_gid:2750"
install -D -m 0644 "$fresh_config" \
  "$fresh_root/etc/tmpfiles.d/agent-knowledge.conf"
rm -r -- "$fresh_root/run"
systemd-tmpfiles --create --root="$fresh_root" agent-knowledge.conf
test "$(stat -c '%u:%g:%a' "$fresh_root/run/agent-knowledge")" = \
  "$queue_uid:$ingress_gid:2750"
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
install -d -m 2750 -o "$worker_uid" -g "$gateway_gid" \
  "$test_root/storage/search-indexes"
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

install -d -m 0750 -o "$worker_uid" -g "$worker_gid" \
  "$test_root/quartz-integration"
cat >"$test_root/quartz-integration/build-site" <<'EOF'
#!/bin/sh
printf '%s\n' '<p>fictional site</p>' >"$5/index.html"
EOF
chown "$worker_uid":"$worker_gid" "$test_root/quartz-integration/build-site"
chmod 0500 "$test_root/quartz-integration/build-site"
cat >"$test_root/worker.yaml" <<EOF
schema_version: 1
storage:
  queue_root: $test_root/storage/queue
  repository_root: $test_root/storage/repository
  content_root: $test_root/storage/content
  work_root: $test_root/storage/work
  release_root: $test_root/storage/releases
  search_index_root: $test_root/storage/search-indexes
repository:
  official_branch: main
  author_name: Fictional Knowledge Worker
  author_email: worker@fictional.invalid
quartz:
  program: $test_root/quartz-integration/build-site
  integration_root: $test_root/quartz-integration
  timeout_seconds: 30
batch:
  debounce_seconds: 30
  maximum_age_seconds: 300
  maximum_scan_entries: 1024
  maximum_requests: 100
  maximum_recovery_requests: 10000
EOF
chown root:"$worker_gid" "$test_root/worker.yaml"
chmod 0640 "$test_root/worker.yaml"
set +e
setpriv --reuid="$worker_uid" --regid="$worker_gid" --groups="$queue_gid" \
  env "HOME=$test_root/worker-home" \
  timeout --signal=TERM --kill-after=5 2 \
  "$test_root/agent-knowledge" worker run --config "$test_root/worker.yaml" \
  >"$test_root/worker.log" 2>&1
worker_status=$?
set -e
if ((worker_status != 124)); then
  echo "Worker systemd identity smoke test exited unexpectedly: ${worker_status}" >&2
  sed 's/^/  /' "$test_root/worker.log" >&2
  exit 1
fi
grep -Fq '"event":"worker_started"' "$test_root/worker.log"
grep -Fq '"event":"worker_stopped"' "$test_root/worker.log"

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
cp -a "$test_root/package-two" "$test_root/package-three"
sed -i \
  -e 's/01K00000000000000000000002/01K00000000000000000000004/g' \
  -e 's/01K00000000000000000000003/01K00000000000000000000005/g' \
  "$test_root/package-three/request.json" \
  "$test_root/package-three/payload/run/index.md"

cat >"$test_root/gateway.yaml" <<EOF
schema_version: 4
identity:
  gateway_uid: $gateway_uid
storage:
  queue_socket: $test_root/run/queue-ingress.sock
  git_directory: $test_root/storage/repository
  content_root: $test_root/storage/content
  search_index_root: $test_root/storage/search-indexes
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

if setpriv --reuid="$queue_uid" --regid="$queue_gid" --groups="$ingress_gid" \
  "$test_root/agent-knowledge" queue-ingress serve \
  --queue-root "$test_root/storage/queue" \
  --socket-path "$test_root/run/queue-ingress.sock" </dev/null \
  >"$test_root/unsafe-ingress.log" 2>&1; then
  echo "Queue Ingress accepted an unrelated supplementary group" >&2
  exit 1
fi
grep -Fq 'Queue Ingress identity validation failed' \
  "$test_root/unsafe-ingress.log"

chown "$queue_uid":"$queue_gid" "$test_root/run"
if setpriv --reuid="$queue_uid" --regid="$queue_gid" --clear-groups \
  "$test_root/agent-knowledge" queue-ingress serve \
  --queue-root "$test_root/storage/queue" \
  --socket-path "$test_root/run/queue-ingress.sock" </dev/null \
  >"$test_root/collapsed-ingress.log" 2>&1; then
  echo "Queue Ingress accepted a collapsed socket client group" >&2
  exit 1
fi
grep -Fq 'queue-owner and ingress-client groups' \
  "$test_root/collapsed-ingress.log"
chown "$queue_uid":"$ingress_gid" "$test_root/run"

chmod 2770 "$test_root/run"
if setpriv --reuid="$queue_uid" --regid="$queue_gid" --clear-groups \
  "$test_root/agent-knowledge" queue-ingress serve \
  --queue-root "$test_root/storage/queue" \
  --socket-path "$test_root/run/queue-ingress.sock" </dev/null \
  >"$test_root/writable-runtime.log" 2>&1; then
  echo "Queue Ingress accepted a group-writable socket namespace" >&2
  exit 1
fi
grep -Fq 'mode 2770 does not match required 2750' \
  "$test_root/writable-runtime.log"
chmod 2750 "$test_root/run"

if setpriv --reuid="$gateway_uid" --regid="$gateway_gid" \
  --groups="$ingress_gid,$queue_gid" \
  env SSH_ORIGINAL_COMMAND='akp-v1 list' \
  "$test_root/agent-knowledge" gateway --config "$test_root/gateway.yaml" \
  --client-id fictional-node-a </dev/null \
  >"$test_root/unsafe-gateway.log" 2>&1; then
  echo "Gateway accepted an unrelated supplementary group" >&2
  exit 1
fi
grep -Fq '"error_code":"INTERNAL_ERROR"' "$test_root/unsafe-gateway.log"

chmod 2770 "$test_root/storage/repository"
if setpriv --reuid="$gateway_uid" --regid="$gateway_gid" \
  --groups="$ingress_gid" \
  env SSH_ORIGINAL_COMMAND='akp-v1 list' \
  "$test_root/agent-knowledge" gateway --config "$test_root/gateway.yaml" \
  --client-id fictional-node-a </dev/null \
  >"$test_root/writable-repository.log" 2>&1; then
  echo "Gateway accepted a group-writable repository" >&2
  exit 1
fi
grep -Fq '"error_code":"INTERNAL_ERROR"' \
  "$test_root/writable-repository.log"
chmod 2750 "$test_root/storage/repository"

umask 0007
mismatch_socket=$test_root/run/unexpected-ingress.sock
cp "$test_root/gateway.yaml" "$test_root/mismatch-gateway.yaml"
sed -i \
  "s#queue_socket: .*#queue_socket: $mismatch_socket#" \
  "$test_root/mismatch-gateway.yaml"
chown root:"$gateway_gid" "$test_root/mismatch-gateway.yaml"
chmod 0640 "$test_root/mismatch-gateway.yaml"
systemd-socket-activate --accept --inetd \
  --listen="$mismatch_socket" \
  setpriv --reuid="$queue_uid" --regid="$queue_gid" --clear-groups \
  "$test_root/agent-knowledge" queue-ingress serve \
  --queue-root "$test_root/storage/queue" \
  --socket-path "$test_root/run/queue-ingress.sock" \
  >"$test_root/mismatch-activation.log" 2>&1 &
mismatch_activation_pid=$!
for _ in $(seq 1 100); do
  [[ -S $mismatch_socket ]] && break
  sleep 0.05
done
test -S "$mismatch_socket"
chown "$queue_uid":"$ingress_gid" "$mismatch_socket"
chmod 0660 "$mismatch_socket"
if setpriv --reuid="$gateway_uid" --regid="$gateway_gid" --groups="$ingress_gid" \
  env SSH_ORIGINAL_COMMAND='akp-v1 submit' \
  "$test_root/agent-knowledge" gateway --config "$test_root/mismatch-gateway.yaml" \
  --client-id fictional-node-a <"$test_root/request.tar" \
  >"$test_root/mismatch-gateway.log" 2>&1; then
  echo "Queue Ingress accepted a connection from an unexpected activated socket" >&2
  exit 1
fi
for _ in $(seq 1 100); do
  grep -Fq 'does not match configured path' "$test_root/mismatch-activation.log" && break
  sleep 0.05
done
grep -Fq 'does not match configured path' "$test_root/mismatch-activation.log"
kill "$mismatch_activation_pid" 2>/dev/null || true
wait "$mismatch_activation_pid" 2>/dev/null || true
mismatch_activation_pid=
rm -f -- "$mismatch_socket"

systemd-socket-activate --accept --inetd \
  --listen="$test_root/run/queue-ingress.sock" \
  setpriv --reuid="$queue_uid" --regid="$queue_gid" --clear-groups \
  "$test_root/agent-knowledge" queue-ingress serve \
  --queue-root "$test_root/storage/queue" \
  --socket-path "$test_root/run/queue-ingress.sock" \
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

search_response=$(
  printf '%s\n' '{"protocol_version":1,"query":"\"Fictional privilege-boundary body\"","maximum_results":10}' |
    setpriv --reuid="$gateway_uid" --regid="$gateway_gid" --groups="$ingress_gid" \
      env SSH_ORIGINAL_COMMAND='akp-v1 search' \
      "$test_root/agent-knowledge" gateway --config "$test_root/gateway.yaml" \
      --client-id fictional-node-a
)
grep -Fq '01K00000000000000000000001' <<<"$search_response"

gateway_account=fictional-ak-gateway
gateway_group=fictional-ak-gateway
ingress_group=fictional-ak-ingress
client_account=fictional-ak-client
client_group=fictional-ak-client
for account in "$gateway_account" "$client_account" "$gateway_uid" "$client_uid"; do
  if getent passwd "$account" >/dev/null; then
    echo "OpenSSH test account or UID is already allocated: ${account}" >&2
    exit 1
  fi
done
for group in \
  "$gateway_group" "$ingress_group" "$client_group" \
  "$gateway_gid" "$ingress_gid" "$client_gid"; do
  if getent group "$group" >/dev/null; then
    echo "OpenSSH test group or GID is already allocated: ${group}" >&2
    exit 1
  fi
done

groupadd --gid "$gateway_gid" "$gateway_group"
gateway_group_created=true
groupadd --gid "$ingress_gid" "$ingress_group"
ingress_group_created=true
groupadd --gid "$client_gid" "$client_group"
client_group_created=true
install -d -m 0755 -o 0 -g 0 "$test_root/gateway-home"
install -d -m 0700 -o "$client_uid" -g "$client_gid" "$test_root/client-home"
useradd --uid "$gateway_uid" --gid "$gateway_gid" --groups "$ingress_group" \
  --home-dir "$test_root/gateway-home" \
  --shell "$test_root/agent-knowledge-ssh-shell" --password NP \
  --no-create-home "$gateway_account"
gateway_account_created=true
useradd --uid "$client_uid" --gid "$client_gid" \
  --home-dir "$test_root/client-home" --shell /bin/sh --password NP \
  --no-create-home "$client_account"
client_account_created=true

if [[ ! -e /var/empty ]]; then
  install -d -m 0755 -o 0 -g 0 /var/empty
  var_empty_created=true
fi
var_empty_owner=$(stat -c '%u:%g' /var/empty)
var_empty_mode=$(stat -c '%a' /var/empty)
if [[ ! -d /var/empty || -L /var/empty || $var_empty_owner != 0:0 ]] ||
  (((8#$var_empty_mode & 0022) != 0)); then
  echo "OpenSSH privilege-separation root must be a root-owned protected directory" >&2
  exit 1
fi

if ! getent passwd sshd >/dev/null; then
  if getent passwd "$privsep_uid" >/dev/null; then
    echo "OpenSSH privilege-separation UID is already allocated: ${privsep_uid}" >&2
    exit 1
  fi
  if ! getent group sshd >/dev/null; then
    if getent group "$privsep_gid" >/dev/null; then
      echo "OpenSSH privilege-separation GID is already allocated: ${privsep_gid}" >&2
      exit 1
    fi
    groupadd --gid "$privsep_gid" sshd
    privsep_group_created=true
  fi
  useradd --uid "$privsep_uid" --gid sshd --home-dir /var/empty \
    --shell /bin/false --password '*' --no-create-home sshd
  privsep_account_created=true
fi

ssh_keygen_binary=$openssh_bin/ssh-keygen
ssh_binary=$openssh_bin/ssh
sshd_binary=$openssh_bin/sshd
"$ssh_keygen_binary" -q -t ed25519 -N '' \
  -C fictional-agent-knowledge-host -f "$test_root/ssh-host-key"
"$ssh_keygen_binary" -q -t ed25519 -N '' \
  -C fictional-agent-knowledge-client -f "$test_root/client-key"
chmod 0600 "$test_root/ssh-host-key" "$test_root/client-key"
chown "$client_uid":"$client_gid" "$test_root/client-key" "$test_root/client-key.pub"

client_public_key=$(<"$test_root/client-key.pub")
install -d -m 0750 -o 0 -g "$gateway_gid" \
  "$test_root/gateway-home/.ssh"
printf 'restrict,command="akg-v1 %s fictional-node-a" %s\n' \
  "$test_root/gateway.yaml" "$client_public_key" \
  >"$test_root/gateway-home/.ssh/authorized_keys"
chown 0:"$gateway_gid" "$test_root/gateway-home/.ssh/authorized_keys"
chmod 0640 "$test_root/gateway-home/.ssh/authorized_keys"

ssh_port=
for attempt in $(seq 0 99); do
  candidate_port=$((20000 + ((BASHPID + attempt) % 20000)))
  cat >"$test_root/sshd-config" <<EOF
Port $candidate_port
ListenAddress 127.0.0.1
AddressFamily inet
HostKey $test_root/ssh-host-key
PidFile $test_root/sshd.pid
AuthorizedKeysFile .ssh/authorized_keys
AuthorizedKeysCommand none
TrustedUserCAKeys none
AuthenticationMethods publickey
PubkeyAuthentication yes
PasswordAuthentication no
KbdInteractiveAuthentication no
HostbasedAuthentication no
UsePAM no
PermitRootLogin no
AllowUsers $gateway_account
AllowGroups $gateway_group
PermitTTY no
DisableForwarding yes
AllowAgentForwarding no
AllowTcpForwarding no
GatewayPorts no
X11Forwarding no
PermitTunnel no
PermitUserEnvironment no
PermitUserRC no
MaxAuthTries 3
MaxSessions 1
LoginGraceTime 10
ClientAliveInterval 15
ClientAliveCountMax 1
StrictModes yes
PrintMotd no
PrintLastLog no
UseDNS no
LogLevel VERBOSE
EOF
  "$sshd_binary" -t -f "$test_root/sshd-config"
  "$sshd_binary" -D -e -f "$test_root/sshd-config" \
    >"$test_root/sshd.log" 2>&1 &
  candidate_pid=$!
  sleep 0.1
  if kill -0 "$candidate_pid" 2>/dev/null; then
    sshd_pid=$candidate_pid
    ssh_port=$candidate_port
    break
  fi
  wait "$candidate_pid" 2>/dev/null || true
done
if [[ -z $ssh_port ]]; then
  echo "could not bind an OpenSSH test listener" >&2
  exit 1
fi
for _ in $(seq 1 100); do
  if (: <>"/dev/tcp/127.0.0.1/$ssh_port") 2>/dev/null; then
    break
  fi
  if ! kill -0 "$sshd_pid" 2>/dev/null; then
    echo "OpenSSH test listener exited before accepting connections" >&2
    exit 1
  fi
  sleep 0.05
done
if ! (: <>"/dev/tcp/127.0.0.1/$ssh_port") 2>/dev/null; then
  echo "OpenSSH test listener did not become ready" >&2
  exit 1
fi

install -d -m 0700 -o "$client_uid" -g "$client_gid" "$test_root/client-home/.ssh"
known_host=$(<"$test_root/ssh-host-key.pub")
printf '[127.0.0.1]:%s %s\n' "$ssh_port" "$known_host" \
  >"$test_root/client-home/.ssh/known_hosts"
cat >"$test_root/client-home/.ssh/config" <<EOF
Host fictional-knowledge
  HostName 127.0.0.1
  Port $ssh_port
  User $gateway_account
  IdentityFile $test_root/client-key
  IdentitiesOnly yes
  BatchMode yes
  StrictHostKeyChecking yes
  UserKnownHostsFile $test_root/client-home/.ssh/known_hosts
  GlobalKnownHostsFile /dev/null
  PasswordAuthentication no
  KbdInteractiveAuthentication no
  RequestTTY no
EOF
chown -R "$client_uid":"$client_gid" "$test_root/client-home/.ssh"
chmod 0600 "$test_root/client-home/.ssh/config"
chmod 0644 "$test_root/client-home/.ssh/known_hosts"

client_list_response=$(
  setpriv --reuid="$client_uid" --regid="$client_gid" --clear-groups \
    env "PATH=$openssh_bin:/usr/bin:/bin" HOME="$test_root/client-home" \
    USER="$client_account" LOGNAME="$client_account" \
    "$test_root/agent-knowledge" client list \
    --destination fictional-knowledge --maximum-results 10 --timeout-seconds 10
)
grep -Fq '01K00000000000000000000001' <<<"$client_list_response"

client_submit_response=$(
  setpriv --reuid="$client_uid" --regid="$client_gid" --clear-groups \
    env "PATH=$openssh_bin:/usr/bin:/bin" HOME="$test_root/client-home" \
    USER="$client_account" LOGNAME="$client_account" \
    "$test_root/agent-knowledge" client submit \
    --destination fictional-knowledge --package-root "$test_root/package-three" \
    --timeout-seconds 10
)
grep -Fq '"status":"accepted"' <<<"$client_submit_response"
grep -Fq '"client_id":"fictional-node-a"' \
  "$test_root/storage/queue/pending/01K00000000000000000000004/acceptance.json"
test "$(stat -c '%u:%g' \
  "$test_root/storage/queue/pending/01K00000000000000000000004/acceptance.json")" = \
  "$queue_uid:$queue_gid"
client_status_response=$(
  setpriv --reuid="$client_uid" --regid="$client_gid" --clear-groups \
    env "PATH=$openssh_bin:/usr/bin:/bin" HOME="$test_root/client-home" \
    USER="$client_account" LOGNAME="$client_account" \
    "$test_root/agent-knowledge" client status \
    --destination fictional-knowledge \
    --request-id 01K00000000000000000000004 --timeout-seconds 10
)
grep -Fq '"status":"pending"' <<<"$client_status_response"

set +e
invalid_command_output=$(
  setpriv --reuid="$client_uid" --regid="$client_gid" --clear-groups \
    "$ssh_binary" -F "$test_root/client-home/.ssh/config" fictional-knowledge id 2>&1
)
invalid_command_status=$?
set -e
if ((invalid_command_status == 0)); then
  echo "OpenSSH forced command allowed an arbitrary command" >&2
  exit 1
fi
grep -Fq '"error_code":"INVALID_PROTOCOL"' <<<"$invalid_command_output"
if grep -Fq 'uid=' <<<"$invalid_command_output"; then
  echo "OpenSSH forced command executed the requested shell command" >&2
  exit 1
fi

set +e
setpriv --reuid="$client_uid" --regid="$client_gid" --clear-groups \
  timeout 10 "$ssh_binary" -F "$test_root/client-home/.ssh/config" \
  -o ExitOnForwardFailure=yes -N -R "0:127.0.0.1:$ssh_port" \
  fictional-knowledge >"$test_root/forwarding.log" 2>&1
forwarding_status=$?
set -e
if ((forwarding_status == 0 || forwarding_status == 124)); then
  echo "OpenSSH restriction allowed remote port forwarding" >&2
  exit 1
fi
if ! grep -Fq 'remote port forwarding failed' "$test_root/forwarding.log"; then
  echo "OpenSSH did not report an explicit forwarding rejection" >&2
  exit 1
fi

set +e
printf '%s\n' '{"protocol_version":1,"maximum_results":10}' |
  setpriv --reuid="$client_uid" --regid="$client_gid" --clear-groups \
    timeout 10 "$ssh_binary" -F "$test_root/client-home/.ssh/config" \
    -tt fictional-knowledge 'akp-v1 list' \
    >"$test_root/tty.log" 2>&1
tty_status=${PIPESTATUS[1]}
set -e
if ((tty_status == 124)); then
  echo "OpenSSH TTY restriction check timed out" >&2
  exit 1
fi
if ! grep -Fq 'PTY allocation request failed' "$test_root/tty.log"; then
  echo "OpenSSH restriction allowed TTY allocation" >&2
  exit 1
fi

if setpriv --reuid="$gateway_uid" --regid="$gateway_gid" --groups="$ingress_gid" \
  test -r "$test_root/storage/queue/queue-id"; then
  echo "Gateway identity can read the durable queue" >&2
  exit 1
fi
if setpriv --reuid="$gateway_uid" --regid="$gateway_gid" --groups="$ingress_gid" \
  touch "$test_root/storage/queue/pending/gateway-write.fixture" 2>/dev/null; then
  echo "Gateway identity can write the durable queue" >&2
  exit 1
fi
if setpriv --reuid="$queue_uid" --regid="$queue_gid" --clear-groups \
  test -r "$test_root/storage/repository"; then
  echo "queue ingress identity can read the repository" >&2
  exit 1
fi
if setpriv --reuid="$queue_uid" --regid="$queue_gid" --clear-groups \
  touch "$test_root/storage/repository/ingress-write.fixture" 2>/dev/null; then
  echo "queue ingress identity can write the repository" >&2
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
