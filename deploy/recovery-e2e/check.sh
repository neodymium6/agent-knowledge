#!/usr/bin/env bash
set -Eeuo pipefail

if [[ $# -ne 1 || $1 != /* || $(id -u) -ne 0 ]]; then
  echo "usage: sudo $0 <absolute-agent-knowledge-binary>" >&2
  exit 2
fi

binary=$1
test_root=$(mktemp -d /tmp/agent-knowledge-recovery-e2e.XXXXXX)
storage_root=$(mktemp -d /var/lib/fictional-agent-knowledge-recovery.XXXXXX)
runtime_root=/run/$(basename "$storage_root")
worker_pid=
created_accounts=()
created_groups=()

report_error() {
  local status=$?
  local line=$1
  local command=$2
  trap - ERR
  printf 'recovery E2E failed at line %s: %s (exit %s)\n' \
    "$line" "$command" "$status" >&2
  exit "$status"
}
trap 'report_error "$LINENO" "$BASH_COMMAND"' ERR

gateway_account=fictional-ak-recovery-gateway
queue_account=fictional-ak-recovery-queue
worker_account=fictional-ak-recovery-worker
gateway_group=fictional-ak-recovery-gateway
ingress_group=fictional-ak-recovery-ingress
queue_group=fictional-ak-recovery-queue
worker_group=fictional-ak-recovery-worker
gateway_uid=61201
queue_uid=61202
worker_uid=61203
gateway_gid=61201
queue_gid=61202
worker_gid=61203
ingress_gid=61204

cleanup() {
  cleanup_status=$?
  set +e
  if [[ -n $worker_pid ]]; then
    kill -TERM "$worker_pid" 2>/dev/null || true
    wait "$worker_pid" 2>/dev/null || true
  fi
  if ((cleanup_status != 0)); then
    for log in "$test_root"/worker-*.log; do
      if [[ -s $log ]]; then
        echo "$(basename "$log"):" >&2
        sed 's/^/  /' "$log" >&2
      fi
    done
    for diagnostic in \
      "$test_root"/submit-*.json \
      "$test_root"/status-*.json \
      "$test_root"/unsafe-bootstrap.log; do
      if [[ -s $diagnostic ]]; then
        echo "$(basename "$diagnostic"):" >&2
        sed 's/^/  /' "$diagnostic" >&2
      fi
    done
  fi
  for account in "${created_accounts[@]}"; do
    userdel "$account" 2>/dev/null || true
  done
  for group in "${created_groups[@]}"; do
    groupdel "$group" 2>/dev/null || true
  done
  rm -rf -- "$runtime_root" "$storage_root" "$test_root"
  exit "$cleanup_status"
}
trap cleanup EXIT

for program in getent git groupadd groupdel jq setpriv tar useradd userdel; do
  if ! command -v "$program" >/dev/null; then
    echo "recovery E2E requires ${program}" >&2
    exit 1
  fi
done
if [[ ! -x $binary ]]; then
  echo "agent-knowledge binary is not executable" >&2
  exit 1
fi

for identity in \
  "$gateway_account" "$queue_account" "$worker_account" \
  "$gateway_uid" "$queue_uid" "$worker_uid"; do
  if getent passwd "$identity" >/dev/null; then
    echo "recovery E2E account or UID is already allocated: ${identity}" >&2
    exit 1
  fi
done
for identity in \
  "$gateway_group" "$ingress_group" "$queue_group" "$worker_group" \
  "$gateway_gid" "$ingress_gid" "$queue_gid" "$worker_gid"; do
  if getent group "$identity" >/dev/null; then
    echo "recovery E2E group or GID is already allocated: ${identity}" >&2
    exit 1
  fi
done

groupadd --gid "$gateway_gid" "$gateway_group"
created_groups+=("$gateway_group")
groupadd --gid "$ingress_gid" "$ingress_group"
created_groups+=("$ingress_group")
groupadd --gid "$queue_gid" "$queue_group"
created_groups+=("$queue_group")
groupadd --gid "$worker_gid" "$worker_group"
created_groups+=("$worker_group")
useradd --uid "$gateway_uid" --gid "$gateway_gid" --groups "$ingress_group" \
  --home-dir /var/empty --shell /bin/false --password '*' --no-create-home \
  "$gateway_account"
created_accounts+=("$gateway_account")
useradd --uid "$queue_uid" --gid "$queue_gid" \
  --home-dir /var/empty --shell /bin/false --password '*' --no-create-home \
  "$queue_account"
created_accounts+=("$queue_account")
useradd --uid "$worker_uid" --gid "$worker_gid" --groups "$queue_group" \
  --home-dir /var/empty --shell /bin/false --password '*' --no-create-home \
  "$worker_account"
created_accounts+=("$worker_account")

chmod 0755 "$test_root" "$storage_root"
quartz_root=$test_root/quartz
install -d -m 0755 -o 0 -g 0 "$quartz_root"
cat >"$quartz_root/build-site" <<'EOF'
#!/bin/sh
set -eu
if [ "$#" -ne 5 ] || [ "$1" != build ] || [ "$2" != -d ] || [ "$4" != -o ]; then
  exit 2
fi
test -d "$3"
test -d "$5"
printf '%s\n' '<p>fictional recovery E2E site</p>' >"$5/index.html"
EOF
chmod 0555 "$quartz_root/build-site"

config=$test_root/worker.yaml
cat >"$config" <<EOF
schema_version: 1
storage:
  queue_root: $storage_root/queue
  repository_root: $storage_root/repository
  content_root: $storage_root/content
  work_root: $storage_root/work
  release_root: $storage_root/releases
repository:
  official_branch: main
  author_name: Fictional Recovery Worker
  author_email: recovery-worker@fictional.invalid
quartz:
  program: $quartz_root/build-site
  integration_root: $quartz_root
  timeout_seconds: 30
batch:
  debounce_seconds: 1
  maximum_age_seconds: 2
  maximum_scan_entries: 1024
  maximum_requests: 100
  maximum_recovery_requests: 10000
EOF
chown 0:"$worker_gid" "$config"
chmod 0640 "$config"

"$binary" admin bootstrap-storage \
  --config "$config" \
  --runtime-directory "$runtime_root" \
  --worker-owner "$worker_account" \
  --worker-group "$worker_group" \
  --queue-owner "$queue_account" \
  --queue-group "$queue_group" \
  --gateway-owner "$gateway_account" \
  --gateway-group "$gateway_group" \
  --ingress-group "$ingress_group" >"$test_root/bootstrap.json"
jq -e '.status == "initialized"' "$test_root/bootstrap.json" >/dev/null

make_package() {
  local package_root=$1
  local request_id=$2
  local document_id=$3
  local title=$4
  local body=$5
  install -d -m 0755 "$package_root/payload/run"
  cat >"$package_root/request.json" <<EOF
{
  "protocol_version": 1,
  "request_id": "$request_id",
  "title": "$title",
  "project": "fictional-recovery",
  "document_type": "experiment",
  "created_at": "2026-08-04T00:00:00Z",
  "operations": [{
    "type": "create_document",
    "document_id": "$document_id",
    "content": "run/index.md"
  }]
}
EOF
  cat >"$package_root/payload/run/index.md" <<EOF
---
schema_version: 1
document_id: $document_id
title: $title
created: 2026-08-04T00:00:00Z
request_id: $request_id
status: active
---
$body
EOF
  chmod 0644 "$package_root/request.json" "$package_root/payload/run/index.md"
}

submit_package() {
  local package_root=$1
  local response=$2
  setpriv --reuid="$queue_uid" --regid="$queue_gid" --clear-groups \
    "$binary" admin submit \
    --queue-root "$storage_root/queue" \
    --package-root "$package_root" >"$response"
  jq -e '.status == "accepted"' "$response" >/dev/null
}

start_worker() {
  local log=$1
  setpriv --reuid="$worker_uid" --regid="$worker_gid" --groups="$queue_gid" \
    env HOME=/var/empty \
    "$binary" worker run --config "$config" >"$log" 2>&1 &
  worker_pid=$!
}

stop_worker() {
  kill -TERM "$worker_pid"
  wait "$worker_pid"
  worker_pid=
}

wait_for_counts() {
  local pending=$1
  local completed=$2
  local status_file=$3
  for _ in $(seq 1 120); do
    if "$binary" admin status --config "$config" >"$status_file" 2>/dev/null &&
      jq -e ".queue.pending == $pending and .queue.processing == 0 and .queue.completed == $completed and .queue.failed == 0" \
        "$status_file" >/dev/null; then
      return 0
    fi
    sleep 0.25
  done
  echo "queue did not reach pending=${pending}, completed=${completed}" >&2
  return 1
}

first_request=01K00000000000000000000010
first_document=01K00000000000000000000011
second_request=01K00000000000000000000020
second_document=01K00000000000000000000021
make_package "$test_root/package-one" "$first_request" "$first_document" \
  "Fictional pre-backup result" "Pre-backup durable body."
submit_package "$test_root/package-one" "$test_root/submit-one.json"
start_worker "$test_root/worker-before-backup.log"
wait_for_counts 0 1 "$test_root/status-before-backup.json"
stop_worker

make_package "$test_root/package-two" "$second_request" "$second_document" \
  "Fictional pending result" "Post-restore durable body."
submit_package "$test_root/package-two" "$test_root/submit-two.json"
wait_for_counts 1 1 "$test_root/status-at-backup.json"

queue_identity_before=$(<"$storage_root/queue/queue-id")
commit_before=$(git --git-dir="$storage_root/repository" rev-parse refs/heads/main)
release_before=$(readlink "$storage_root/releases/current")
tar --acls --xattrs --numeric-owner -C /var/lib -cpf "$test_root/storage.tar" \
  "$(basename "$storage_root")"

rm -rf -- "$runtime_root" "$storage_root"
tar --acls --xattrs --numeric-owner -C /var/lib -xpf "$test_root/storage.tar"

if "$binary" admin bootstrap-storage \
  --config "$config" \
  --runtime-directory "$runtime_root" \
  --worker-owner "$worker_account" \
  --worker-group "$worker_group" \
  --queue-owner "$queue_account" \
  --queue-group "$queue_group" \
  --gateway-owner "$gateway_account" \
  --gateway-group "$gateway_group" \
  --ingress-group "$ingress_group" >"$test_root/unsafe-bootstrap.log" 2>&1; then
  echo "normal bootstrap accepted copied storage without explicit rebinding" >&2
  exit 1
fi
grep -Eq \
  '^(queue validation failed: durable queue instance identity is invalid|repository binding validation failed: repository is bound to a different writer configuration|release validation failed: release storage binding changed)$' \
  "$test_root/unsafe-bootstrap.log"

"$binary" admin rebind-restored-storage \
  --config "$config" \
  --runtime-directory "$runtime_root" \
  --worker-owner "$worker_account" \
  --worker-group "$worker_group" \
  --queue-owner "$queue_account" \
  --queue-group "$queue_group" \
  --gateway-owner "$gateway_account" \
  --gateway-group "$gateway_group" \
  --ingress-group "$ingress_group" >"$test_root/restore.json"
jq -e '.status == "rebound"' "$test_root/restore.json" >/dev/null

test "$(<"$storage_root/queue/queue-id")" = "$queue_identity_before"
test "$(git --git-dir="$storage_root/repository" rev-parse refs/heads/main)" = \
  "$commit_before"
test "$(readlink "$storage_root/releases/current")" = "$release_before"
grep -Fq 'Pre-backup durable body.' \
  "$storage_root/content/projects/fictional-recovery/experiments/2026-08-04-$first_document/index.md"
test -f "$storage_root/releases/current/site/index.html"
wait_for_counts 1 1 "$test_root/status-after-restore.json"

start_worker "$test_root/worker-after-restore.log"
wait_for_counts 0 2 "$test_root/status-after-restart.json"
stop_worker
grep -Fq 'Post-restore durable body.' \
  "$storage_root/content/projects/fictional-recovery/experiments/2026-08-04-$second_document/index.md"
test "$(git --git-dir="$storage_root/repository" rev-list --count refs/heads/main)" = 3

printf '%s\n' '{"status":"passed"}'
