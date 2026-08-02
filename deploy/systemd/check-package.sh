#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 || $1 != /* ]]; then
  echo "usage: $0 <absolute-package-path>" >&2
  exit 2
fi

package_path=$1
service="$package_path/lib/systemd/system/agent-knowledge-worker.service"
sysusers="$package_path/lib/sysusers.d/agent-knowledge.conf"
tmpfiles="$package_path/lib/tmpfiles.d/agent-knowledge.conf"

test -f "$service"
test -f "$sysusers"
test -f "$tmpfiles"
test "$(grep -Fxc "ExecStart=$package_path/bin/agent-knowledge worker run --config /etc/agent-knowledge/worker.yaml" "$service")" -eq 1
for directive in \
  'Type=exec' \
  'User=agent-knowledge' \
  'Group=agent-knowledge' \
  'Restart=on-failure' \
  'KillMode=mixed' \
  'TimeoutStopSec=15min' \
  'ConditionPathExists=/etc/agent-knowledge/worker.yaml' \
  'StartLimitIntervalSec=5min' \
  'StartLimitBurst=5' \
  'ReadWritePaths=/var/lib/agent-knowledge'; do
  test "$(grep -Fxc "$directive" "$service")" -eq 1
done
if grep -Fq '@agentKnowledge@' "$service"; then
  echo "unsubstituted service placeholder" >&2
  exit 1
fi
SYSTEMD_LOG_LEVEL=err systemd-analyze verify --man=no --generators=no "$service"

test_root=$(mktemp -d)
trap 'rm -rf -- "$test_root"' EXIT
systemd-sysusers --dry-run --root="$test_root" "$sysusers"
systemd-tmpfiles --dry-run --create --graceful --root="$test_root" "$tmpfiles"
test "$(grep -Ec '^u agent-knowledge - "Agent Knowledge service account" /var/lib/agent-knowledge /bin/sh$' "$sysusers")" -eq 1
test "$(grep -Ec '^d /var/lib/agent-knowledge 0750 root agent-knowledge -$' "$tmpfiles")" -eq 1
test "$(grep -Ec '^d /var/lib/agent-knowledge/(queue|repository|content|work|releases) 0750 agent-knowledge agent-knowledge -$' "$tmpfiles")" -eq 5
