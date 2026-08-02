#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 || $1 != /* ]]; then
  echo "usage: $0 <absolute-package-path>" >&2
  exit 2
fi

package_path=$1
service="$package_path/lib/systemd/system/agent-knowledge-worker.service"
ingress_socket="$package_path/lib/systemd/system/agent-knowledge-queue-ingress.socket"
ingress_service="$package_path/lib/systemd/system/agent-knowledge-queue-ingress@.service"
sysusers="$package_path/lib/sysusers.d/agent-knowledge.conf"
tmpfiles="$package_path/lib/tmpfiles.d/agent-knowledge.conf"

test -f "$service"
test -f "$ingress_socket"
test -f "$ingress_service"
test -f "$sysusers"
test -f "$tmpfiles"
test "$(grep -Fxc "ExecStart=$package_path/bin/agent-knowledge worker run --config /etc/agent-knowledge/worker.yaml" "$service")" -eq 1
for directive in \
  'Type=exec' \
  'User=agent-knowledge' \
  'Group=agent-knowledge' \
  'SupplementaryGroups=agent-knowledge-queue' \
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

for directive in \
  'ListenStream=/run/agent-knowledge/queue-ingress.sock' \
  'SocketUser=agent-knowledge-queue' \
  'SocketGroup=agent-knowledge-ingress' \
  'SocketMode=0660' \
  'DirectoryMode=2750' \
  'Accept=yes' \
  'MaxConnections=64' \
  'RemoveOnStop=yes'; do
  test "$(grep -Fxc "$directive" "$ingress_socket")" -eq 1
done
for directive in \
  'User=agent-knowledge-queue' \
  'Group=agent-knowledge-queue' \
  "ExecStart=$package_path/bin/agent-knowledge queue-ingress serve --queue-root /var/lib/agent-knowledge/queue" \
  'StandardInput=socket' \
  'StandardOutput=socket' \
  'StandardError=journal' \
  'RuntimeMaxSec=65min' \
  'UMask=0007' \
  'PrivateNetwork=yes' \
  'ReadWritePaths=/var/lib/agent-knowledge/queue' \
  'RestrictAddressFamilies=AF_UNIX'; do
  test "$(grep -Fxc "$directive" "$ingress_service")" -eq 1
done
if grep -Fq '@agentKnowledge@' "$ingress_service"; then
  echo "unsubstituted queue ingress service placeholder" >&2
  exit 1
fi
SYSTEMD_LOG_LEVEL=err systemd-analyze verify --man=no --generators=no \
  "$ingress_socket" "$ingress_service"

test_root=$(mktemp -d)
trap 'rm -rf -- "$test_root"' EXIT
systemd-sysusers --dry-run --root="$test_root" "$sysusers"
systemd-tmpfiles --dry-run --create --graceful --root="$test_root" "$tmpfiles"
install -D -m 0644 "$tmpfiles" "$test_root/etc/tmpfiles.d/agent-knowledge.conf"
systemd-tmpfiles --dry-run --create --graceful --root="$test_root" \
  agent-knowledge.conf
test "$(grep -Ec '^u agent-knowledge - "Agent Knowledge Worker service account" /var/lib/agent-knowledge -$' "$sysusers")" -eq 1
test "$(grep -Ec '^u agent-knowledge-queue - "Agent Knowledge queue ingress account" /var/lib/agent-knowledge -$' "$sysusers")" -eq 1
test "$(grep -Ec '^g agent-knowledge-gateway - -$' "$sysusers")" -eq 1
test "$(grep -Ec '^g agent-knowledge-ingress - -$' "$sysusers")" -eq 1
test "$(grep -Ec '^m agent-knowledge agent-knowledge-queue$' "$sysusers")" -eq 1
if grep -Fq '/bin/sh' "$sysusers"; then
  echo "Worker account must not have a login shell" >&2
  exit 1
fi
test "$(grep -Ec '^d /var/lib/agent-knowledge 0751 root agent-knowledge-queue -$' "$tmpfiles")" -eq 1
test "$(grep -Ec '^d /var/lib/agent-knowledge/queue 2770 agent-knowledge-queue agent-knowledge-queue -$' "$tmpfiles")" -eq 1
test "$(grep -Ec '^d /var/lib/agent-knowledge/queue/(\.locks|incoming|quarantine|worker-tmp|pending|processing|completed|failed) 2770 agent-knowledge-queue agent-knowledge-queue -$' "$tmpfiles")" -eq 8
test "$(grep -Ec '^z /var/lib/agent-knowledge/queue(/(\.locks|incoming|quarantine|worker-tmp|pending|processing|completed|failed))? 2770 - - -$' "$tmpfiles")" -eq 9
test "$(grep -Ec '^f /var/lib/agent-knowledge/queue/\.locks/(queue|repository-writer)\.lock 0660 agent-knowledge-queue agent-knowledge-queue -$' "$tmpfiles")" -eq 2
test "$(grep -Ec '^d /var/lib/agent-knowledge/(repository|content) 2750 agent-knowledge agent-knowledge-gateway -$' "$tmpfiles")" -eq 2
test "$(grep -Ec '^z /var/lib/agent-knowledge/(repository|content) 2750 - - -$' "$tmpfiles")" -eq 2
test "$(grep -Ec '^d /var/lib/agent-knowledge/(work|releases) 0750 agent-knowledge agent-knowledge -$' "$tmpfiles")" -eq 2
test "$(grep -Ec '^d /run/agent-knowledge 2750 agent-knowledge-queue agent-knowledge-ingress -$' "$tmpfiles")" -eq 1
test "$(grep -Ec '^z /run/agent-knowledge 2750 - - -$' "$tmpfiles")" -eq 1
