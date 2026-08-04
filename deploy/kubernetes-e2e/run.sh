#!/usr/bin/env bash
set -euo pipefail

readonly SCRIPT_DIRECTORY
SCRIPT_DIRECTORY=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
readonly REPOSITORY_ROOT
REPOSITORY_ROOT=$(cd -- "$SCRIPT_DIRECTORY/../.." && pwd)
readonly NAMESPACE=agent-knowledge-e2e
readonly REQUEST_ID=01K00000000000000000000000
readonly DOCUMENT_ID=01K00000000000000000000002
readonly KIND_NODE_IMAGE=kindest/node:v1.35.5@sha256:ce977ae6d65918d0b58a5f8b5e940429c2ce42fa3a5619ec2bbc60b949c0ac95
readonly CLUSTER_NAME="agent-knowledge-e2e-$$"
LOCAL_SSH_PORT=

: "${AGENT_KNOWLEDGE_CSI_HOSTPATH_SOURCE:?run this test through the Nix development shell}"
: "${AGENT_KNOWLEDGE_EXTERNAL_SNAPSHOTTER_SOURCE:?run this test through the Nix development shell}"

for program in docker jq kind kubectl kustomize nix ssh-keygen tar; do
  command -v "$program" >/dev/null || {
    echo "required E2E program is unavailable: $program" >&2
    exit 2
  }
done

test -d "$AGENT_KNOWLEDGE_CSI_HOSTPATH_SOURCE/deploy/kubernetes-1.35"
test -d "$AGENT_KNOWLEDGE_EXTERNAL_SNAPSHOTTER_SOURCE/client/config/crd"

temporary_directory=$(mktemp -d "${TMPDIR:-/tmp}/agent-knowledge-kubernetes-e2e.XXXXXX")
export KUBECONFIG="$temporary_directory/kubeconfig"
cluster_started=false
port_forward_pid=

diagnostics() {
  echo "Kubernetes E2E diagnostics:" >&2
  kubectl get nodes,pods,pvc,pv --all-namespaces -o wide >&2 || true
  kubectl get events --all-namespaces --sort-by=.lastTimestamp >&2 || true
  kubectl describe statefulset/agent-knowledge -n "$NAMESPACE" >&2 || true
  kubectl describe pod/agent-knowledge-0 -n "$NAMESPACE" >&2 || true
  kubectl logs statefulset/agent-knowledge -n "$NAMESPACE" \
    --all-containers --prefix >&2 || true
  kubectl logs job/seed-agent-knowledge-quartz-v1 -n "$NAMESPACE" \
    --all-containers --prefix >&2 || true
}

cleanup() {
  status=$?
  trap - EXIT INT TERM
  if [ "$status" -ne 0 ] && [ "$cluster_started" = true ]; then
    diagnostics
  fi
  if [ -n "$port_forward_pid" ]; then
    kill "$port_forward_pid" 2>/dev/null || true
    wait "$port_forward_pid" 2>/dev/null || true
  fi
  if [ "$cluster_started" = true ]; then
    kind delete cluster --name "$CLUSTER_NAME" || true
  fi
  rm -rf -- "$temporary_directory"
  exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

build_output() {
  nix build "$1" --no-link --print-out-paths
}

load_image() {
  kind load image-archive --name "$CLUSTER_NAME" "$1"
}

run_client() {
  HOME="$temporary_directory/client-home" "$client_program" "$@"
}

start_port_forward() {
  if [ -n "$port_forward_pid" ]; then
    kill "$port_forward_pid" 2>/dev/null || true
    wait "$port_forward_pid" 2>/dev/null || true
  fi
  : >"$temporary_directory/port-forward.log"
  kubectl port-forward --address 127.0.0.1 \
    -n "$NAMESPACE" service/agent-knowledge-ssh \
    :2222 >"$temporary_directory/port-forward.log" 2>&1 &
  port_forward_pid=$!
  for _attempt in $(seq 1 30); do
    LOCAL_SSH_PORT=$(sed -n \
      's/^Forwarding from 127\.0\.0\.1:\([0-9][0-9]*\) -> 2222$/\1/p' \
      "$temporary_directory/port-forward.log" | head -n 1)
    if [ -n "$LOCAL_SSH_PORT" ]; then
      return 0
    fi
    if ! kill -0 "$port_forward_pid" 2>/dev/null; then
      cat "$temporary_directory/port-forward.log" >&2
      return 1
    fi
    sleep 1
  done
  echo "SSH port-forward did not become ready" >&2
  return 1
}

wait_for_completed_request() {
  status_file=$temporary_directory/status.json
  for _attempt in $(seq 1 90); do
    run_client client status --destination agent-knowledge-e2e \
      --request-id "$REQUEST_ID" --timeout-seconds 10 >"$status_file"
    case $(jq -r .status "$status_file") in
      completed)
        return 0
        ;;
      failed)
        echo "E2E request failed:" >&2
        jq . "$status_file" >&2
        return 1
        ;;
    esac
    sleep 2
  done
  echo "E2E request did not complete before the deadline" >&2
  return 1
}

cd "$REPOSITORY_ROOT"
docker info >/dev/null

client_package=$(build_output .#agent-knowledge)
worker_image=$(build_output .#worker-container-image)
queue_ingress_image=$(build_output .#queue-ingress-container-image)
openssh_gateway_image=$(build_output .#openssh-gateway-container-image)
storage_bootstrap_image=$(build_output .#storage-bootstrap-container-image)
quartz_fixture_image=$(build_output .#kubernetes-e2e-quartz-container-image)
client_program=$client_package/bin/agent-knowledge

cluster_started=true
kind create cluster --name "$CLUSTER_NAME" \
  --config "$SCRIPT_DIRECTORY/kind.yaml" \
  --image "$KIND_NODE_IMAGE" \
  --wait 180s

for crd in \
  snapshot.storage.k8s.io_volumesnapshotclasses.yaml \
  snapshot.storage.k8s.io_volumesnapshotcontents.yaml \
  snapshot.storage.k8s.io_volumesnapshots.yaml; do
  kubectl apply -f \
    "$AGENT_KNOWLEDGE_EXTERNAL_SNAPSHOTTER_SOURCE/client/config/crd/$crd"
done

bash "$SCRIPT_DIRECTORY/deploy-csi.sh"

load_image "$worker_image"
load_image "$queue_ingress_image"
load_image "$openssh_gateway_image"
load_image "$storage_bootstrap_image"
load_image "$quartz_fixture_image"

kubectl apply -f "$SCRIPT_DIRECTORY/setup.yaml"
kubectl wait --for=condition=complete job/seed-agent-knowledge-quartz-v1 \
  -n "$NAMESPACE" --timeout=180s
kubectl delete job/seed-agent-knowledge-quartz-v1 -n "$NAMESPACE" --wait=true

ssh-keygen -q -t ed25519 -N '' -C agent-knowledge-e2e-client \
  -f "$temporary_directory/client-key"
ssh-keygen -q -t ed25519 -N '' -C agent-knowledge-e2e-host \
  -f "$temporary_directory/host-key"
client_public_key=$(<"$temporary_directory/client-key.pub")
printf 'restrict,command="akg-v1 /etc/agent-knowledge/gateway.yaml fictional-ci-node" %s\n' \
  "$client_public_key" >"$temporary_directory/authorized_keys"

kubectl create secret generic agent-knowledge-ssh-v1 \
  -n "$NAMESPACE" \
  --from-file=ssh_host_ed25519_key="$temporary_directory/host-key" \
  --from-file=authorized_keys="$temporary_directory/authorized_keys" \
  --dry-run=client -o json >"$temporary_directory/secret.json"
jq '.immutable = true' "$temporary_directory/secret.json" \
  >"$temporary_directory/immutable-secret.json"
kubectl apply -f "$temporary_directory/immutable-secret.json"

kustomize build --load-restrictor LoadRestrictionsRootOnly "$SCRIPT_DIRECTORY" \
  >"$temporary_directory/rendered.yaml"
kubectl apply -f "$temporary_directory/rendered.yaml"
kubectl rollout status statefulset/agent-knowledge -n "$NAMESPACE" --timeout=300s
kubectl get node "$CLUSTER_NAME-control-plane" -o json \
  | jq -e '.status.features.supplementalGroupsPolicy == true' >/dev/null

mkdir -p "$temporary_directory/client-home/.ssh"
chmod 0700 "$temporary_directory/client-home/.ssh"
start_port_forward
read -r host_key_type host_key_data _comment <"$temporary_directory/host-key.pub"
printf '[127.0.0.1]:%s %s %s\n' "$LOCAL_SSH_PORT" "$host_key_type" "$host_key_data" \
  >"$temporary_directory/client-home/.ssh/known_hosts"
cat >"$temporary_directory/client-home/.ssh/config" <<EOF
Host agent-knowledge-e2e
  HostName 127.0.0.1
  Port $LOCAL_SSH_PORT
  User agent-knowledge-gateway
  IdentityFile $temporary_directory/client-key
  IdentitiesOnly yes
  StrictHostKeyChecking yes
  UserKnownHostsFile $temporary_directory/client-home/.ssh/known_hosts
  LogLevel ERROR
EOF
chmod 0600 "$temporary_directory/client-home/.ssh/config" \
  "$temporary_directory/client-home/.ssh/known_hosts"

run_client client list --destination agent-knowledge-e2e \
  --maximum-results 10 --timeout-seconds 10 \
  >"$temporary_directory/initial-list.json"
jq -e '.documents == []' "$temporary_directory/initial-list.json" >/dev/null

package_root=$temporary_directory/request
mkdir -p "$package_root/payload/benchmark"
cat >"$package_root/request.json" <<EOF
{
  "protocol_version": 1,
  "request_id": "$REQUEST_ID",
  "title": "Record fictional Kubernetes benchmark",
  "project": "fictional-solver",
  "document_type": "experiment",
  "node": "fictional-ci-node",
  "agent": "codex",
  "session": "01K00000000000000000000001",
  "created_at": "2026-08-04T00:00:00Z",
  "operations": [
    {
      "type": "create_document",
      "document_id": "$DOCUMENT_ID",
      "content": "benchmark/index.md"
    },
    {
      "type": "add_attachment",
      "document_id": "$DOCUMENT_ID",
      "source": "benchmark/results.csv",
      "name": "results.csv"
    }
  ]
}
EOF
cat >"$package_root/payload/benchmark/index.md" <<EOF
---
schema_version: 1
document_id: $DOCUMENT_ID
title: Fictional Kubernetes benchmark
created: 2026-08-04T00:00:00Z
node: fictional-ci-node
agent: codex
session: 01K00000000000000000000001
request_id: $REQUEST_ID
tags:
  - kubernetes
  - benchmark
status: active
---
Fictional Kubernetes persistence needle.
EOF
printf '%s\n' 'step,value' '1,42' >"$package_root/payload/benchmark/results.csv"

run_client client submit --destination agent-knowledge-e2e \
  --package-root "$package_root" --timeout-seconds 30 \
  >"$temporary_directory/submit.json"
jq -e --arg request_id "$REQUEST_ID" \
  '.status == "accepted" and .request_id == $request_id' \
  "$temporary_directory/submit.json" >/dev/null
wait_for_completed_request

run_client client search --destination agent-knowledge-e2e \
  --query 'persistence needle' --project fictional-solver \
  --maximum-results 10 --timeout-seconds 10 \
  >"$temporary_directory/search-before.json"
jq -e --arg document_id "$DOCUMENT_ID" \
  '.documents | length == 1 and .[0].metadata.document_id == $document_id' \
  "$temporary_directory/search-before.json" >/dev/null

run_client client get --destination agent-knowledge-e2e \
  --document-id "$DOCUMENT_ID" --timeout-seconds 10 \
  >"$temporary_directory/get-before.json"
jq -e --arg document_id "$DOCUMENT_ID" \
  '.document.summary.metadata.document_id == $document_id
    and (.document.markdown | contains("Fictional Kubernetes persistence needle."))' \
  "$temporary_directory/get-before.json" >/dev/null
commit_before=$(jq -r .commit "$temporary_directory/get-before.json")

run_client client export --destination agent-knowledge-e2e \
  --document-id "$DOCUMENT_ID" --timeout-seconds 10 \
  >"$temporary_directory/bundle-before.tar"
tar -tf "$temporary_directory/bundle-before.tar" \
  >"$temporary_directory/bundle-members.txt"
grep -Fx index.md "$temporary_directory/bundle-members.txt" >/dev/null
grep -Fx results.csv "$temporary_directory/bundle-members.txt" >/dev/null
test "$(tar -xOf "$temporary_directory/bundle-before.tar" results.csv)" = \
  $'step,value\n1,42'

kubectl delete pod/agent-knowledge-0 -n "$NAMESPACE" --wait=true
kubectl wait --for=create pod/agent-knowledge-0 \
  -n "$NAMESPACE" --timeout=60s
kubectl wait --for=condition=Ready pod/agent-knowledge-0 \
  -n "$NAMESPACE" --timeout=300s
start_port_forward

run_client client get --destination agent-knowledge-e2e \
  --document-id "$DOCUMENT_ID" --timeout-seconds 10 \
  >"$temporary_directory/get-after.json"
jq -e --arg commit "$commit_before" --arg document_id "$DOCUMENT_ID" \
  '.commit == $commit and .document.summary.metadata.document_id == $document_id' \
  "$temporary_directory/get-after.json" >/dev/null
run_client client search --destination agent-knowledge-e2e \
  --query 'persistence needle' --project fictional-solver \
  --maximum-results 10 --timeout-seconds 10 \
  >"$temporary_directory/search-after.json"
jq -e --arg commit "$commit_before" --arg document_id "$DOCUMENT_ID" \
  '.commit == $commit
    and (.documents | length == 1 and .[0].metadata.document_id == $document_id)' \
  "$temporary_directory/search-after.json" >/dev/null

echo "Kubernetes E2E passed at commit $commit_before"
