#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <e2e-directory>" >&2
  exit 2
fi

e2e_directory=$1
base_directory=$(cd -- "$e2e_directory/../kubernetes" && pwd)
temporary_directory=$(mktemp -d "${TMPDIR:-/tmp}/agent-knowledge-kubernetes-e2e-check.XXXXXX")
trap 'rm -rf -- "$temporary_directory"' EXIT
rendered_manifest=$temporary_directory/rendered.yaml
resources_json=$temporary_directory/resources.json
setup_json=$temporary_directory/setup.json
kind_json=$temporary_directory/kind.json

bash -n "$e2e_directory/run.sh"
bash -n "$e2e_directory/deploy-csi.sh"
if ! awk '
  FNR == 1 { previous = "" }
  /^readonly [A-Z][A-Z0-9_]*$/ {
    if (previous !~ "^" $2 "=") {
      exit 1
    }
  }
  { previous = $0 }
' "$e2e_directory/run.sh" "$e2e_directory/deploy-csi.sh"; then
  echo "E2E scripts must assign variables before marking them read-only" >&2
  exit 1
fi
bash "$base_directory/check-manifests.sh" \
  "$e2e_directory" "$base_directory/kube-linter.yaml"
kube-linter lint --config "$base_directory/kube-linter.yaml" \
  "$e2e_directory/setup.yaml"

kustomize build --load-restrictor LoadRestrictionsRootOnly \
  "$e2e_directory" >"$rendered_manifest"
yq eval-all -o=json -I=0 '[.]' "$rendered_manifest" >"$resources_json"
yq eval-all -o=json -I=0 '[.]' "$e2e_directory/setup.yaml" >"$setup_json"
yq -o=json -I=0 '.' "$e2e_directory/kind.yaml" >"$kind_json"

jq -e '
  map(select(. != null)) as $resources
  | def resource($kind; $name):
      $resources | map(select(.kind == $kind and .metadata.name == $name))[0];
    def stateful_set: resource("StatefulSet"; "agent-knowledge");
    def configuration:
      $resources | map(select(.kind == "ConfigMap"))[0];
    ($resources | all(.[]; .metadata.namespace == "agent-knowledge-e2e"))
    and (stateful_set.spec.template.spec.containers
      | map(.image) | sort == [
        "agent-knowledge-openssh-gateway:0.1.3",
        "agent-knowledge-queue-ingress:0.1.3",
        "agent-knowledge-worker:0.1.3"
      ])
    and (stateful_set.spec.template.spec.initContainers
      | all(.[]; .image == "agent-knowledge-storage-bootstrap:0.1.3"))
    and (stateful_set.spec.volumeClaimTemplates[0].spec.storageClassName
      == "csi-hostpath-sc")
    and (stateful_set.spec.volumeClaimTemplates[0].spec.resources.requests.storage
      == "256Mi")
    and (configuration.metadata.name | startswith("agent-knowledge-config-"))
    and (configuration.data["worker.yaml"]
      | contains("debounce_seconds: 1"))
' "$resources_json" >/dev/null

jq -e '
  map(select(. != null)) as $resources
  | def resource($kind; $name):
      $resources | map(select(.kind == $kind and .metadata.name == $name))[0];
    ($resources | length == 5)
    and (resource("Namespace"; "agent-knowledge-e2e") != null)
    and (resource("StorageClass"; "csi-hostpath-sc").provisioner
      == "hostpath.csi.k8s.io")
    and (resource("PersistentVolumeClaim"; "agent-knowledge-quartz-v1").spec.accessModes
      == ["ReadWriteOncePod"])
    and (resource("PersistentVolumeClaim"; "agent-knowledge-quartz-v1").spec.storageClassName
      == "csi-hostpath-sc")
    and (resource("Job"; "seed-agent-knowledge-quartz-v1").spec.backoffLimit == 0)
    and (resource("Job"; "seed-agent-knowledge-quartz-v1").spec.template.spec
      .automountServiceAccountToken == false)
    and (resource("Job"; "seed-agent-knowledge-quartz-v1").spec.template.spec.containers[0]
      .image == "agent-knowledge-kubernetes-e2e-quartz:0.1.3")
    and (resource("Job"; "seed-agent-knowledge-quartz-v1").spec.template.spec.containers[0]
      .securityContext.allowPrivilegeEscalation == false)
    and (resource("Job"; "seed-agent-knowledge-quartz-v1").spec.template.spec.containers[0]
      .securityContext.readOnlyRootFilesystem == true)
    and (resource("Job"; "seed-agent-knowledge-quartz-v1").spec.template.spec.containers[0]
      .securityContext.capabilities.drop == ["ALL"])
    and (resource("NetworkPolicy"; "deny-quartz-seed-ingress").spec.policyTypes
      == ["Ingress"])
    and (resource("NetworkPolicy"; "deny-quartz-seed-ingress").spec
      | has("ingress") | not)
' "$setup_json" >/dev/null

jq -e '
  .nodes[0].kubeadmConfigPatches as $patches
  | any($patches[]; contains("podPidsLimit: 512"))
    and any($patches[];
      contains("apiVersion: kubeadm.k8s.io/v1beta3")
      and contains("node-labels: \"agent-knowledge.io/pod-pids-limit=512\""))
' "$kind_json" >/dev/null

grep -Fx '#!/opt/agent-knowledge-quartz/bin/busybox sh' \
  "$e2e_directory/build-site" >/dev/null
grep -F 'kubectl wait --for=create pod/agent-knowledge-0' \
  "$e2e_directory/run.sh" >/dev/null
grep -F 'service/agent-knowledge-ssh' "$e2e_directory/run.sh" >/dev/null
grep -F ':2222 >"$temporary_directory/port-forward.log"' \
  "$e2e_directory/run.sh" >/dev/null
grep -F 'configure_client_ssh' "$e2e_directory/run.sh" >/dev/null
grep -F 'client_package=$(build_output .#kubernetes-e2e-client)' \
  "$e2e_directory/run.sh" >/dev/null
grep -F ' -F %q "$@"' "$e2e_directory/run.sh" >/dev/null
test "$(grep -c '@sha256:' "$e2e_directory/deploy-csi.sh")" -eq 8
if grep -Eq 'curl|wget|https?://' "$e2e_directory/deploy-csi.sh"; then
  echo "CSI deployment must not download runtime resources" >&2
  exit 1
fi
