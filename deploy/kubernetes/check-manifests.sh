#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 1 ] || [ "$#" -gt 2 ]; then
  echo "usage: $0 <kustomize-directory> [kube-linter-config]" >&2
  exit 2
fi

manifest_directory=$1
lint_config=${2:-$manifest_directory/kube-linter.yaml}
temporary_directory=$(mktemp -d "${TMPDIR:-/tmp}/agent-knowledge-kubernetes.XXXXXX")
trap 'rm -rf -- "$temporary_directory"' EXIT
rendered_manifest=$temporary_directory/rendered.yaml
resources_json=$temporary_directory/resources.json

kustomize build --load-restrictor LoadRestrictionsRootOnly \
  "$manifest_directory" >"$rendered_manifest"
kube-linter lint --config "$lint_config" \
  "$rendered_manifest"
yq eval-all -o=json -I=0 '[.]' "$rendered_manifest" >"$resources_json"

jq -e '
  map(select(. != null)) as $resources
  | def resources($kind): $resources | map(select(.kind == $kind));
    def stateful_set: resources("StatefulSet")[0];
    def pod_spec: stateful_set.spec.template.spec;
    def containers: pod_spec.containers;
    def init_containers: pod_spec.initContainers;
    def container($name): containers | map(select(.name == $name))[0];
    def init_container($name): init_containers | map(select(.name == $name))[0];
    def volume($name): pod_spec.volumes | map(select(.name == $name))[0];
    def mount($container; $name):
      $container.volumeMounts | map(select(.name == $name))[0];
    ($resources | length == 6)
    and (resources("ServiceAccount") | length == 1)
    and (resources("ConfigMap") | length == 1)
    and (resources("ConfigMap")[0].immutable == true)
    and (resources("Service") | length == 2)
    and (resources("NetworkPolicy") | length == 1)
    and (resources("StatefulSet") | length == 1)
    and (resources("Secret") | length == 0)
    and (resources("NetworkPolicy")[0].spec.policyTypes == ["Ingress"])
    and (resources("NetworkPolicy")[0].spec.ingress[0].ports
      == [{"port": 2222, "protocol": "TCP"}])
    and (stateful_set.spec.replicas == 1)
    and (stateful_set.spec.serviceName == "agent-knowledge-headless")
    and (stateful_set.spec.updateStrategy.type == "RollingUpdate")
    and (stateful_set.spec.persistentVolumeClaimRetentionPolicy == {
      "whenDeleted": "Retain", "whenScaled": "Retain"
    })
    and (stateful_set.spec.selector.matchLabels
      == stateful_set.spec.template.metadata.labels)
    and (resources("ConfigMap")[0].metadata.name
      | startswith("agent-knowledge-config-"))
    and (volume("configuration").configMap.name
      == resources("ConfigMap")[0].metadata.name)
    and (stateful_set.spec.volumeClaimTemplates | length == 1)
    and (stateful_set.spec.volumeClaimTemplates[0].metadata.name == "knowledge")
    and (stateful_set.spec.volumeClaimTemplates[0].spec.accessModes
      == ["ReadWriteOncePod"])
    and (pod_spec.automountServiceAccountToken == false)
    and (pod_spec.serviceAccountName == "agent-knowledge")
    and (pod_spec.terminationGracePeriodSeconds >= 600)
    and (pod_spec.nodeSelector == {
      "agent-knowledge.io/pod-pids-limit": "512"
    })
    and (pod_spec.securityContext.seccompProfile.type == "RuntimeDefault")
    and (pod_spec.securityContext.supplementalGroupsPolicy == "Merge")
    and (pod_spec.securityContext | has("fsGroup") | not)
    and (pod_spec.securityContext | has("supplementalGroups") | not)
    and ((pod_spec.hostNetwork // false) == false)
    and ((pod_spec.hostPID // false) == false)
    and ((pod_spec.hostIPC // false) == false)
    and (init_containers | length == 4)
    and (containers | length == 3)
    and ([init_containers[], containers[]]
      | all(.[]; .securityContext.allowPrivilegeEscalation == false))
    and ([init_containers[], containers[]]
      | all(.[]; .securityContext.readOnlyRootFilesystem == true))
    and ([init_containers[], containers[]]
      | all(.[]; .securityContext.capabilities.drop == ["ALL"]))
    and ([init_containers[], containers[]]
      | map(select(.securityContext.runAsUser == 0) | .name) | sort
      == [
        "openssh-gateway",
        "prepare-ssh-directory",
        "stage-ssh-authorized-keys",
        "stage-ssh-host-key",
        "storage-bootstrap"
      ])
    and (init_container("prepare-ssh-directory").command == ["/bin/install"])
    and (init_container("prepare-ssh-directory").args
      == ["--directory", "--mode=0755", "/staged"])
    and (init_container("stage-ssh-host-key").command == ["/bin/install"])
    and (init_container("stage-ssh-host-key").args == [
      "--mode=0400",
      "/projected/ssh_host_ed25519_key",
      "/staged/ssh_host_ed25519_key"
    ])
    and (init_container("stage-ssh-authorized-keys").command == ["/bin/install"])
    and (init_container("stage-ssh-authorized-keys").args == [
      "--mode=0444",
      "/projected/authorized_keys",
      "/staged/authorized_keys"
    ])
    and (init_container("storage-bootstrap").securityContext.capabilities.add | sort
      == ["CHOWN", "DAC_OVERRIDE", "FOWNER", "FSETID"])
    and (container("openssh-gateway").securityContext.capabilities.add | sort
      == ["SETGID", "SETUID", "SYS_CHROOT"])
    and (container("worker").securityContext.runAsUser == 10003)
    and (container("worker").securityContext.runAsGroup == 10003)
    and (container("worker").securityContext.runAsNonRoot == true)
    and (container("queue-ingress").securityContext.runAsUser == 10002)
    and (container("queue-ingress").securityContext.runAsGroup == 10002)
    and (container("queue-ingress").securityContext.runAsNonRoot == true)
    and ([init_containers[], containers[]]
      | all(.[].volumeMounts[]?; has("subPathExpr") | not))
    and ([init_containers[], containers[]]
      | map({
          container: .name,
          mounts: [.volumeMounts[]? | select(has("subPath"))
            | {name, mountPath, subPath}]
        })
      | map(select(.mounts | length > 0))
      == [{
        "container": "openssh-gateway",
        "mounts": [{
          "name": "knowledge",
          "mountPath": "/var/lib/agent-knowledge/search-indexes",
          "subPath": "search-indexes"
        }]
      }])
    and (mount(init_container("storage-bootstrap"); "knowledge").readOnly != true)
    and (mount(container("worker"); "knowledge").readOnly != true)
    and (mount(container("queue-ingress"); "knowledge").readOnly != true)
    and (mount(container("openssh-gateway"); "knowledge").readOnly == true)
    and ([container("openssh-gateway").volumeMounts[]
      | select(.name == "knowledge")] | length == 2)
    and (container("openssh-gateway").volumeMounts
      | any(.[];
        .name == "knowledge"
        and .mountPath == "/var/lib/agent-knowledge/search-indexes"
        and .readOnly == false
        and .subPath == "search-indexes"))
    and (mount(container("worker"); "quartz").readOnly == true)
    and (volume("runtime") | has("emptyDir"))
    and (volume("sshd-runtime") | has("emptyDir"))
    and (volume("ssh-credentials").emptyDir.medium == "Memory")
    and (mount(container("openssh-gateway"); "ssh-credentials").readOnly == true)
    and (mount(container("openssh-gateway"); "ssh-credentials").mountPath
      == "/etc/agent-knowledge-ssh")
    and (container("openssh-gateway").volumeMounts
      | any(.[]; .name == "ssh-credentials-source") | not)
    and (volume("ssh-credentials-source").secret.secretName
      == "agent-knowledge-ssh-v1")
    and (volume("ssh-credentials-source").secret.items
      | map(select(.key == "ssh_host_ed25519_key"))[0].mode == 256)
    and (volume("ssh-credentials-source").secret.items
      | map(select(.key == "authorized_keys"))[0].mode == 292)
    and (volume("quartz").persistentVolumeClaim.claimName
      == "agent-knowledge-quartz-v1")
    and (volume("quartz").persistentVolumeClaim.readOnly == true)
    and (pod_spec.volumes | all(.[]; has("hostPath") | not))
    and (resources("ConfigMap")[0].data | keys | sort
      == ["gateway.yaml", "sshd_config", "worker.yaml"])
    and (resources("ConfigMap")[0].data.sshd_config
      | contains("HostKey /etc/agent-knowledge-ssh/ssh_host_ed25519_key"))
    and (resources("ConfigMap")[0].data.sshd_config
      | contains("AuthorizedKeysFile /etc/agent-knowledge-ssh/authorized_keys"))
' "$resources_json" >/dev/null
