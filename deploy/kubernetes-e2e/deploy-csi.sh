#!/usr/bin/env bash
set -euo pipefail

: "${AGENT_KNOWLEDGE_CSI_HOSTPATH_SOURCE:?run this test through the Nix development shell}"
: "${AGENT_KNOWLEDGE_CSI_ATTACHER_RBAC:?run this test through the Nix development shell}"
: "${AGENT_KNOWLEDGE_CSI_EXTERNAL_HEALTH_MONITOR_RBAC:?run this test through the Nix development shell}"
: "${AGENT_KNOWLEDGE_CSI_PROVISIONER_RBAC:?run this test through the Nix development shell}"
: "${AGENT_KNOWLEDGE_CSI_RESIZER_RBAC:?run this test through the Nix development shell}"
: "${AGENT_KNOWLEDGE_CSI_SNAPSHOTTER_RBAC:?run this test through the Nix development shell}"

readonly HOSTPATH_DIRECTORY
HOSTPATH_DIRECTORY="$AGENT_KNOWLEDGE_CSI_HOSTPATH_SOURCE/deploy/kubernetes-1.35/hostpath"

for rbac in \
  "$AGENT_KNOWLEDGE_CSI_PROVISIONER_RBAC" \
  "$AGENT_KNOWLEDGE_CSI_ATTACHER_RBAC" \
  "$AGENT_KNOWLEDGE_CSI_SNAPSHOTTER_RBAC" \
  "$AGENT_KNOWLEDGE_CSI_RESIZER_RBAC" \
  "$AGENT_KNOWLEDGE_CSI_EXTERNAL_HEALTH_MONITOR_RBAC"; do
  kubectl apply -f "$rbac"
done

pin_images() {
  sed \
    -e 's#registry.k8s.io/sig-storage/hostpathplugin:v1.17.1#registry.k8s.io/sig-storage/hostpathplugin:v1.17.1@sha256:ec6ded2430d4c5c6251e8d3a6ca55c675d6bd1b1ee8c012eab13aaa81be9c967#g' \
    -e 's#registry.k8s.io/sig-storage/csi-external-health-monitor-controller:v0.18.0#registry.k8s.io/sig-storage/csi-external-health-monitor-controller:v0.18.0@sha256:430c1f2267152ce6e4547ebbad225a2b399d03e5894f2f39ba233a38e4750a47#g' \
    -e 's#registry.k8s.io/sig-storage/csi-node-driver-registrar:v2.17.0#registry.k8s.io/sig-storage/csi-node-driver-registrar:v2.17.0@sha256:f9de845b170155199f2a2a3f9531cf13d78e31235e9db6b6582a8b0db0a50dad#g' \
    -e 's#registry.k8s.io/sig-storage/livenessprobe:v2.19.0#registry.k8s.io/sig-storage/livenessprobe:v2.19.0@sha256:06da0d5b8908072f2e4522692aee8dc119fba7247a9658497e1153992cd777e9#g' \
    -e 's#registry.k8s.io/sig-storage/csi-attacher:v4.12.0#registry.k8s.io/sig-storage/csi-attacher:v4.12.0@sha256:b9dc9a714a484ccdeeb6f86d88d4db9b7a5ecfc5a55da6db3a60bb3fa33c278a#g' \
    -e 's#registry.k8s.io/sig-storage/csi-provisioner:v6.3.0#registry.k8s.io/sig-storage/csi-provisioner:v6.3.0@sha256:a4b0b1a37605b7b04a293e136edf7006ec1786a8eb3f4e5a945f81d667dcc371#g' \
    -e 's#registry.k8s.io/sig-storage/csi-resizer:v2.2.1#registry.k8s.io/sig-storage/csi-resizer:v2.2.1@sha256:ea1d25e23479000c7e8eeb92d827df66258df4e482ca054c5e7ce3fc0f5c41a5#g' \
    -e 's#registry.k8s.io/sig-storage/csi-snapshotter:v8.6.0#registry.k8s.io/sig-storage/csi-snapshotter:v8.6.0@sha256:42af0929bcd60a43499825c078a60ff0534e08af8fbeb283aa391be40feb9f3e#g'
}

for manifest in \
  csi-hostpath-driverinfo.yaml \
  csi-hostpath-plugin.yaml \
  csi-hostpath-snapshotclass.yaml; do
  pin_images <"$HOSTPATH_DIRECTORY/$manifest" | kubectl apply -f -
done

kubectl rollout status statefulset/csi-hostpathplugin --timeout=300s
