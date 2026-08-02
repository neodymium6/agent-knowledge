#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 4 ]]; then
  echo "usage: $0 <image-archive> <architecture> <entrypoint> <version>" >&2
  exit 2
fi

image_archive=$1
expected_architecture=$2
expected_entrypoint=$3
expected_version=$4

archive_members=$(tar -tzf "$image_archive")
if grep -Eqv '^(manifest\.json|[0-9a-f]{64}\.json|[0-9a-f]{64}/layer\.tar)$' \
  <<<"$archive_members"; then
  echo "container archive contains an unsafe or unsupported member path" >&2
  exit 1
fi
if [[ -n $(sort <<<"$archive_members" | uniq -d) ]]; then
  echo "container archive contains duplicate member paths" >&2
  exit 1
fi

work_directory=$(mktemp -d "${TMPDIR:-/tmp}/agent-knowledge-image.XXXXXX")
trap 'rm -rf -- "$work_directory"' EXIT
tar --no-same-owner --no-same-permissions -xzf "$image_archive" -C "$work_directory"

if [[ ! -f $work_directory/manifest.json || -L $work_directory/manifest.json ]]; then
  echo "container archive manifest is not a regular file" >&2
  exit 1
fi

manifest=$(<"$work_directory/manifest.json")
config_path=$(jq -er '
  if length == 1 and (.[0].Config | test("^[0-9a-f]{64}\\.json$")) then
    .[0].Config
  else
    error("the archive must contain exactly one image configuration")
  end
' <<<"$manifest")
if [[ ! -f $work_directory/$config_path || -L $work_directory/$config_path ]]; then
  echo "container image configuration is not a regular file" >&2
  exit 1
fi
config=$(<"$work_directory/$config_path")

jq -e \
  --arg architecture "$expected_architecture" \
  --arg entrypoint "$expected_entrypoint" \
  --arg version "$expected_version" \
  '
    .created == "1970-01-01T00:00:01+00:00" and
    .architecture == $architecture and
    .os == "linux" and
    .config.User == "10003:10003" and
    .config.WorkingDir == "/var/lib/agent-knowledge" and
    .config.Entrypoint == [$entrypoint] and
    .config.Cmd == null and
    .config.Env == ["HOME=/var/lib/agent-knowledge"] and
    .config.StopSignal == "SIGTERM" and
    .config.Labels == {
      "org.opencontainers.image.source": "https://github.com/neodymium6/agent-knowledge",
      "org.opencontainers.image.title": "Agent Knowledge",
      "org.opencontainers.image.version": $version
    }
  ' <<<"$config" >/dev/null

layer_paths=$(jq -er '
  if length == 1 and
     .[0].RepoTags == ["agent-knowledge:" + $version] and
     (.[0].Layers | length > 0) and
     all(.[0].Layers[]; test("^[0-9a-f]{64}/layer\\.tar$"))
  then
    .[0].Layers[]
  else
    error("the archive manifest does not match the image contract")
  end
' --arg version "$expected_version" <<<"$manifest")

layer_contents="$work_directory/layer-contents"
while IFS= read -r layer_path; do
  if [[ ! -f $work_directory/$layer_path || -L $work_directory/$layer_path ]]; then
    echo "container image layer is not a regular file: ${layer_path}" >&2
    exit 1
  fi
  tar --absolute-names -tf "$work_directory/$layer_path" >>"$layer_contents"
done <<<"$layer_paths"

entrypoint_path=${expected_entrypoint#/}
for required_path in etc/passwd etc/group var/lib/agent-knowledge "$entrypoint_path"; do
  if ! grep -Eq "^(\./|/)?${required_path}/?$" "$layer_contents"; then
    echo "container image is missing ${required_path}" >&2
    exit 1
  fi
done

if grep -Eq '^(\./|/)?(bin/(ba)?sh|usr/bin/(ba)?sh)$' "$layer_contents"; then
  echo "container image must not expose a conventional shell path" >&2
  exit 1
fi
