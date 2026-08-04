#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 3 ]; then
  echo "usage: $0 <image-archive> <architecture> <version>" >&2
  exit 2
fi

image_archive=$1
expected_architecture=$2
expected_version=$3

archive_members=$(tar -tzf "$image_archive")
if grep -Eqv '^(manifest\.json|[0-9a-f]{64}\.json|[0-9a-f]{64}/layer\.tar)$' \
  <<<"$archive_members"; then
  echo "fixture image archive contains an unsupported member path" >&2
  exit 1
fi
if [ -n "$(sort <<<"$archive_members" | uniq -d)" ]; then
  echo "fixture image archive contains duplicate member paths" >&2
  exit 1
fi

temporary_directory=$(mktemp -d "${TMPDIR:-/tmp}/agent-knowledge-quartz-image.XXXXXX")
trap 'rm -rf -- "$temporary_directory"' EXIT
tar --no-same-owner --no-same-permissions -xzf "$image_archive" \
  -C "$temporary_directory"

manifest_file=$temporary_directory/manifest.json
test -f "$manifest_file" && test ! -L "$manifest_file"
config_path=$(jq -er \
  --arg image "agent-knowledge-kubernetes-e2e-quartz:$expected_version" '
    if length == 1
      and .[0].RepoTags == [$image]
      and (.[0].Config | test("^[0-9a-f]{64}\\.json$"))
      and (.[0].Layers | length > 0)
      and all(.[0].Layers[]; test("^[0-9a-f]{64}/layer\\.tar$"))
    then .[0].Config
    else error("fixture image manifest does not match its contract")
    end
  ' "$manifest_file")

config_file=$temporary_directory/$config_path
test -f "$config_file" && test ! -L "$config_file"
jq -e \
  --arg architecture "$expected_architecture" \
  --arg version "$expected_version" '
    .created == "1970-01-01T00:00:01+00:00"
    and .architecture == $architecture
    and .os == "linux"
    and .config.User == "0"
    and .config.WorkingDir == "/"
    and .config.Entrypoint == ["/bin/sh"]
    and .config.Labels == {
      "org.opencontainers.image.source": "https://github.com/neodymium6/agent-knowledge",
      "org.opencontainers.image.title": "Agent Knowledge Kubernetes E2E Quartz Fixture",
      "org.opencontainers.image.version": $version
    }
  ' "$config_file" >/dev/null

layer_members=$temporary_directory/layer-members
while IFS= read -r layer_path; do
  layer_file=$temporary_directory/$layer_path
  test -f "$layer_file" && test ! -L "$layer_file"
  tar --absolute-names -tf "$layer_file" \
    | sed -e 's#^\./##' -e 's#^/##' -e 's#/$##' >>"$layer_members"
done < <(jq -er '.[0].Layers[]' "$manifest_file")

for required_path in bin/busybox bin/sh fixture/build-site; do
  if ! grep -Fxq "$required_path" "$layer_members"; then
    echo "fixture image is missing $required_path" >&2
    exit 1
  fi
done
