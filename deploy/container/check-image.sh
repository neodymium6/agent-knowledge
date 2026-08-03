#!/usr/bin/env bash

set -euo pipefail

if [[ $# -ne 13 ]]; then
  echo "usage: $0 <image-archive> <architecture> <entrypoint> <version> <passwd> <group> <image-name> <user> <namespace> <action> <working-directory> <title> <ca-bundle-or-dash>" >&2
  exit 2
fi

image_archive=$1
expected_architecture=$2
expected_entrypoint=$3
expected_version=$4
expected_passwd=$5
expected_group=$6
expected_image_name=$7
expected_user=$8
expected_namespace=$9
expected_action=${10}
expected_working_directory=${11}
expected_title=${12}
expected_ca_bundle=${13}

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
  --arg user "$expected_user" \
  --arg namespace "$expected_namespace" \
  --arg action "$expected_action" \
  --arg working_directory "$expected_working_directory" \
  --arg title "$expected_title" \
  --arg ca_bundle "$expected_ca_bundle" \
  '
    .created == "1970-01-01T00:00:01+00:00" and
    .architecture == $architecture and
    .os == "linux" and
    .config.User == $user and
    .config.WorkingDir == $working_directory and
    .config.Entrypoint == (
      if $namespace == "openssh-gateway" then
        [$entrypoint, "-D", "-e", "-f", "/etc/agent-knowledge/sshd_config"]
      elif $action == "-" then
        [$entrypoint, $namespace]
      else
        [$entrypoint, $namespace, $action]
      end
    ) and
    .config.Cmd == null and
    .config.Env == (
      if $ca_bundle == "-" then
        ["HOME=" + $working_directory]
      else
        ["HOME=" + $working_directory, "SSL_CERT_FILE=" + $ca_bundle]
      end
    ) and
    .config.ExposedPorts == (
      if $namespace == "openssh-gateway" then
        {"2222/tcp": {}}
      else
        null
      end
    ) and
    .config.StopSignal == "SIGTERM" and
    .config.Labels == {
      "org.opencontainers.image.source": "https://github.com/neodymium6/agent-knowledge",
      "org.opencontainers.image.title": $title,
      "org.opencontainers.image.version": $version
    }
  ' <<<"$config" >/dev/null

layer_paths=$(jq -er '
  if length == 1 and
     .[0].RepoTags == [$image_name + ":" + $version] and
     (.[0].Layers | length > 0) and
     all(.[0].Layers[]; test("^[0-9a-f]{64}/layer\\.tar$"))
  then
    .[0].Layers[]
  else
    error("the archive manifest does not match the image contract")
  end
' --arg version "$expected_version" \
  --arg image_name "$expected_image_name" <<<"$manifest")

layer_contents="$work_directory/layer-contents"
while IFS= read -r layer_path; do
  if [[ ! -f $work_directory/$layer_path || -L $work_directory/$layer_path ]]; then
    echo "container image layer is not a regular file: ${layer_path}" >&2
    exit 1
  fi
  tar --absolute-names -tf "$work_directory/$layer_path" >>"$layer_contents"
done <<<"$layer_paths"

validate_normalized_path() {
  local target_path=$1

  if [[ -z $target_path ||
    $target_path == -* ||
    $target_path == /* ||
    $target_path == */ ||
    $target_path == *//* ]] ||
    grep -Eq '(^|/)\.\.?(/|$)' <<<"$target_path"; then
    echo "container image contains an unsafe or noncanonical path: ${target_path}" >&2
    return 1
  fi
}

normalized_layer_contents="$work_directory/normalized-layer-contents"
while IFS= read -r member; do
  if [[ $member == / || $member == ./ || $member == . ]]; then
    continue
  fi
  normalized=$member
  case $normalized in
    ./*)
      normalized=${normalized#./}
      ;;
    /*)
      normalized=${normalized#/}
      ;;
  esac
  if [[ $normalized == */ ]]; then
    normalized=${normalized%/}
  fi
  validate_normalized_path "$normalized"
  printf '%s\n' "$normalized" >>"$normalized_layer_contents"
done <"$layer_contents"

if grep -Eq '(^|/)\.wh\.' "$normalized_layer_contents"; then
  echo "container image must not contain whiteout entries" >&2
  exit 1
fi

entrypoint_path=${expected_entrypoint#/}
working_directory_path=${expected_working_directory#/}
for required_path in \
  etc/passwd \
  etc/group \
  "$working_directory_path" \
  "$entrypoint_path"; do
  if ! grep -Fxq "$required_path" "$normalized_layer_contents"; then
    echo "container image is missing ${required_path}" >&2
    exit 1
  fi
done
if [[ $expected_ca_bundle != - ]]; then
  ca_bundle_path=${expected_ca_bundle#/}
  if ! grep -Fxq "$ca_bundle_path" "$normalized_layer_contents"; then
    echo "container image is missing ${ca_bundle_path}" >&2
    exit 1
  fi
fi

for unique_path in \
  etc/passwd \
  etc/group \
  "$working_directory_path" \
  "$entrypoint_path"; do
  if [[ $(grep -Fxc "$unique_path" "$normalized_layer_contents") -ne 1 ]]; then
    echo "container image path must occur in exactly one layer: ${unique_path}" >&2
    exit 1
  fi
done
if [[ $expected_ca_bundle != - ]] &&
  [[ $(grep -Fxc "$ca_bundle_path" "$normalized_layer_contents") -ne 1 ]]; then
  echo "container image path must occur in exactly one layer: ${ca_bundle_path}" >&2
  exit 1
fi

validate_immutable_metadata() {
  local listing=$1
  local target_path=$2
  local mode owner remainder

  read -r mode owner remainder <<<"$listing"
  if [[ $owner != 0/0 ]]; then
    echo "container image path is not root-owned: ${target_path}" >&2
    return 1
  fi
  if [[ ${mode:0:1} != l && (${mode:5:1} == w || ${mode:8:1} == w) ]]; then
    echo "container image path is writable by a non-root identity: ${target_path}" >&2
    return 1
  fi
}

extract_image_file() {
  local target_path=$1
  local destination=$2
  local requirement=${3:-regular}
  local depth=${4:-0}
  local layer_path member normalized listing link_target

  if [[ $depth -gt 4 ]]; then
    echo "container image link chain is too deep: ${target_path}" >&2
    return 1
  fi

  validate_image_ancestors "$target_path" "$depth"

  while IFS= read -r layer_path; do
    while IFS= read -r member; do
      normalized=${member#./}
      normalized=${normalized#/}
      normalized=${normalized%/}
      if [[ $normalized == "$target_path" ]]; then
        listing=$(tar --absolute-names --numeric-owner --no-recursion -t -v \
          -f "$work_directory/$layer_path" -- "$member")
        validate_immutable_metadata "$listing" "$target_path"
        case ${listing:0:1} in
          -)
            if [[ ${listing:7:1} != r ]]; then
              echo "container image path is not readable by the configured identity: ${target_path}" >&2
              return 1
            fi
            if [[ $requirement == executable && ${listing:9:1} != x ]]; then
              echo "container image entrypoint is not executable: ${target_path}" >&2
              return 1
            fi
            tar --absolute-names -x -O \
              -f "$work_directory/$layer_path" -- "$member" >"$destination"
            return 0
            ;;
          l)
            link_target=${listing##* -> }
            if [[ $link_target != /* ]]; then
              echo "container image contains a relative link: ${target_path}" >&2
              return 1
            fi
            link_target=${link_target#/}
            validate_normalized_path "$link_target"
            extract_image_file \
              "$link_target" "$destination" "$requirement" "$((depth + 1))"
            return
            ;;
          *)
            echo "container image path does not resolve to a regular file: ${target_path}" >&2
            return 1
            ;;
        esac
      fi
    done < <(tar --absolute-names -tf "$work_directory/$layer_path")
  done <<<"$layer_paths"

  return 1
}

validate_image_directory() {
  local target_path=$1
  local requirement=${2:-optional}
  local depth=${3:-0}
  local layer_path member normalized listing link_target
  local found=false

  if [[ $depth -gt 4 ]]; then
    echo "container image directory link chain is too deep: ${target_path}" >&2
    return 1
  fi

  while IFS= read -r layer_path; do
    while IFS= read -r member; do
      normalized=${member#./}
      normalized=${normalized#/}
      normalized=${normalized%/}
      if [[ $normalized == "$target_path" ]]; then
        listing=$(tar --absolute-names --numeric-owner --no-recursion -t -v \
          -f "$work_directory/$layer_path" -- "$member")
        validate_immutable_metadata "$listing" "$target_path"
        case ${listing:0:1} in
          d)
            if [[ ${listing:9:1} != x ]]; then
              echo "container image directory is not traversable: ${target_path}" >&2
              return 1
            fi
            found=true
            ;;
          l)
            link_target=${listing##* -> }
            if [[ $link_target != /* ]]; then
              echo "container image contains a relative directory link: ${target_path}" >&2
              return 1
            fi
            link_target=${link_target#/}
            validate_normalized_path "$link_target"
            validate_image_ancestors "$link_target" "$((depth + 1))"
            validate_image_directory \
              "$link_target" required "$((depth + 1))"
            found=true
            ;;
          *)
            echo "container image path is not a directory: ${target_path}" >&2
            return 1
            ;;
        esac
      fi
    done < <(tar --absolute-names -tf "$work_directory/$layer_path")
  done <<<"$layer_paths"

  if [[ $found == false && $requirement == required ]]; then
    echo "container image is missing a required directory: ${target_path}" >&2
    return 1
  fi
}

validate_image_ancestors() {
  local target_path=$1
  local depth=${2:-0}
  local ancestor=${target_path%/*}

  while [[ $ancestor != "$target_path" && -n $ancestor ]]; do
    validate_image_directory "$ancestor" optional "$depth"
    target_path=$ancestor
    ancestor=${target_path%/*}
  done
}

validate_image_root() {
  local layer_path member listing

  while IFS= read -r layer_path; do
    while IFS= read -r member; do
      if [[ $member == / || $member == ./ || $member == . ]]; then
        listing=$(tar --absolute-names --numeric-owner --no-recursion -t -v \
          -f "$work_directory/$layer_path" -- "$member")
        validate_immutable_metadata "$listing" /
        if [[ ${listing:0:1} != d || ${listing:9:1} != x ]]; then
          echo "container image root is not a traversable directory" >&2
          return 1
        fi
      fi
    done < <(tar --absolute-names -tf "$work_directory/$layer_path")
  done <<<"$layer_paths"
}

validate_image_root
extract_image_file etc/passwd "$work_directory/passwd"
extract_image_file etc/group "$work_directory/group"
extract_image_file "$entrypoint_path" "$work_directory/entrypoint" executable
if [[ $expected_namespace == openssh-gateway ]]; then
  for executable_path in bin/agent-knowledge bin/agent-knowledge-ssh-shell; do
    if [[ $(grep -Fxc "$executable_path" "$normalized_layer_contents") -ne 1 ]]; then
      echo "OpenSSH Gateway image path must occur in exactly one layer: ${executable_path}" >&2
      exit 1
    fi
    extract_image_file \
      "$executable_path" "$work_directory/${executable_path##*/}" executable
    if [[ ! -s $work_directory/${executable_path##*/} ]]; then
      echo "OpenSSH Gateway image executable must not be empty: ${executable_path}" >&2
      exit 1
    fi
    executable_magic=$(od -An -tx1 -N4 "$work_directory/${executable_path##*/}")
    executable_magic=${executable_magic//[[:space:]]/}
    if [[ $executable_magic != 7f454c46 ]]; then
      echo "OpenSSH Gateway image executable must be an ELF binary: ${executable_path}" >&2
      exit 1
    fi
  done
fi
if [[ $expected_ca_bundle != - ]]; then
  extract_image_file "$ca_bundle_path" "$work_directory/ca-bundle"
fi
validate_image_ancestors "$working_directory_path"
validate_image_directory "$working_directory_path" required
if ! cmp -s "$expected_passwd" "$work_directory/passwd"; then
  echo "container passwd database does not match the packaged identities" >&2
  exit 1
fi
if ! cmp -s "$expected_group" "$work_directory/group"; then
  echo "container group database does not match the packaged identities" >&2
  exit 1
fi
if [[ ! -s $work_directory/entrypoint ]]; then
  echo "container entrypoint must not be empty" >&2
  exit 1
fi
if [[ $expected_ca_bundle != - && ! -s $work_directory/ca-bundle ]]; then
  echo "container CA bundle must not be empty" >&2
  exit 1
fi

if grep -Eq '^(bin/(ba)?sh|usr/bin/(ba)?sh)$' "$normalized_layer_contents"; then
  echo "container image must not expose a conventional shell path" >&2
  exit 1
fi
if [[ $expected_namespace == queue-ingress ]] &&
  grep -Eq '(^|/)(bin/git|bin/ssh|etc/ssl/certs/ca-bundle\.crt)$' \
    "$normalized_layer_contents"; then
  echo "queue ingress image contains a Worker-only executable or CA bundle" >&2
  exit 1
fi
if [[ $expected_namespace == gateway ]]; then
  if ! grep -Eq '(^|/)bin/git$' "$normalized_layer_contents"; then
    echo "Gateway image is missing the read-only Git executable" >&2
    exit 1
  fi
  if grep -Eq '(^|/)(bin/ssh|etc/ssl/certs/ca-bundle\.crt)$' \
    "$normalized_layer_contents"; then
    echo "Gateway image contains a Worker-only SSH executable or CA bundle" >&2
    exit 1
  fi
fi
if [[ $expected_namespace == openssh-gateway ]]; then
  for required_executable in bin/sshd bin/agent-knowledge bin/agent-knowledge-ssh-shell; do
    if ! grep -Fxq "$required_executable" "$normalized_layer_contents"; then
      echo "OpenSSH Gateway image is missing ${required_executable}" >&2
      exit 1
    fi
  done
  if ! grep -Eq '(^|/)bin/git$' "$normalized_layer_contents"; then
    echo "OpenSSH Gateway image is missing the read-only Git executable" >&2
    exit 1
  fi
  if grep -Eq '(^|/)(etc/ssl/certs/ca-bundle\.crt|authorized_keys|ssh_host_[^/]*)$|^etc/agent-knowledge($|/)' \
    "$normalized_layer_contents"; then
    echo "OpenSSH Gateway image contains deployment configuration, key material, or a CA bundle" >&2
    exit 1
  fi
fi
