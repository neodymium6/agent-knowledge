#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "usage: $0 QUARTZ_ROOT" >&2
  exit 2
fi

script_directory=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
content_directory="$script_directory/content"
quartz_root=$(realpath -- "$1")
quartz_program="$quartz_root/quartz/bootstrap-cli.mjs"

test -x "$quartz_program"
test -f "$quartz_root/package-lock.json"
test -d "$quartz_root/node_modules"

temporary_directory=$(mktemp -d "${TMPDIR:-/tmp}/agent-knowledge-quartz-e2e.XXXXXX")
trap 'rm -rf -- "$temporary_directory"' EXIT
output_directory="$temporary_directory/public"
mkdir "$output_directory"

(
  cd "$quartz_root"
  "$quartz_program" build -d "$content_directory" -o "$output_directory"
)

test -s "$output_directory/index.html"
grep -F 'Fictional accelerator report' "$output_directory/index.html" >/dev/null
grep -F 'memory-usage.svg' "$output_directory/index.html" >/dev/null
grep -F 'results.csv' "$output_directory/index.html" >/dev/null
grep -F 'href="./profiler-report"' "$output_directory/index.html" >/dev/null

for attachment in memory-usage.svg results.csv; do
  cmp "$content_directory/$attachment" "$output_directory/$attachment"
done
cmp "$content_directory/profiler-report.html" "$output_directory/profiler-report"
