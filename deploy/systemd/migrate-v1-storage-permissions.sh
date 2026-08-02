#!/usr/bin/env bash
set -euo pipefail

if [[ $(id -u) -ne 0 || $# -lt 1 || $# -gt 4 || $1 != /* || $1 == / ]]; then
  echo "usage: sudo $0 <absolute-storage-root> [queue-owner queue-group gateway-group]" >&2
  exit 2
fi

storage_root=${1%/}
queue_owner=${2:-agent-knowledge-queue}
queue_group=${3:-agent-knowledge-queue}
gateway_group=${4:-agent-knowledge-gateway}
queue_root=$storage_root/queue
repository_root=$storage_root/repository
content_root=$storage_root/content

for root in "$storage_root" "$queue_root" "$repository_root" "$content_root"; do
  if [[ ! -d $root || -L $root || $(readlink -f -- "$root") != "$root" ]]; then
    echo "storage migration requires an existing canonical directory: $root" >&2
    exit 1
  fi
done

if [[ ! -f $queue_root/.locks/queue.lock || ! -f $queue_root/.locks/repository-writer.lock ]]; then
  echo "storage migration requires an initialized queue" >&2
  exit 1
fi

exec {queue_lock}<>"$queue_root/.locks/queue.lock"
exec {worker_lock}<>"$queue_root/.locks/repository-writer.lock"
if ! flock -n "$queue_lock" || ! flock -n "$worker_lock"; then
  echo "stop every Gateway and Worker process before migrating storage" >&2
  exit 1
fi

shopt -s nullglob dotglob
incoming_entries=("$queue_root/incoming"/*)
if ((${#incoming_entries[@]} != 0)); then
  echo "queue/incoming must be empty before migrating storage" >&2
  exit 1
fi
shopt -u nullglob dotglob

for root in "$queue_root" "$repository_root" "$content_root"; do
  unsafe=$(find "$root" -xdev \( -type l -o -type f -links +1 \) -print -quit)
  if [[ -n $unsafe ]]; then
    echo "storage migration refuses a symbolic link or hard-linked file: $unsafe" >&2
    exit 1
  fi
done

find "$queue_root" -xdev -exec chgrp --no-dereference "$queue_group" {} +
find "$queue_root" -xdev -type d -exec chmod g=rx,g+s,o= {} +
find "$queue_root" -xdev -type f -exec chmod g=r,o= {} +

queue_directories=(
  "$queue_root"
  "$queue_root/.locks"
  "$queue_root/incoming"
  "$queue_root/quarantine"
  "$queue_root/worker-tmp"
  "$queue_root/pending"
  "$queue_root/processing"
  "$queue_root/completed"
  "$queue_root/failed"
)
chown "$queue_owner:$queue_group" "${queue_directories[@]}"
chmod 2770 "${queue_directories[@]}"
chown "$queue_owner:$queue_group" \
  "$queue_root/.locks/queue.lock" \
  "$queue_root/.locks/repository-writer.lock"
chmod 0660 \
  "$queue_root/.locks/queue.lock" \
  "$queue_root/.locks/repository-writer.lock"

for root in "$repository_root" "$content_root"; do
  find "$root" -xdev -exec chgrp --no-dereference "$gateway_group" {} +
  find "$root" -xdev -type d -exec chmod g=rx,g+s,o= {} +
  find "$root" -xdev -type f -exec chmod g=r,o= {} +
done

sync -f "$queue_root"
sync -f "$repository_root"
sync -f "$content_root"
