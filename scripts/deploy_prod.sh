#!/usr/bin/env bash
set -euo pipefail

# Run on the production host from a checked-out release directory.
# Usage: scripts/deploy_prod.sh <git-sha> <image-tag>
SHA=${1:?git sha required}
IMAGE=${2:?image tag required}
ROOT=${PARSERS_ROOT:-/root/Parsers}
COMPOSE="$ROOT/docker-compose.yml"
TS=$(date +%Y%m%d_%H%M%S)
BACKUP="$ROOT/backups/$TS"
OLD_LISTING=$(docker inspect -f '{{.Image}}' parsers-listing-1)
OLD_DELIST=$(docker inspect -f '{{.Image}}' parsers-delist-1)

mkdir -p "$BACKUP"
cp -a "$COMPOSE" "$BACKUP/"
cp -a "$ROOT/state" "$BACKUP/state"
printf '%s\n' "$OLD_LISTING" > "$BACKUP/old-listing.digest"
printf '%s\n' "$OLD_DELIST" > "$BACKUP/old-delist.digest"
printf '%s\n' "$SHA" > "$BACKUP/release.sha"

rollback() {
  echo "ROLLBACK to $OLD_LISTING / $OLD_DELIST" >&2
  cp -a "$BACKUP/docker-compose.yml" "$COMPOSE"
  docker compose -f "$COMPOSE" up -d --no-deps listing delist
}
trap rollback ERR

python3 - "$COMPOSE" "$IMAGE" <<'PY'
from pathlib import Path
import sys
p=Path(sys.argv[1]); image=sys.argv[2]; s=p.read_text()
lines=s.splitlines()
service=None
for i,line in enumerate(lines):
    if line.startswith('  ') and line.strip().endswith(':') and not line.startswith('    '):
        service=line.strip()[:-1]
    if service in {'listing','delist'} and line.strip().startswith('image:'):
        lines[i]='    image: '+image
p.write_text('\n'.join(lines)+'\n')
PY

docker compose -f "$COMPOSE" config -q
for service in listing delist; do
  docker compose -f "$COMPOSE" up -d --no-deps "$service"
  container="parsers-${service}-1"
  for _ in $(seq 1 24); do
    status=$(docker inspect -f '{{.State.Status}}' "$container")
    health=$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$container")
    [[ "$status" == running ]] || false
    [[ "$health" == healthy ]] && break
    sleep 5
  done
  [[ $(docker inspect -f '{{.State.Health.Status}}' "$container") == healthy ]]
done

trap - ERR
printf '%s\n' "$TS" > "$ROOT/backups/LATEST"
echo "DEPLOY_OK sha=$SHA image=$IMAGE backup=$BACKUP"
