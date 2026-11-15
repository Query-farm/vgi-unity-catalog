#!/usr/bin/env bash
# Start (or stop) a disposable Unity Catalog OSS server with authorization enabled,
# seeded by seed_uc.py. Used only to (re)record test/fixtures/uc_recording.json.
#
#   test/fixtures/uc_server.sh start   # prints nothing; server on http://localhost:${UC_PORT:-18080}
#   test/fixtures/uc_server.sh token   # the container's local admin token (never commit it)
#   test/fixtures/uc_server.sh stop
set -euo pipefail
NAME=vgi-unity-catalog-uc
IMAGE=unitycatalog/unitycatalog:v0.7.0
PORT=${UC_PORT:-18080}
HERE=$(cd "$(dirname "$0")" && pwd)
case "${1:-}" in
  start)
    docker rm -f "$NAME" >/dev/null 2>&1 || true
    docker create --name "$NAME" -m 1500m -p "$PORT:8080" "$IMAGE" >/dev/null
    docker start "$NAME" >/dev/null
    docker exec "$NAME" sed -i 's/^server.authorization=disable/server.authorization=enable/' etc/conf/server.properties
    printf 'storage-root.tables=file:///home/unitycatalog/etc/data/managed\nstorage-root.models=file:///home/unitycatalog/etc/data/managed\n' \
      | docker exec -i "$NAME" sh -c 'cat >> etc/conf/server.properties'
    docker restart "$NAME" >/dev/null
    for _ in $(seq 1 90); do
      code=$(curl -s -o /dev/null -w '%{http_code}' "http://localhost:$PORT/api/2.1/unity-catalog/catalogs" || true)
      [ "$code" = 401 ] && break
      sleep 1
    done
    python3 "$HERE/seed_uc.py" "http://localhost:$PORT" "$(docker exec "$NAME" cat etc/conf/token.txt)"
    ;;
  token) docker exec "$NAME" cat etc/conf/token.txt ;;
  stop) docker rm -f "$NAME" >/dev/null ;;
  *) echo "usage: $0 start|token|stop" >&2; exit 2 ;;
esac
