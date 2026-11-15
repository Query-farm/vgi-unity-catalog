#!/usr/bin/env bash
# managed-by: vgi-starters (edit upstream in shared/, then run tools/render.py)
#
# Run test/sql/*.test through DuckDB (the haybarn-unittest sqllogictest runner)
# against this worker, over one transport.
#
#   WORKER=/abs/path/to/worker-executable TRANSPORT=stdio ci/run-integration.sh
#
# Required environment:
#   WORKER            absolute path to an executable that serves the worker:
#                     no arguments = stdio; `--http --port 0` = HTTP, printing
#                     `PORT:<n>` on stdout once listening.
# Optional:
#   TRANSPORT         stdio | launch | http                        (default: stdio)
#   HAYBARN_UNITTEST  path to haybarn-unittest   (default: the one on PATH)
#   PYTHON            command for the mock server, may contain spaces (default: python3)
#   MIN_EXECUTED      fail if fewer test cases ran (default: 1)
#   EXPECTED_SKIP_REASONS  newline-separated skip reasons that are allowed,
#                     e.g. "require-env UNITY_CATALOG_LIVE". Any other skip fails.
#   RECIPES_WORKER    command that runs a recipe by name (`$RECIPES_WORKER <recipe>`);
#                     when set, examples/test/*.test run too (stdio/launch only)
#   KEEP_STAGE=1      keep the staged test tree for debugging
#
# Transports:
#   stdio   DuckDB spawns the worker and talks over stdin/stdout (the default).
#   launch  DuckDB starts the worker once on an AF_UNIX socket and pools
#           connections (`LOCATION 'launch:/path'`); best for slow-starting
#           runtimes such as the JVM.
#   http    the worker runs as an HTTP server (`--http`), as in Docker deploys.
#
# Why the extra checks: the sqllogictest runner turns some errors into silent
# SKIPs (anything mentioning "HTTP" without --test-config) and exits 0 when no
# test matched. Both would make a broken lane look green, so this script passes
# a config that disables error-skipping and fails on any unexpected skip.
set -euo pipefail

: "${WORKER:?absolute path to the worker executable}"
TRANSPORT="${TRANSPORT:-stdio}"
HAYBARN_UNITTEST="${HAYBARN_UNITTEST:-$(command -v haybarn-unittest || true)}"
PYTHON="${PYTHON:-python3}"
MIN_EXECUTED="${MIN_EXECUTED:-1}"
[ -n "$HAYBARN_UNITTEST" ] || { echo "::error::haybarn-unittest not found (make tools)"; exit 1; }

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
STAGE="$(mktemp -d)"
PIDS=()
cleanup() {
  for p in ${PIDS[@]+"${PIDS[@]}"}; do kill "$p" 2>/dev/null || true; done
  [ "${KEEP_STAGE:-0}" = "1" ] && echo "stage kept at $STAGE" || rm -rf "$STAGE"
}
trap cleanup EXIT

# start_server <log> <cmd...> — run a server that prints PORT:<n>; sets PORT.
PORT=""
start_server() {
  local log="$1"; shift
  # Create the log before backgrounding: the child opens its redirect only
  # once it is scheduled, and the poll below would otherwise read a missing
  # file, which under set -e -o pipefail kills the whole run.
  : > "$log"
  "$@" >"$log" 2>&1 &
  local pid=$!
  PIDS+=("$pid")
  for _ in $(seq 1 120); do
    kill -0 "$pid" 2>/dev/null || { echo "::error::'$*' exited early:"; cat "$log"; return 1; }
    PORT="$(sed -n 's/^PORT:\([0-9][0-9]*\).*/\1/p' "$log" | head -1)"
    [ -n "$PORT" ] && return 0
    sleep 0.5
  done
  echo "::error::'$*' never printed PORT:<n>"; cat "$log"; return 1
}

# The strict Unity Catalog replay server (test/mock_uc): answers only exchanges recorded
# from a real open-source Unity Catalog v0.7.0 server (test/fixtures/uc_recording.json),
# requires 'Authorization: Bearer test-token', and fails anything unrecorded with HTTP 418.
#   UNITY_CATALOG_MOCK      open source; rate-limits the first sighting of every third request
#   UNITY_CATALOG_MOCK_DBX  Databricks flavour: also serves test/fixtures/databricks_extras.json
# UC_RECORD_UPSTREAM=<url> UC_RECORD_TOKEN=<token> turns both into recording proxies in front of a
# real server (see test/fixtures/uc_server.sh), rewriting test/fixtures/uc_recording.json.
if [ -n "${UC_RECORD_UPSTREAM:-}" ]; then
  export MOCK_RECORD_UPSTREAM="$UC_RECORD_UPSTREAM" MOCK_RECORD_TOKEN="${UC_RECORD_TOKEN:-}"
  export MOCK_RECORD_OUT="$REPO/test/fixtures/uc_recording.json"
  start_server "$STAGE/mock.log" $PYTHON "$REPO/test/mock_uc/server.py"
else
  MOCK_FLAKY=1 start_server "$STAGE/mock.log" $PYTHON "$REPO/test/mock_uc/server.py"
fi
export UNITY_CATALOG_MOCK="http://127.0.0.1:$PORT"
MOCK_FLAVOR=databricks start_server "$STAGE/mock-dbx.log" $PYTHON "$REPO/test/mock_uc/server.py"
export UNITY_CATALOG_MOCK_DBX="http://127.0.0.1:$PORT"

HTTP=0
case "$TRANSPORT" in
  stdio)  export UNITY_CATALOG_WORKER="$WORKER" ;;
  launch) export UNITY_CATALOG_WORKER="launch:$WORKER" ;;
  http)
    HTTP=1
    start_server "$STAGE/worker.log" "$WORKER" --http --port 0
    export UNITY_CATALOG_WORKER="http://127.0.0.1:$PORT"
    ;;
  *) echo "::error::unknown TRANSPORT=$TRANSPORT (expected stdio|launch|http)"; exit 1 ;;
esac

# Stage the tests, rewriting `require vgi` into a signed INSTALL + LOAD (the
# standalone runner links no extensions) and loading httpfs for HTTP workers.
mkdir -p "$STAGE/test/sql"
TEST_FILES=("$REPO"/test/sql/*.test)
if [ -n "${RECIPES_WORKER:-}" ] && [ "$HTTP" = "0" ]; then
  TEST_FILES+=("$REPO"/examples/test/*.test)
fi
for f in "${TEST_FILES[@]}"; do
  awk -v http="$HTTP" '
    /^require[ \t]+vgi[ \t]*$/ {
      print "statement ok"; print "LOAD vgi;"; print ""
      if (http == 1) { print "statement ok"; print "LOAD httpfs;"; print "" }
      next
    }
    { print }' "$f" > "$STAGE/test/sql/$(basename "$f")"
done

# Install the extensions once, refreshing any stale cached copy of vgi.
cat > "$STAGE/test/_warm.test" <<'EOF'
statement ok
FORCE INSTALL vgi FROM community;

statement ok
INSTALL httpfs FROM core;

statement ok
INSTALL json FROM core;
EOF
"$HAYBARN_UNITTEST" --keep-home --test-dir "$STAGE" "test/_warm.test" >/dev/null 2>&1 \
  || echo "::warning::extension warm-up did not fully succeed"
rm -f "$STAGE/test/_warm.test"

cat > "$STAGE/no_error_skip.json" <<'EOF'
{"skip_error_messages": ["__vgi_unity_catalog_never_matches__"]}
EOF

echo "Running test/sql over $TRANSPORT (worker: $UNITY_CATALOG_WORKER)"
LOG="$STAGE/unittest.log"
rc=0
( cd "$STAGE" && "$HAYBARN_UNITTEST" --keep-home --test-dir "$STAGE" \
    --test-config "$STAGE/no_error_skip.json" "test/sql/*" ) 2>&1 | tee "$LOG" && rc=0 || rc="${PIPESTATUS[0]}"

if grep -q 'No test cases matched\|No tests ran' "$LOG"; then
  echo "::error::no test cases matched; the staging is wrong"; exit 1
fi
total="$(sed -n 's/^\[[0-9]*\/\([0-9]*\)\].*/\1/p' "$LOG" | tail -1)"
total="${total:-0}"
skips="$(awk '/^Skipped tests for the following reasons:/{b=1;next} b&&/^[[:space:]]*$/{b=0} b&&match($0,/: [0-9]+[[:space:]]*$/){print substr($0,1,RSTART-1) "\t" substr($0,RSTART+2)+0}' "$LOG")"
skipped=0
while IFS=$'\t' read -r reason n; do
  [ -n "$reason" ] || continue
  skipped=$((skipped + n))
  if ! printf '%s\n' "${EXPECTED_SKIP_REASONS:-}" | grep -qxF "$reason"; then
    echo "::error::unexpected skip: '$reason'. A test file silently stopped running;" \
         "fix the gate or add it to EXPECTED_SKIP_REASONS."
    rc=1
  fi
done <<< "$skips"
executed=$((total - skipped))
echo "executed $executed of $total test files over $TRANSPORT ($skipped skipped)"
if [ "$executed" -lt "$MIN_EXECUTED" ]; then
  echo "::error::only $executed test files ran (MIN_EXECUTED=$MIN_EXECUTED)"; rc=1
fi
if grep -h 'UNRECORDED' "$STAGE/mock.log" "$STAGE/mock-dbx.log"; then
  echo "::error::the worker sent requests the Unity Catalog recording does not contain (see above)"; rc=1
fi
if [ "$HTTP" = "1" ] && ! kill -0 "${PIDS[2]}" 2>/dev/null; then
  echo "::error::the HTTP worker died during the run:"; cat "$STAGE/worker.log"; rc=1
fi
exit "$rc"
