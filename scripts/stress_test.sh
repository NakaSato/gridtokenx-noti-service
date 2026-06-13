#!/usr/bin/env bash
#
# Stress-test the noti-service notification pipeline.
#
# Fires TOTAL SendNotification requests at the ConnectRPC endpoint with
# CONCURRENCY workers in flight. Each request carries a unique idempotency
# key so Redis dedup does not silently absorb the load. Reports throughput,
# error rate, and latency percentiles (p50/p90/p99).
#
# Usage:
#   ./scripts/stress_test.sh
#   TOTAL=5000 CONCURRENCY=100 ./scripts/stress_test.sh
#   GRPC_PORT=8090 CHANNEL=0 ./scripts/stress_test.sh
#
# Env:
#   GRPC_PORT    ConnectRPC port (default 8090 = PORT+10)
#   TOTAL        Total requests to send (default 1000)
#   CONCURRENCY  Workers in flight (default 50)
#   CHANNEL      Notification channel enum (default 0 = email)
#   TEMPLATE     Template id (default welcome.txt.tera)
#   USER_ID      Target user uuid (default all-ones test uuid)
#   RECIPIENT    Recipient (default stress@gridtokenx.com)

set -euo pipefail

GRPC_PORT=${GRPC_PORT:-8090}
TOTAL=${TOTAL:-1000}
CONCURRENCY=${CONCURRENCY:-50}
CHANNEL=${CHANNEL:-0}
TEMPLATE=${TEMPLATE:-welcome.txt.tera}
USER_ID=${USER_ID:-"00000000-0000-0000-0000-000000000001"}
RECIPIENT=${RECIPIENT:-"stress@gridtokenx.com"}

GRPC_URL="http://localhost:${GRPC_PORT}"
ENDPOINT="${GRPC_URL}/noti.NotificationService/SendNotification"

GREEN='\033[0;32m'; RED='\033[0;31m'; BLUE='\033[0;34m'; YELLOW='\033[0;33m'; NC='\033[0m'

command -v curl >/dev/null || { echo "curl required"; exit 1; }

echo "=========================================================="
echo "⚡ noti-service STRESS TEST"
echo "Endpoint:    $ENDPOINT"
echo "Total reqs:  $TOTAL"
echo "Concurrency: $CONCURRENCY"
echo "Channel:     $CHANNEL   Template: $TEMPLATE"
echo "=========================================================="

# Preflight: confirm server is up before flooding it.
if ! curl -s -o /dev/null --max-time 3 "http://localhost:$((GRPC_PORT - 10))/health"; then
    echo -e "${YELLOW}⚠ health check on PORT $((GRPC_PORT - 10)) failed — is the service running?${NC}"
fi

RUN_DIR=$(mktemp -d)
trap 'rm -rf "$RUN_DIR"' EXIT

# One request. Writes "<http_status> <time_total_seconds>" to a per-request file.
fire() {
    local i=$1
    local key="stress-${RUN_PID}-${i}"
    local body
    body=$(printf '{"user_id":"%s","channel":%s,"recipient":"%s","template_id":"%s","variables_json":"{\\"name\\":\\"Stress\\"}","idempotency_key":"%s"}' \
        "$USER_ID" "$CHANNEL" "$RECIPIENT" "$TEMPLATE" "$key")
    curl -s -o /dev/null -w '%{http_code} %{time_total}\n' \
        --max-time 30 \
        -H 'Content-Type: application/json' \
        -d "$body" \
        "$ENDPOINT" > "${RUN_DIR}/r_${i}" 2>/dev/null || echo "000 30" > "${RUN_DIR}/r_${i}"
}
export -f fire
export RUN_DIR ENDPOINT USER_ID CHANNEL RECIPIENT TEMPLATE
export RUN_PID=$$

# Sub-second wall clock. `date +%N` is GNU-only (BSD/macOS date emits a literal
# "N"), so use perl's Time::HiRes — present on every stock macOS.
now() { perl -MTime::HiRes=time -e 'printf "%.6f\n", time'; }

echo -e "${BLUE}▶ firing $TOTAL requests, $CONCURRENCY in flight...${NC}"
START=$(now)

# Throttled fan-out: fire CONCURRENCY at a time, drain, repeat.
# (Batch drain instead of `wait -n` so this runs on macOS bash 3.2.)
i=1
while ((i <= TOTAL)); do
    batch_end=$((i + CONCURRENCY - 1))
    ((batch_end > TOTAL)) && batch_end=$TOTAL
    for ((j = i; j <= batch_end; j++)); do
        fire "$j" &
    done
    wait
    i=$((batch_end + 1))
done

END=$(now)
ELAPSED=$(awk -v a="$START" -v b="$END" 'BEGIN{print b - a}')

# Aggregate results. `find` (not a `r_*` glob) so zero result files yields an
# empty file instead of a literal unmatched glob that aborts under `set -e`.
find "${RUN_DIR}" -maxdepth 1 -name 'r_*' -exec cat {} + > "${RUN_DIR}/all" 2>/dev/null || true
TOTAL_DONE=$(wc -l < "${RUN_DIR}/all" | tr -d ' ')
OK=$(awk '$1==200{c++} END{print c+0}' "${RUN_DIR}/all")
FAIL=$((TOTAL_DONE - OK))

echo ""
echo "=========================================================="
echo "RESULTS"
echo "=========================================================="
printf "elapsed:      %.2fs\n" "$ELAPSED"
awk -v e="$ELAPSED" -v t="$TOTAL_DONE" \
    'BEGIN{ if (e > 0) printf "throughput:   %.1f req/s\n", t/e; else print "throughput:   n/a (elapsed=0)" }'
echo -e "ok (200):     ${GREEN}${OK}${NC}"
if ((FAIL > 0)); then
    echo -e "failed:       ${RED}${FAIL}${NC}"
else
    echo -e "failed:       ${FAIL}"
fi

# Status-code breakdown.
echo "status codes:"
awk '{c[$1]++} END{for(s in c) printf "  %s: %d\n", s, c[s]}' "${RUN_DIR}/all" | sort

# Latency percentiles (seconds → ms).
awk '{print $2}' "${RUN_DIR}/all" | sort -n > "${RUN_DIR}/lat"
N=$(wc -l < "${RUN_DIR}/lat" | tr -d ' ')
pct() {
    local p=$1
    local idx
    # Nearest-rank: idx = ceil(N * p / 100), clamped to [1, N]. Plain int()
    # floors and understates the tail when N*p isn't a multiple of 100.
    idx=$(awk -v n="$N" -v p="$p" 'BEGIN{i=int((n*p + 99)/100); if(i<1)i=1; if(i>n)i=n; print i}')
    awk -v i="$idx" 'NR==i{printf "%.0f", $1*1000}' "${RUN_DIR}/lat"
}
echo "latency (ms):"
if ((N == 0)); then
    echo "  no data (0 samples)"
else
    echo "  p50: $(pct 50)   p90: $(pct 90)   p99: $(pct 99)   max: $(awk 'END{printf "%.0f", $1*1000}' "${RUN_DIR}/lat")"
fi

echo "=========================================================="
if ((TOTAL_DONE == 0)); then
    echo -e "${RED}✘ no responses recorded — service unreachable?${NC}"
    exit 1
fi
if ((FAIL > 0)); then
    echo -e "${RED}✘ completed with ${FAIL} failures${NC}"
    exit 1
fi
echo -e "${GREEN}✓ all ${OK} requests succeeded${NC}"
