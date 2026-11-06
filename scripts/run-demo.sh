#!/usr/bin/env bash
# Starts the server plus two demo workers against a fresh local database:
# one healthy (fail-rate 0) and one flaky (fail-rate 0.3, the default),
# enqueues a batch of jobs, and leaves everything running so you can watch
# retries/DLQ behavior live and poke at the HTTP API yourself. Ctrl-C to
# stop everything.

set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p data

pids=()
cleanup() {
  echo
  echo "stopping..."
  kill "${pids[@]}" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

./bin/idemq-server --addr=:8080 --db=data/idemq.db --reap-interval=1s &
pids+=($!)
sleep 0.5

echo "enqueuing 10 sample jobs onto queue 'emails'..."
for i in $(seq 1 10); do
  curl -s -X POST localhost:8080/v1/queues/emails/enqueue \
    -d "{\"payload\":{\"to\":\"user$i@example.com\",\"subject\":\"Welcome #$i\"},\"max_attempts\":4}" \
    > /dev/null
done

./bin/idemq-worker-demo --addr=http://localhost:8080 --worker-id=worker-a --fail-rate=0.3 &
pids+=($!)
./bin/idemq-worker-demo --addr=http://localhost:8080 --worker-id=worker-b --fail-rate=0.3 &
pids+=($!)

echo "running. try:"
echo "  curl localhost:8080/v1/queues/emails/stats"
echo "  curl localhost:8080/v1/queues/emails/dlq"
echo "(Ctrl-C to stop)"
wait
