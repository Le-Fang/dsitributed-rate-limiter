#!/bin/sh
# Each topology shares a container network namespace. Advertised loopback
# ports equal published host ports, so both Redis peers and host clients can
# reach every advertised address on Linux and Docker Desktop.
set -eu
mode=${1:?cluster or sentinel}
root=/tmp/rate-limiter-topology
mkdir -p "$root"
pids=''
cleanup() {
  trap - EXIT INT TERM
  for pid in $pids; do kill "$pid" 2>/dev/null || true; done
  wait || true
}
trap cleanup EXIT INT TERM
start() {
  port=$1
  shift
  mkdir -p "$root/$port"
  redis-server --port "$port" --bind 0.0.0.0 --protected-mode no \
    --save '' --appendonly no --maxmemory-policy noeviction \
    --dir "$root/$port" "$@" >"$root/$port/server.log" 2>&1 &
  pids="$pids $!"
}
ready() {
  port=$1
  attempt=0
  until redis-cli -p "$port" ping | grep -q PONG; do
    attempt=$((attempt+1))
    if [ "$attempt" -ge 50 ]; then cat "$root/$port/server.log"; return 1; fi
    sleep 0.1
  done
}
case "$mode" in
  cluster)
    for port in 17000 17001 17002; do
      start "$port" --cluster-enabled yes --cluster-config-file nodes.conf \
        --cluster-node-timeout 1000 --cluster-announce-ip 127.0.0.1 \
        --cluster-announce-port "$port" --cluster-announce-bus-port "$((port+10000))"
      ready "$port"
    done
    redis-cli --cluster create 127.0.0.1:17000 127.0.0.1:17001 127.0.0.1:17002 \
      --cluster-replicas 0 --cluster-yes
    ;;
  sentinel)
    start 17379
    ready 17379
    start 17380 --replicaof 127.0.0.1 17379 --replica-announce-ip 127.0.0.1 --replica-announce-port 17380
    ready 17380
    cat >"$root/sentinel.conf" <<CONFIG
port 27379
bind 0.0.0.0
protected-mode no
dir $root
sentinel announce-ip 127.0.0.1
sentinel announce-port 27379
sentinel monitor limiter-test 127.0.0.1 17379 1
sentinel down-after-milliseconds limiter-test 1000
sentinel failover-timeout limiter-test 5000
sentinel parallel-syncs limiter-test 1
CONFIG
    redis-server "$root/sentinel.conf" --sentinel >"$root/sentinel.log" 2>&1 &
    pids="$pids $!"
    ready 27379
    ;;
  *) echo "unknown topology: $mode" >&2; exit 2 ;;
esac
touch "$root/ready"
wait
