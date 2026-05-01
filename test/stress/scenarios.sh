#!/usr/bin/env bash
# Scripted stress scenarios for mqtt2db-go. Run against a fully-up
# Compose stack:
#
#   docker compose -f deploy/docker-compose.yml up -d
#   make migrate ARGS="up"
#   ./bin/mqtt2db-go --config=config.example.yaml &
#   ./test/stress/scenarios.sh steady
#
# Scenarios:
#   steady   constant 5K msg/s for 60s, 1000 devices
#   burst    20K msg/s for 5s, 5K msg/s for 5s, repeated
#   soak     2K msg/s for 10 minutes
#   pause    drive enough volume to trigger spill -> pause then recover
set -euo pipefail

LOADGEN=${LOADGEN:-go run ./test/loadgen}
BROKER=${BROKER:-tcp://localhost:1883}
TENANT=${TENANT:-acme}

run_loadgen() {
    local rate="$1" duration="$2" devices="$3" payload="${4:-128}" clients="${5:-8}"
    echo ">> loadgen rate=$rate duration=$duration devices=$devices payload=$payload clients=$clients"
    $LOADGEN \
        --broker="$BROKER" \
        --rate="$rate" \
        --duration="$duration" \
        --devices="$devices" \
        --payload="$payload" \
        --clients="$clients" \
        --tenant="$TENANT"
}

scenario_steady()  { run_loadgen 5000 60s 1000 128 8;  }
scenario_burst()   {
    for i in 1 2 3 4 5; do
        run_loadgen 20000 5s 1000 128 16
        run_loadgen  5000 5s 1000 128  8
    done
}
scenario_soak()    { run_loadgen 2000 10m 5000 256 8;  }
scenario_pause()   {
    # Aggressive rate against a single device to maximise dedup pressure
    # and let the buffer hit spill / pause.
    run_loadgen 30000 30s 50 64 16
}

case "${1:-}" in
    steady|burst|soak|pause)
        eval "scenario_$1"
        ;;
    *)
        echo "Usage: $0 {steady|burst|soak|pause}" >&2
        exit 2
        ;;
esac
