#!/usr/bin/env bash
# Starts the broker image with no configuration and checks that it is a broker a client can
# use: healthy on a fresh named volume without running as root, one record produced and
# consumed over the Kafka wire protocol and one over the native protocol, and both still
# there after a restart on the same volume.
#
#   scripts/ci/image-smoke.sh
#
# Environment:
#   SURGEWAVE_BROKER_IMAGE  broker image under test, required
#   SURGEWAVE_CLI_IMAGE     image of the surgewave CLI (native protocol), required
#   KAFKA_IMAGE             image with the Apache Kafka command-line clients (Kafka wire
#                           protocol); default: Apache Kafka 4.1.0, pinned by digest
#   SURGEWAVE_DATA_MOUNT    host directory to mount as the data directory instead of a fresh
#                           named volume
#   COMPOSE_PROJECT_NAME    compose project (default: surgewave-image-smoke)
#
# The clients run in the broker's network namespace: a broker without configuration tells
# clients it is localhost, as it does to a client on the Docker host of `docker run -p 9092:9092`.
#
# Nothing here sleeps or polls: `docker compose up --wait` returns when the health check
# passes. Containers and the volume of the project are removed first and at the end.
set -euo pipefail

: "${SURGEWAVE_BROKER_IMAGE:?set SURGEWAVE_BROKER_IMAGE to the broker image under test}"
: "${SURGEWAVE_CLI_IMAGE:?set SURGEWAVE_CLI_IMAGE to the surgewave CLI image}"
KAFKA_IMAGE="${KAFKA_IMAGE:-apache/kafka@sha256:bff074a5d0051dbc0bbbcd25b045bb1fe84833ec0d3c7c965d1797dd289ec88f}"
export SURGEWAVE_BROKER_IMAGE
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-surgewave-image-smoke}"

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
compose_file="$root/scripts/ci/image-smoke.compose.yml"
bootstrap=localhost:9092
# Topics are named per run.
run_id="$(date +%s)"
kafka_topic="smoke-kafka-$run_id"
native_topic="smoke-native-$run_id"

dc() { docker compose -f "$compose_file" "$@"; }
broker() { dc ps --quiet broker; }
cli() { docker run --rm --network "container:$(broker)" "$SURGEWAVE_CLI_IMAGE" "$@"; }
# MSYS_NO_PATHCONV keeps Git Bash on Windows from rewriting the path inside the container; it means nothing elsewhere.
kafka() {
    MSYS_NO_PATHCONV=1 docker run --rm --interactive --network "container:$(broker)" "$KAFKA_IMAGE"         "/opt/kafka/bin/$1" "${@:2}"
}

fail() {
    echo "FAILED: $*" >&2
    dc ps --all >&2 || true
    dc logs --tail 80 >&2 || true
    exit 1
}

fresh() { dc down --volumes --remove-orphans; }

# Blocks until the health check passes; fails when the broker turns unhealthy or exits.
up() { dc up --detach --wait broker || fail "the broker did not become healthy"; }

consume_kafka() {
    kafka kafka-console-consumer.sh --bootstrap-server "$bootstrap" --topic "$kafka_topic" \
        --from-beginning --max-messages 1 --timeout-ms 60000 2> /dev/null
}

consume_native() {
    cli consume "$native_topic" --partition 0 --offset earliest --max-messages 1 --format Plain \
        --bootstrap-server "$bootstrap"
}

check_records() {
    local read
    read="$(consume_kafka)" || fail "no record of $kafka_topic over the Kafka wire protocol ($1)"
    [[ "$read" == *"value-kafka"* ]] || fail "the Kafka wire protocol did not return the record of $kafka_topic ($1): $read"
    read="$(consume_native)" || fail "no record of $native_topic over the native protocol ($1)"
    [[ "$read" == *"value-native"* ]] || fail "the native protocol did not return the record of $native_topic ($1): $read"
    echo "ok: one record read over the Kafka wire protocol and one over the native protocol ($1)"
}

trap 'fresh > /dev/null 2>&1 || true' EXIT

fresh
up
uid="$(docker exec "$(broker)" id -u)"
[[ "$uid" != "0" ]] || fail "the broker runs as root"
echo "ok: healthy with no configuration on ${SURGEWAVE_DATA_MOUNT:-a fresh named volume}, as uid $uid"

kafka kafka-topics.sh --bootstrap-server "$bootstrap" --create --topic "$kafka_topic" --partitions 1 --replication-factor 1 \
    > /dev/null || fail "topic $kafka_topic was not created over the Kafka wire protocol"
echo "value-kafka" | kafka kafka-console-producer.sh --bootstrap-server "$bootstrap" --topic "$kafka_topic" \
    || fail "no record was produced over the Kafka wire protocol"

cli topics create "$native_topic" --partitions 1 --replication-factor 1 --bootstrap-server "$bootstrap" > /dev/null \
    || fail "topic $native_topic was not created over the native protocol"
cli produce "$native_topic" --partition 0 --value "value-native" --bootstrap-server "$bootstrap" > /dev/null \
    || fail "no record was produced over the native protocol"

check_records "first start"

dc stop broker
up
check_records "restarted on its data"

echo "image smoke passed: $SURGEWAVE_BROKER_IMAGE"
