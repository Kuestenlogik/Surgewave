#!/usr/bin/env bash
# Brings deployments/docker/docker-compose.cluster.yml up and checks that it is a cluster:
# three healthy brokers, a topic with replication factor 3 created through one broker and
# read through the others, the topic listed by every broker over gRPC and over REST through
# the ports the file publishes on the Docker host, a broker that is stopped and comes back,
# all brokers restarted on their data.
#
#   scripts/ci/compose-cluster-smoke.sh [scenario ...]
#
# Scenarios (default: together restart-one restart-all):
#   together      all three brokers started at once
#   alone         broker 1 alone is live and not ready; ready once broker 2 runs; then broker 3
#   late-1        brokers 2 and 3 first, broker 1 once they are healthy
#   late-3        brokers 1 and 2 first, broker 3 once they are healthy
#   restart-one   each broker in turn stopped and started again (needs a running cluster)
#   restart-all   all brokers stopped and started together on their data (needs a running cluster)
#
# A scenario that starts brokers starts from nothing: containers and volumes of the project
# are removed first. Everything is removed again at the end.
#
# Environment:
#   SURGEWAVE_BROKER_IMAGE  broker image under test (the compose file's default otherwise)
#   SURGEWAVE_CLI_IMAGE     image of the surgewave CLI, required
#   COMPOSE_PROJECT_NAME    compose project (default: surgewave-smoke)
#   COMPOSE_OVERRIDE        optional second compose file, e.g. CPU limits or other ports
#   GRPCURL_IMAGE           image of grpcurl, for the gRPC calls (default: pinned below)
#
# Needs docker with compose, and curl for the REST calls.
#
# Nothing here sleeps or polls: `docker compose up --wait` returns when the health checks of
# the services pass, and a log line is waited for on the log stream.
set -euo pipefail

: "${SURGEWAVE_CLI_IMAGE:?set SURGEWAVE_CLI_IMAGE to the surgewave CLI image}"
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-surgewave-smoke}"

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
compose_files=(-f "$root/deployments/docker/docker-compose.cluster.yml")
if [[ -n "${COMPOSE_OVERRIDE:-}" ]]; then
    compose_files+=(-f "$COMPOSE_OVERRIDE")
fi
brokers=(surgewave-1 surgewave-2 surgewave-3)
network="${COMPOSE_PROJECT_NAME}_surgewave-cluster"
# Topics are named per run.
run_id="$(date +%s)"
# The brokers' HTTP endpoints inside their containers: REST, /health and /metrics on the first,
# gRPC on the second. Which host port each is published on is asked of Compose.
rest_port=9095
grpc_port=9098
# Pinned by digest: fullstorydev/grpcurl:v1.9.3-alpine.
grpcurl_image="${GRPCURL_IMAGE:-fullstorydev/grpcurl@sha256:4614424ed58e9b9837c48b6b8eadb9ef40491d5af3499bcc8b378e9c64a9e4a9}"

dc() { docker compose "${compose_files[@]}" "$@"; }
cli() { docker run --rm --network "$network" "$SURGEWAVE_CLI_IMAGE" "$@"; }
# The broker executable asks its own health endpoint; the image's working directory is the application's.
probe() { dc exec -T "$1" dotnet surgewave-broker.dll health "$2"; }
# The port of the Docker host a broker's port is published on.
published() {
    local address
    address="$(dc port "$1" "$2")" || fail "$1 does not publish port $2"
    echo "${address##*:}"
}
# grpcurl in the Docker host's network: it reaches a broker as a client on the host does,
# through the published port.
grpc() { docker run --rm --network host "$grpcurl_image" -plaintext "$@"; }

fail() {
    echo "FAILED: $*" >&2
    dc ps >&2 || true
    dc logs --tail 80 >&2 || true
    exit 1
}

fresh() { dc down --volumes --remove-orphans; }

# Blocks until the services' health checks pass; fails when one turns unhealthy or exits.
up() { dc up --detach --wait --no-deps "$@" || fail "not healthy: $*"; }

# Creates a topic with replication factor 3 through one broker, sees it through the others,
# writes a record through the leader of partition 0 and reads it from every broker: a follower
# answers from its own replica.
check_topic() {
    local topic="smoke-$1-$run_id" value="value-$1" leader="" broker
    cli topics create "$topic" --partitions 1 --replication-factor 3 --bootstrap-server surgewave-1:9092 \
        || fail "topic $topic was not created through surgewave-1"
    for broker in "${brokers[@]}"; do
        cli topics describe "$topic" --bootstrap-server "$broker:9092" > /dev/null \
            || fail "$broker does not know topic $topic"
        dc logs --no-log-prefix "$broker" | grep "Topic $topic created with 1 partitions, RF=3" > /dev/null \
            || fail "$broker did not apply topic $topic with replication factor 3"
    done
    for broker in "${brokers[@]}"; do
        if cli produce "$topic" --partition 0 --value "$value" --bootstrap-server "$broker:9092" > /dev/null 2>&1; then
            leader="$broker"
            break
        fi
    done
    [[ -n "$leader" ]] || fail "no broker accepted a record for $topic-0"
    check_record "$1"
    echo "ok: $topic created through surgewave-1, led by $leader, read from all three brokers"
    check_http "$1"
}

# Every broker lists the topic over gRPC and over REST, each through the port published for
# it on the Docker host. Without TLS they are two ports: a cleartext endpoint serves HTTP/2,
# which gRPC needs, or HTTP/1.1, and a deployment that publishes the REST port alone has no
# gRPC (HTTP_1_1_REQUIRED).
check_http() {
    local topic="smoke-$1-$run_id" broker port listed
    for broker in "${brokers[@]}"; do
        port="$(published "$broker" "$grpc_port")"
        listed="$(grpc "localhost:$port" klsurgewave.TopicService/ListTopics)" \
            || fail "$broker did not answer a gRPC call on port $port of the Docker host"
        [[ "$listed" == *"\"$topic\""* ]] || fail "$broker does not list $topic over gRPC: $listed"
        port="$(published "$broker" "$rest_port")"
        listed="$(curl --fail --silent --show-error "http://localhost:$port/v3/topics")" \
            || fail "$broker did not answer a REST call on port $port of the Docker host"
        [[ "$listed" == *"\"$topic\""* ]] || fail "$broker does not list $topic over REST: $listed"
    done
    echo "ok: $topic listed by all three brokers over gRPC and over REST, through the published ports"
}

# A record written through the partition's leader is read from every broker: a follower
# answers from its own replica.
check_record() {
    local topic="smoke-$1-$run_id" value="value-$1" broker read
    for broker in "${brokers[@]}"; do
        read="$(cli consume "$topic" --partition 0 --offset earliest --max-messages 1 --format Plain \
            --bootstrap-server "$broker:9092")" || fail "$broker did not answer for $topic"
        [[ "$read" == *"$value"* ]] || fail "$broker did not return the record of $topic: $read"
    done
}

run_together() {
    fresh
    up "${brokers[@]}"
    check_topic together
}

run_alone() {
    fresh
    dc up --detach --no-deps surgewave-1
    # The line a broker writes when its listener is bound and it waits for the metadata log.
    grep -q -m 1 "Application started" < <(dc logs --follow --no-log-prefix surgewave-1) \
        || fail "surgewave-1 did not start"
    probe surgewave-1 live || fail "surgewave-1 alone is not live"
    if probe surgewave-1 ready; then
        fail "surgewave-1 reports ready without a quorum"
    fi
    echo "ok: surgewave-1 alone is live and not ready"
    up surgewave-1 surgewave-2
    echo "ok: surgewave-1 and surgewave-2 are ready as soon as there are two"
    up surgewave-3
    check_topic alone
}

run_late() {
    local late="surgewave-$1" early=() broker
    for broker in "${brokers[@]}"; do
        [[ "$broker" == "$late" ]] || early+=("$broker")
    done
    fresh
    up "${early[@]}"
    echo "ok: ${early[*]} are healthy without $late"
    up "$late"
    check_topic "late-$1"
}

run_restart_one() {
    local broker
    check_topic restart-one
    for broker in "${brokers[@]}"; do
        dc stop "$broker"
        up "$broker"
        echo "ok: $broker is healthy again"
        check_record restart-one
        check_http restart-one
    done
}

run_restart_all() {
    check_topic restart-all
    dc stop
    up "${brokers[@]}"
    check_record restart-all
    check_http restart-all
    check_topic after-restart-all
}

trap 'fresh > /dev/null 2>&1 || true' EXIT

scenarios=("$@")
if [[ ${#scenarios[@]} -eq 0 ]]; then
    scenarios=(together restart-one restart-all)
fi
for scenario in "${scenarios[@]}"; do
    echo "== $scenario"
    case "$scenario" in
        together) run_together ;;
        alone) run_alone ;;
        late-1) run_late 1 ;;
        late-3) run_late 3 ;;
        restart-one) run_restart_one ;;
        restart-all) run_restart_all ;;
        *) echo "unknown scenario '$scenario'" >&2; exit 2 ;;
    esac
done
echo "all scenarios passed: ${scenarios[*]}"
