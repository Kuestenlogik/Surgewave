#!/usr/bin/env bash
# Checks that a Surgewave StatefulSet already applied to a Kubernetes cluster is a broker
# cluster: every pod ready, a topic replicated to every broker created through one broker and
# read through the others, a deleted pod that rejoins, all pods deleted at once, a rolling
# restart.
#
#   scripts/ci/kubernetes-cluster-smoke.sh <namespace> <statefulset> <headless-service> [scenario ...]
#
# Scenarios (default: ready delete-one delete-all rolling-restart):
#   ready            the rollout completes and a replicated topic is read from every broker
#   delete-one       each pod in turn is deleted and becomes ready again
#   delete-all       all pods are deleted at once and come back on their volumes
#   rolling-restart  kubectl rollout restart: one pod at a time, each ready before the next
#
# Works for the Helm chart (deployments/helm/surgewave) and the plain manifests
# (deployments/kubernetes) alike; it uses whatever kubectl context is current.
#
# Environment:
#   SURGEWAVE_CLI_IMAGE        image of the surgewave CLI the cluster can run, required
#   SURGEWAVE_CLI_PULL_POLICY  its pull policy (default: IfNotPresent)
#   SURGEWAVE_CLIENT_PORT      the brokers' client port (default: 9092)
#   ROLLOUT_TIMEOUT            how long a rollout may take (default: 300s)
#
# Nothing here sleeps or polls: `kubectl rollout status` returns when the pods' readiness
# probes pass.
set -euo pipefail

if [[ $# -lt 3 ]]; then
    echo "usage: $0 <namespace> <statefulset> <headless-service> [scenario ...]" >&2
    exit 2
fi
namespace="$1" statefulset="$2" service="$3"
shift 3
: "${SURGEWAVE_CLI_IMAGE:?set SURGEWAVE_CLI_IMAGE to the surgewave CLI image}"
pull_policy="${SURGEWAVE_CLI_PULL_POLICY:-IfNotPresent}"
port="${SURGEWAVE_CLIENT_PORT:-9092}"
timeout="${ROLLOUT_TIMEOUT:-300s}"

k() { kubectl --namespace "$namespace" "$@"; }

replicas="$(k get statefulset "$statefulset" -o jsonpath='{.spec.replicas}')"
pods=()
for ((i = 0; i < replicas; i++)); do
    pods+=("$statefulset-$i")
done
client="surgewave-smoke-cli"
# Topics are named per run, so a cluster can be checked more than once.
run_id="$(date +%s)"

fail() {
    echo "FAILED: $*" >&2
    k get pods -o wide >&2 || true
    k get events --sort-by=.lastTimestamp >&2 | tail -n 30 || true
    for pod in "${pods[@]}"; do
        k logs "$pod" --tail 40 >&2 || true
    done
    exit 1
}

# The address a broker tells its clients: the pod's stable DNS name.
address() { echo "$1.$service.$namespace.svc:$port"; }

# One pod with the CLI for the whole run; the commands are executed in it, which returns their
# output and exit code as they are.
start_client() {
    k run "$client" --restart=Never --image="$SURGEWAVE_CLI_IMAGE" --image-pull-policy="$pull_policy" \
        --command -- sleep infinity
    k wait --for=condition=Ready "pod/$client" --timeout="$timeout" || fail "the CLI pod did not start"
}

cli() { k exec "$client" -- dotnet surgewave.dll "$@"; }

rolled_out() { k rollout status "statefulset/$statefulset" --timeout="$timeout" || fail "rollout of $statefulset: $*"; }

# Deletes pods and returns once the StatefulSet has created them again and they are ready.
replace_pods() {
    local pod
    k delete pod "$@" --wait=true
    for pod in "$@"; do
        k wait --for=create "pod/$pod" --timeout="$timeout" || fail "$pod was not created again"
        k wait --for=condition=Ready "pod/$pod" --timeout="$timeout" || fail "$pod did not become ready again"
    done
    rolled_out "after deleting $*"
}

check_topic() {
    local topic="smoke-$1-$run_id" value="value-$1" leader="" pod
    cli topics create "$topic" --partitions 1 --replication-factor "$replicas" --bootstrap-server "$(address "${pods[0]}")" \
        || fail "topic $topic was not created through ${pods[0]}"
    for pod in "${pods[@]}"; do
        cli topics describe "$topic" --bootstrap-server "$(address "$pod")" > /dev/null \
            || fail "$pod does not know topic $topic"
        k logs "$pod" | grep "Topic $topic created with 1 partitions, RF=$replicas" > /dev/null \
            || fail "$pod did not apply topic $topic with replication factor $replicas"
    done
    for pod in "${pods[@]}"; do
        if cli produce "$topic" --partition 0 --value "$value" --bootstrap-server "$(address "$pod")" > /dev/null 2>&1; then
            leader="$pod"
            break
        fi
    done
    [[ -n "$leader" ]] || fail "no broker accepted a record for $topic-0"
    check_record "$1"
    echo "ok: $topic created through ${pods[0]}, led by $leader, read from all $replicas brokers"
}

# A record written through the partition's leader is read from every broker: a follower
# answers from its own replica.
check_record() {
    local topic="smoke-$1-$run_id" value="value-$1" pod read
    for pod in "${pods[@]}"; do
        read="$(cli consume "$topic" --partition 0 --offset earliest --max-messages 1 --format Plain \
            --bootstrap-server "$(address "$pod")")" || fail "$pod did not answer for $topic"
        [[ "$read" == *"$value"* ]] || fail "$pod did not return the record of $topic: $read"
    done
}

run_ready() {
    rolled_out "initial"
    check_topic ready
}

run_delete_one() {
    local pod
    check_topic delete-one
    for pod in "${pods[@]}"; do
        replace_pods "$pod"
        check_record delete-one
        echo "ok: $pod rejoined and is ready"
    done
}

run_delete_all() {
    check_topic delete-all
    replace_pods "${pods[@]}"
    check_record delete-all
    check_topic after-delete-all
}

run_rolling_restart() {
    check_topic rolling-restart
    k rollout restart "statefulset/$statefulset"
    rolled_out "rolling restart"
    check_record rolling-restart
    echo "ok: rolling restart, one pod at a time"
}

scenarios=("$@")
if [[ ${#scenarios[@]} -eq 0 ]]; then
    scenarios=(ready delete-one delete-all rolling-restart)
fi
trap 'k delete pod "$client" --ignore-not-found --wait=false > /dev/null 2>&1 || true' EXIT
start_client
for scenario in "${scenarios[@]}"; do
    echo "== $scenario"
    case "$scenario" in
        ready) run_ready ;;
        delete-one) run_delete_one ;;
        delete-all) run_delete_all ;;
        rolling-restart) run_rolling_restart ;;
        *) echo "unknown scenario '$scenario'" >&2; exit 2 ;;
    esac
done
echo "all scenarios passed: ${scenarios[*]}"
