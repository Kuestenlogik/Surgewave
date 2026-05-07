#!/bin/bash
set -euo pipefail

# Upgrade Surgewave via Helm chart
# Usage: ./upgrade.sh [--replicas N] [--namespace NS] [--values FILE]

NAMESPACE="${NAMESPACE:-surgewave}"
RELEASE_NAME="${RELEASE_NAME:-surgewave}"
VALUES_FILE=""
EXTRA_ARGS=()

while [[ $# -gt 0 ]]; do
    case $1 in
        --namespace)
            NAMESPACE="$2"
            shift 2
            ;;
        --values|-f)
            VALUES_FILE="$2"
            shift 2
            ;;
        --release)
            RELEASE_NAME="$2"
            shift 2
            ;;
        --set)
            EXTRA_ARGS+=(--set "$2")
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART_DIR="${SCRIPT_DIR}/../helm/surgewave"

echo "Upgrading Surgewave..."
echo "  Release:   ${RELEASE_NAME}"
echo "  Namespace: ${NAMESPACE}"
echo "  Chart:     ${CHART_DIR}"

HELM_ARGS=(
    upgrade "${RELEASE_NAME}" "${CHART_DIR}"
    --namespace "${NAMESPACE}"
)

if [[ -n "${VALUES_FILE}" ]]; then
    echo "  Values:    ${VALUES_FILE}"
    HELM_ARGS+=(-f "${VALUES_FILE}")
fi

if [[ ${#EXTRA_ARGS[@]} -gt 0 ]]; then
    HELM_ARGS+=("${EXTRA_ARGS[@]}")
fi

helm "${HELM_ARGS[@]}"

echo ""
echo "Surgewave upgraded successfully!"
echo "Run 'kubectl rollout status statefulset/${RELEASE_NAME} -n ${NAMESPACE}' to watch rollout."
