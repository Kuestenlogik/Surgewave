#!/bin/bash
set -euo pipefail

# Install Surgewave via Helm chart
# Usage: ./install.sh [--replicas N] [--namespace NS] [--values FILE]

REPLICAS="${REPLICAS:-3}"
NAMESPACE="${NAMESPACE:-surgewave}"
VALUES_FILE=""
RELEASE_NAME="${RELEASE_NAME:-surgewave}"

while [[ $# -gt 0 ]]; do
    case $1 in
        --replicas)
            REPLICAS="$2"
            shift 2
            ;;
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
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART_DIR="${SCRIPT_DIR}/../helm/surgewave"

echo "Installing Surgewave..."
echo "  Release:   ${RELEASE_NAME}"
echo "  Namespace: ${NAMESPACE}"
echo "  Replicas:  ${REPLICAS}"
echo "  Chart:     ${CHART_DIR}"

HELM_ARGS=(
    install "${RELEASE_NAME}" "${CHART_DIR}"
    --namespace "${NAMESPACE}"
    --create-namespace
    --set "broker.replicaCount=${REPLICAS}"
)

if [[ -n "${VALUES_FILE}" ]]; then
    echo "  Values:    ${VALUES_FILE}"
    HELM_ARGS+=(-f "${VALUES_FILE}")
fi

helm "${HELM_ARGS[@]}"

echo ""
echo "Surgewave installed successfully!"
echo "Run 'kubectl get pods -n ${NAMESPACE}' to check status."
