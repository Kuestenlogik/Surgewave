#!/bin/bash
set -euo pipefail

# Uninstall Surgewave Helm release
# Usage: ./uninstall.sh [--namespace NS] [--release NAME] [--delete-pvcs]

NAMESPACE="${NAMESPACE:-surgewave}"
RELEASE_NAME="${RELEASE_NAME:-surgewave}"
DELETE_PVCS=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --namespace)
            NAMESPACE="$2"
            shift 2
            ;;
        --release)
            RELEASE_NAME="$2"
            shift 2
            ;;
        --delete-pvcs)
            DELETE_PVCS=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

echo "Uninstalling Surgewave..."
echo "  Release:     ${RELEASE_NAME}"
echo "  Namespace:   ${NAMESPACE}"
echo "  Delete PVCs: ${DELETE_PVCS}"

helm uninstall "${RELEASE_NAME}" --namespace "${NAMESPACE}"

if [[ "${DELETE_PVCS}" == "true" ]]; then
    echo "Deleting PersistentVolumeClaims..."
    kubectl delete pvc -l "app.kubernetes.io/instance=${RELEASE_NAME}" -n "${NAMESPACE}" 2>/dev/null || true
    echo "PVCs deleted."
fi

echo ""
echo "Surgewave uninstalled successfully!"
