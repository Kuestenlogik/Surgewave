#!/usr/bin/env bash
# Checks the images of a release for each of their architectures: that the image of an
# architecture holds native binaries of that architecture only - the 0.5.2 linux-arm64 images
# held the x86-64 executable (#269, #276) - and that its labels are whole, and starts the
# linux-arm64 broker image under emulation until it reports ready.
#
#   scripts/ci/image-architecture.sh
#
# Environment:
#   SURGEWAVE_BROKER_IMAGE    broker image index under test, required
#   SURGEWAVE_CONTROL_IMAGE   Control image index under test, required
#   SURGEWAVE_EMULATION       "skip" leaves the start under emulation out, for a Docker daemon
#                             that cannot run linux/arm64; the check of the binaries is made
#                             without running anything
#   COMPOSE_PROJECT_NAME      compose project (default: surgewave-image-architecture)
#
# Nothing here sleeps or polls: `docker compose up --wait` returns when the health check
# passes. The containers and the volume it creates are removed first and at the end.
set -euo pipefail

: "${SURGEWAVE_BROKER_IMAGE:?set SURGEWAVE_BROKER_IMAGE to the broker image under test}"
: "${SURGEWAVE_CONTROL_IMAGE:?set SURGEWAVE_CONTROL_IMAGE to the Control image under test}"
export SURGEWAVE_BROKER_IMAGE
export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-surgewave-image-architecture}"

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
compose_file="$root/scripts/ci/image-smoke.compose.yml"
# Docker's name of a platform, and the machine an ELF file of it names.
platforms=("linux/amd64 x86-64" "linux/arm64 aarch64")

fail() { echo "FAILED: $*" >&2; exit 1; }

container=""
dc() { docker compose -f "$compose_file" "$@"; }
cleanup() {
    [[ -z "$container" ]] || docker rm --force "$container" > /dev/null 2>&1 || true
    dc down --volumes --remove-orphans > /dev/null 2>&1 || true
}
trap cleanup EXIT

for image in "$SURGEWAVE_BROKER_IMAGE" "$SURGEWAVE_CONTROL_IMAGE"; do
    for entry in "${platforms[@]}"; do
        read -r platform machine <<< "$entry"

        architecture="$(docker image inspect --platform "$platform" --format '{{.Os}}/{{.Architecture}}' "$image")" \
            || fail "$image holds no image for $platform"
        [[ "$architecture" == "$platform" ]] || fail "$image for $platform says it is for $architecture"

        # Created, never started: the file system is read as the image holds it, on any host.
        container="$(docker create --platform "$platform" "$image")"
        echo "$image, $platform:"
        docker export "$container" | node "$root/scripts/ci/elf-architecture.mjs" "$machine" \
            || fail "$image for $platform holds native binaries of another architecture"
        docker rm "$container" > /dev/null
        container=""

        # The SDK cut a label value at its first colon in an image of several architectures (#276).
        source_label="$(docker image inspect --platform "$platform" \
            --format '{{index .Config.Labels "org.opencontainers.image.source"}}' "$image")"
        [[ "$source_label" == https://* ]] \
            || fail "$image for $platform: org.opencontainers.image.source is '$source_label', not a URL"
        created_label="$(docker image inspect --platform "$platform" \
            --format '{{index .Config.Labels "org.opencontainers.image.created"}}' "$image")"
        [[ "$created_label" == *T*:*:* ]] \
            || fail "$image for $platform: org.opencontainers.image.created is '$created_label', not a time"
        echo "  labels whole: source $source_label, created $created_label"
    done
done

if [[ "${SURGEWAVE_EMULATION:-}" == "skip" ]]; then
    echo "image architecture check passed, the start under emulation left out"
    exit 0
fi

# The linux-arm64 broker, started as it comes on a fresh named volume, under emulation where the
# host is not arm64. Emulated, the broker takes several times as long to start as it does
# natively, and so does the probe that asks it, a process of its own: the health check waits
# longer for its first answer and gives each probe longer to give it.
export DOCKER_DEFAULT_PLATFORM=linux/arm64
export SURGEWAVE_HEALTH_START_PERIOD="${SURGEWAVE_HEALTH_START_PERIOD:-300s}"
export SURGEWAVE_HEALTH_TIMEOUT="${SURGEWAVE_HEALTH_TIMEOUT:-60s}"
dc down --volumes --remove-orphans
dc up --detach --wait broker || { dc logs --tail 80 >&2 || true; fail "the linux/arm64 broker did not become healthy"; }
machine="$(docker exec "$(dc ps --quiet broker)" uname -m)"
[[ "$machine" == "aarch64" ]] || fail "the broker started for linux/arm64 runs on $machine"
echo "ok: the linux/arm64 broker is healthy with no configuration on a fresh named volume ($machine)"

echo "image architecture check passed"
