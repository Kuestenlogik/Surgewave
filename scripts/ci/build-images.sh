#!/usr/bin/env bash
# Builds the broker and the Control container image for linux-x64 and linux-arm64, each as one
# image index. The release publishes them with this script and CI builds them with it, so the
# way a release builds its images is the way that is tested before one.
#
#   scripts/ci/build-images.sh
#
# Environment:
#   IMAGE_REGISTRY    registry to push to (ghcr.io, docker.io); without it the images go to the
#                     local Docker daemon, which needs the containerd image store to hold an
#                     image of several architectures
#   IMAGE_NAMESPACE   what precedes "surgewave-broker" and "surgewave-control" in the repository
#                     names, with its trailing slash or dash (kuestenlogik/); default: none
#   IMAGE_TAGS        tags, separated by semicolons (0.6.0;latest); default: ci
#   IMAGE_VERSION     version of the build (-p:Version); default: the version of the tree
#   IMAGE_RUNTIMES    runtimes, separated by semicolons; default: linux-x64;linux-arm64
#
# The restore names the runtimes as well: without it the publish ends with NETSDK1047, the
# assets file holding no target for the second runtime.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
runtimes="${IMAGE_RUNTIMES:-linux-x64;linux-arm64}"
tags="${IMAGE_TAGS:-ci}"
namespace="${IMAGE_NAMESPACE:-}"

# A value with a semicolon in it reaches MSBuild whole only inside quotes of its own.
options=(-c Release -t:PublishContainer --no-restore -p:GeneratePackageOnBuild=false
    "-p:ContainerRuntimeIdentifiers=\"$runtimes\"" "-p:ContainerImageTags=\"$tags\"")
[[ -z "${IMAGE_VERSION:-}" ]] || options+=("-p:Version=$IMAGE_VERSION")
[[ -z "${IMAGE_REGISTRY:-}" ]] || options+=("-p:ContainerRegistry=$IMAGE_REGISTRY")

build() {
    local project="$root/src/$1" repository="$namespace$2"
    dotnet restore "$project" "-p:RuntimeIdentifiers=\"$runtimes\""
    dotnet publish "$project" "${options[@]}" "-p:ContainerRepository=$repository"
    echo "built $repository ($tags) for $runtimes${IMAGE_REGISTRY:+ in $IMAGE_REGISTRY}"
}

build Kuestenlogik.Surgewave.Broker.App surgewave-broker
build Kuestenlogik.Surgewave.Control surgewave-control
