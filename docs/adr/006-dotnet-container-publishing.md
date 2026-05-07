# ADR-006: .NET Native Container Publishing

## Status

Accepted

## Date

2026-03

## Context

The original Surgewave build used a multi-stage Dockerfile to produce container images. This approach had several friction points:

- **Docker Desktop licensing:** Docker Desktop requires a commercial license for organizations over a certain size. Developers without it had to use alternative runtimes (Podman, Rancher Desktop), each with its own quirks.
- **Dockerfile maintenance:** The Dockerfile duplicated build logic already present in the .csproj (restore, build, publish, copy). Changes to the project structure required updating both.
- **CI complexity:** The CI pipeline needed Docker-in-Docker or a Docker socket mount to build images.

### Alternatives Considered

- **Keep Dockerfile with Podman:** Fixes the licensing issue but not the maintenance duplication.
- **Buildpacks (Paketo):** Good for standard apps but too opinionated for Surgewave's multi-project structure.
- **Nix-based builds:** Reproducible but steep learning curve and poor Windows support.

## Decision

Use .NET's native container publishing via `dotnet publish /t:PublishContainer`. Configure the container image entirely through MSBuild properties in the `.csproj`:

- Base image: `mcr.microsoft.com/dotnet/runtime-deps:10.0-jammy-chiseled` (~80MB)
- Container name, tags, ports, environment variables all declared in project properties.

Delete the Dockerfile entirely. No Docker tooling is required at build time --- the .NET SDK produces OCI-compliant images directly.

## Consequences

- **No Docker Desktop required.** Any developer with the .NET SDK can build container images.
- **Smaller images.** The chiseled base image has no shell, no package manager, and minimal attack surface (~80MB vs ~200MB+ with a standard base).
- **Single source of truth.** Build configuration lives in the .csproj, not duplicated in a Dockerfile.
- **CI simplification.** No Docker socket needed. `dotnet publish` is sufficient.
- **Lost Dockerfile features:** Multi-stage build caching, `COPY --from`, and arbitrary shell commands during build are no longer available. These were not needed for Surgewave's use case.
