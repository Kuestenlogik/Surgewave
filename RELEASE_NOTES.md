# Surgewave Release Notes

**This file is generated.** Edit the release body on GitHub instead:
https://github.com/Kuestenlogik/Surgewave/releases

The script `scripts/ci/generate-release-notes.mjs` pulls every published
release (and optionally drafts via `--include-drafts`) and writes
the body out here so the notes are readable offline. Curate the body of
the NEXT release in `docs/release-notes/upcoming.md` (see the README
there); use the GitHub Release UI or `gh release edit <tag>` to change
published text; re-run the generator to refresh this mirror.

---
## v0.4.0 — 2026-07-16 — Production hardening & trustworthy admin

This release makes a Surgewave cluster survivable and administrable in production: the entire inter-broker control plane comes off the Kafka wire, broker epochs become failover-durable, the admin surface gets real server-side enforcement — and an exhaustive modern-.NET audit hardened correctness on the hot paths before the tag.

## Highlights

### Native, plugin-free clustering — the control plane leaves the Kafka wire (#60)

The whole inter-broker control plane — LeaderAndIsr, UpdateMetadata, StopReplica, AlterPartition, broker registration/heartbeat and WriteTxnMarkers — now travels the native SRWV protocol on the ReplicationPort. A broker **joins and operates a cluster without the Kafka plugin loaded**. Rolling upgrades stay safe through an IBP-style `inter.broker.protocol` feature negotiation: the finalized level is the cluster-wide minimum, so the wire only flips to native once **every** peer can speak it, and a single older broker pins the cluster to the Kafka wire.

### Durable broker epochs + one membership authority (#72)

Broker epochs are now monotone across controller failover *and* restarts: a composed epoch mint backed by a node-local controller-epoch high-water file, and in Raft mode the epoch is the **committed metadata-log index** (KRaft parity). One `ClusterMembershipService` is the registration authority for both wires, so a broker registered over either protocol heartbeats coherently over the other. Transaction markers now replicate from the **live** coordinator — best-effort with visible per-partition outcomes and a bounded, partition-scoped retry that can never double-write.

### Legacy follower replication wired end-to-end (#69)

Non-Raft follower replication (fetcher, ISR formation, LeaderAndIsr push) is connected end-to-end — followers catch up and the ISR actually forms in the classic mode, including correct replication-port discovery so fetchers dial the right endpoint.

### Trustworthy admin: server-side role enforcement, REST auth, alert evaluation (#37, #38)

Role management leaves Preview: roles are enforced **server-side** (not just hidden in the UI), the broker REST surface authenticates, and alert rules now evaluate in the broker — alerts fire even when no Control UI tab is open.

### Control UI: KV store + transactions (#39, #40)

The Control UI gains full pages for the KV store (`/api/kv`) and for transactions (`/v3/transactions`, including cross-topic), closing the gap between what the broker serves and what the UI can administer.

### Client: automatic protocol selection (#71)

The Surgewave client resolves `auto` / `native` / `kafka` per connection and the native-first auto-detection actually works — one client config serves mixed fleets during migrations.

## Fixes

### Correctness & durability fixes from the modern-.NET perf audit (#73, #74, #75, #76, #77)

A 7-subsystem, adversarially verified audit of the zero-copy/pooling paths surfaced five real defects, all fixed in this release: a span-hash intern-cache collision that could route produced records to the **wrong topic**; two pooled-buffer leaks (decompression rent on the native fetch path, LOH-sized rent on trimmed storage reads); a flush path whose fsync **never reached the disk** (durability was page-cache-only); and a replication size-prefix short-read that desynced the connection.

### Packaging & CI hygiene (#54, #55, #56)

Vulnerable transitives are pinned (Microsoft.OpenApi 2.9.0; the unpatched SQLitePCLRaw advisory is suppressed and tracked in #88), the Broker NuGet no longer references phantom static-web-assets, and the flaky Release-CI replication test is stabilized.

## Acknowledgements

The perf-audit fixes in this release were found by an exhaustive multi-agent review of the transport, codec, storage, SIMD and client layers — the remaining (non-correctness) findings are tracked as #78–#87 for v0.8.

<details>
<summary><b>Installation</b> — NuGet, Container, MSI, Linux, Helm</summary>

**NuGet:**
```bash
dotnet add package Kuestenlogik.Surgewave.Client --version 0.4.0
```

**Container — pre-built (Docker Hub, multi-arch linux/amd64 + arm64):**
```bash
docker pull kuestenlogik/surgewave-broker:0.4.0
docker pull kuestenlogik/surgewave-control:0.4.0
# …oder :latest
```

**Container — alternative registry (GHCR):**
```bash
docker pull ghcr.io/kuestenlogik/surgewave-broker:0.4.0
docker pull ghcr.io/kuestenlogik/surgewave-control:0.4.0
```

**Container — build from source:**
```bash
dotnet publish src/Kuestenlogik.Surgewave.Broker -c Release /t:PublishContainer
dotnet publish src/Kuestenlogik.Surgewave.Control -c Release /t:PublishContainer
```

**Windows MSI (silent install):**
```powershell
msiexec /i surgewave-0.4.0-win-x64.msi /qn
```

**Linux:**
```bash
sudo bash install.sh
```

**Helm:**
```bash
helm install surgewave deploy/helm/surgewave/ --set broker.image.tag=0.4.0
```

</details>

---

_The full commit list, contributors, and compare-URL diff are auto-generated below._

## What's Changed
* chore: bump floor to 0.3.1-dev (after v0.3.1) by @thomas-stegemann in https://github.com/Kuestenlogik/Surgewave/pull/57

## New Contributors
* @thomas-stegemann made their first contribution in https://github.com/Kuestenlogik/Surgewave/pull/57

**Full Changelog**: https://github.com/Kuestenlogik/Surgewave/compare/v0.3.1...v0.4.0

---
## v0.3.1 — 2026-07-02

## Surgewave v0.3.1

### Installation

**NuGet:**
```bash
dotnet add package Kuestenlogik.Surgewave.Client --version 0.3.1
```

**Container — pre-built (Docker Hub, multi-arch linux/amd64 + arm64):**
```bash
docker pull kuestenlogik/surgewave-broker:0.3.1
docker pull kuestenlogik/surgewave-control:0.3.1
# …oder :latest
```

**Container — alternative registry (GHCR):**
```bash
docker pull ghcr.io/kuestenlogik/surgewave-broker:0.3.1
docker pull ghcr.io/kuestenlogik/surgewave-control:0.3.1
```

**Container — build from source:**
```bash
dotnet publish src/Kuestenlogik.Surgewave.Broker -c Release /t:PublishContainer
dotnet publish src/Kuestenlogik.Surgewave.Control -c Release /t:PublishContainer
```

**Windows MSI (silent install):**
```powershell
msiexec /i surgewave-0.3.1-win-x64.msi /qn
```

**Linux:**
```bash
sudo bash install.sh
```

**Helm:**
```bash
helm install surgewave deploy/helm/surgewave/ --set broker.image.tag=0.3.1
```


**Full Changelog**: https://github.com/Kuestenlogik/Surgewave/compare/v0.3.0...v0.3.1

---
## v0.3.0 — 2026-07-02

## Surgewave v0.3.0

### Installation

**NuGet:**
```bash
dotnet add package Kuestenlogik.Surgewave.Client --version 0.3.0
```

**Container — pre-built (Docker Hub, multi-arch linux/amd64 + arm64):**
```bash
docker pull kuestenlogik/surgewave-broker:0.3.0
docker pull kuestenlogik/surgewave-control:0.3.0
# …oder :latest
```

**Container — alternative registry (GHCR):**
```bash
docker pull ghcr.io/kuestenlogik/surgewave-broker:0.3.0
docker pull ghcr.io/kuestenlogik/surgewave-control:0.3.0
```

**Container — build from source:**
```bash
dotnet publish src/Kuestenlogik.Surgewave.Broker -c Release /t:PublishContainer
dotnet publish src/Kuestenlogik.Surgewave.Control -c Release /t:PublishContainer
```

**Windows MSI (silent install):**
```powershell
msiexec /i surgewave-0.3.0-win-x64.msi /qn
```

**Linux:**
```bash
sudo bash install.sh
```

**Helm:**
```bash
helm install surgewave deploy/helm/surgewave/ --set broker.image.tag=0.3.0
```


## What's Changed
* chore(deps): bump actions/checkout from 6 to 7 in the actions group across 1 directory by @dependabot[bot] in https://github.com/Kuestenlogik/Surgewave/pull/33
* chore(deps): bump actions/cache from 5 to 6 in the actions group by @dependabot[bot] in https://github.com/Kuestenlogik/Surgewave/pull/34


**Full Changelog**: https://github.com/Kuestenlogik/Surgewave/compare/v0.2.0...v0.3.0

---
## v0.2.0 — 2026-06-21

## Surgewave v0.2.0

### Installation

**NuGet:**
```bash
dotnet add package Kuestenlogik.Surgewave.Client --version 0.2.0
```

**Container — pre-built (Docker Hub, multi-arch linux/amd64 + arm64):**
```bash
docker pull kuestenlogik/surgewave-broker:0.2.0
docker pull kuestenlogik/surgewave-control:0.2.0
# …oder :latest
```

**Container — alternative registry (GHCR):**
```bash
docker pull ghcr.io/kuestenlogik/surgewave-broker:0.2.0
docker pull ghcr.io/kuestenlogik/surgewave-control:0.2.0
```

**Container — build from source:**
```bash
dotnet publish src/Kuestenlogik.Surgewave.Broker -c Release /t:PublishContainer
dotnet publish src/Kuestenlogik.Surgewave.Control -c Release /t:PublishContainer
```

**Windows MSI (silent install):**
```powershell
msiexec /i surgewave-0.2.0-win-x64.msi /qn
```

**Linux:**
```bash
sudo bash install.sh
```

**Helm:**
```bash
helm install surgewave deploy/helm/surgewave/ --set broker.image.tag=0.2.0
```


## What's Changed
* Bump the dotnet group with 2 updates by @dependabot[bot] in https://github.com/Kuestenlogik/Surgewave/pull/3


**Full Changelog**: https://github.com/Kuestenlogik/Surgewave/compare/v0.1.12...v0.2.0

---
## v0.1.13 — 2026-06-08

## Surgewave v0.1.13

### Installation

**NuGet:**
```bash
dotnet add package Kuestenlogik.Surgewave.Client --version 0.1.13
```

**Container — pre-built (Docker Hub, multi-arch linux/amd64 + arm64):**
```bash
docker pull kuestenlogik/surgewave-broker:0.1.13
docker pull kuestenlogik/surgewave-control:0.1.13
# …oder :latest
```

**Container — alternative registry (GHCR):**
```bash
docker pull ghcr.io/kuestenlogik/surgewave-broker:0.1.13
docker pull ghcr.io/kuestenlogik/surgewave-control:0.1.13
```

**Container — build from source:**
```bash
dotnet publish src/Kuestenlogik.Surgewave.Broker -c Release /t:PublishContainer
dotnet publish src/Kuestenlogik.Surgewave.Control -c Release /t:PublishContainer
```

**Windows MSI (silent install):**
```powershell
msiexec /i surgewave-0.1.13-win-x64.msi /qn
```

**Linux:**
```bash
sudo bash install.sh
```

**Helm:**
```bash
helm install surgewave deploy/helm/surgewave/ --set broker.image.tag=0.1.13
```


## What's Changed
* Bump the dotnet group with 2 updates by @dependabot[bot] in https://github.com/Kuestenlogik/Surgewave/pull/3


**Full Changelog**: https://github.com/Kuestenlogik/Surgewave/compare/v0.1.12...v0.1.13

---
## v0.1.12 — 2026-06-06

## Surgewave v0.1.12

### Installation

**NuGet:**
```bash
dotnet add package Kuestenlogik.Surgewave.Client --version 0.1.12
```

**Container — pre-built (Docker Hub, multi-arch linux/amd64 + arm64):**
```bash
docker pull kuestenlogik/surgewave-broker:0.1.12
docker pull kuestenlogik/surgewave-control:0.1.12
# …oder :latest
```

**Container — alternative registry (GHCR):**
```bash
docker pull ghcr.io/kuestenlogik/surgewave-broker:0.1.12
docker pull ghcr.io/kuestenlogik/surgewave-control:0.1.12
```

**Container — build from source:**
```bash
dotnet publish src/Kuestenlogik.Surgewave.Broker -c Release /t:PublishContainer
dotnet publish src/Kuestenlogik.Surgewave.Control -c Release /t:PublishContainer
```

**Windows MSI (silent install):**
```powershell
msiexec /i surgewave-0.1.12-win-x64.msi /qn
```

**Linux:**
```bash
sudo bash install.sh
```

**Helm:**
```bash
helm install surgewave deploy/helm/surgewave/ --set broker.image.tag=0.1.12
```


## What's Changed
* Bump Microsoft.Build.Framework and 3 others by @dependabot[bot] in https://github.com/Kuestenlogik/Surgewave/pull/2


**Full Changelog**: https://github.com/Kuestenlogik/Surgewave/compare/v0.1.11...v0.1.12

---
## v0.1.11 — 2026-06-02

## Surgewave v0.1.11

### Installation

**NuGet:**
```bash
dotnet add package Kuestenlogik.Surgewave.Client --version 0.1.11
```

**Container — pre-built (Docker Hub, multi-arch linux/amd64 + arm64):**
```bash
docker pull kuestenlogik/surgewave-broker:0.1.11
docker pull kuestenlogik/surgewave-control:0.1.11
# …oder :latest
```

**Container — alternative registry (GHCR):**
```bash
docker pull ghcr.io/kuestenlogik/surgewave-broker:0.1.11
docker pull ghcr.io/kuestenlogik/surgewave-control:0.1.11
```

**Container — build from source:**
```bash
dotnet publish src/Kuestenlogik.Surgewave.Broker -c Release /t:PublishContainer
dotnet publish src/Kuestenlogik.Surgewave.Control -c Release /t:PublishContainer
```

**Windows MSI (silent install):**
```powershell
msiexec /i surgewave-0.1.11-win-x64.msi /qn
```

**Linux:**
```bash
sudo bash install.sh
```

**Helm:**
```bash
helm install surgewave deploy/helm/surgewave/ --set broker.image.tag=0.1.11
```


**Full Changelog**: https://github.com/Kuestenlogik/Surgewave/compare/v0.1.10...v0.1.11

---
## v0.1.10 — 2026-05-31

## Surgewave v0.1.10

### Installation

**NuGet:**
```bash
dotnet add package Kuestenlogik.Surgewave.Client --version 0.1.10
```

**Container — pre-built (Docker Hub, multi-arch linux/amd64 + arm64):**
```bash
docker pull kuestenlogik/surgewave-broker:0.1.10
docker pull kuestenlogik/surgewave-control:0.1.10
# …oder :latest
```

**Container — alternative registry (GHCR):**
```bash
docker pull ghcr.io/kuestenlogik/surgewave-broker:0.1.10
docker pull ghcr.io/kuestenlogik/surgewave-control:0.1.10
```

**Container — build from source:**
```bash
dotnet publish src/Kuestenlogik.Surgewave.Broker -c Release /t:PublishContainer
dotnet publish src/Kuestenlogik.Surgewave.Control -c Release /t:PublishContainer
```

**Windows MSI (silent install):**
```powershell
msiexec /i surgewave-0.1.10-win-x64.msi /qn
```

**Linux:**
```bash
sudo bash install.sh
```

**Helm:**
```bash
helm install surgewave deploy/helm/surgewave/ --set broker.image.tag=0.1.10
```


**Full Changelog**: https://github.com/Kuestenlogik/Surgewave/compare/v0.1.9...v0.1.10

---
## v0.1.9 — 2026-05-30

## Surgewave v0.1.9

### Installation

**NuGet:**
```bash
dotnet add package Kuestenlogik.Surgewave.Client --version 0.1.9
```

**Container — pre-built (Docker Hub, multi-arch linux/amd64 + arm64):**
```bash
docker pull kuestenlogik/surgewave-broker:0.1.9
docker pull kuestenlogik/surgewave-control:0.1.9
# …oder :latest
```

**Container — alternative registry (GHCR):**
```bash
docker pull ghcr.io/kuestenlogik/surgewave-broker:0.1.9
docker pull ghcr.io/kuestenlogik/surgewave-control:0.1.9
```

**Container — build from source:**
```bash
dotnet publish src/Kuestenlogik.Surgewave.Broker -c Release /t:PublishContainer
dotnet publish src/Kuestenlogik.Surgewave.Control -c Release /t:PublishContainer
```

**Windows MSI (silent install):**
```powershell
msiexec /i surgewave-0.1.9-win-x64.msi /qn
```

**Linux:**
```bash
sudo bash install.sh
```

**Helm:**
```bash
helm install surgewave deploy/helm/surgewave/ --set broker.image.tag=0.1.9
```


**Full Changelog**: https://github.com/Kuestenlogik/Surgewave/compare/v0.1.8...v0.1.9

---
## v0.1.6 — 2026-05-30

## Surgewave v0.1.6

### Installation

**NuGet:**
```bash
dotnet add package Kuestenlogik.Surgewave.Client --version 0.1.6
```

**Container — pre-built (Docker Hub, multi-arch linux/amd64 + arm64):**
```bash
docker pull kuestenlogik/surgewave-broker:0.1.6
docker pull kuestenlogik/surgewave-control:0.1.6
# …oder :latest
```

**Container — alternative registry (GHCR):**
```bash
docker pull ghcr.io/kuestenlogik/surgewave-broker:0.1.6
docker pull ghcr.io/kuestenlogik/surgewave-control:0.1.6
```

**Container — build from source:**
```bash
dotnet publish src/Kuestenlogik.Surgewave.Broker -c Release /t:PublishContainer
dotnet publish src/Kuestenlogik.Surgewave.Control -c Release /t:PublishContainer
```

**Windows MSI (silent install):**
```powershell
msiexec /i surgewave-0.1.6-win-x64.msi /qn
```

**Linux:**
```bash
sudo bash install.sh
```

**Helm:**
```bash
helm install surgewave deploy/helm/surgewave/ --set broker.image.tag=0.1.6
```


**Full Changelog**: https://github.com/Kuestenlogik/Surgewave/compare/v0.1.5...v0.1.6

---
## v0.1.5 — 2026-05-27

## Surgewave v0.1.5

### Installation

**NuGet:**
```bash
dotnet add package Kuestenlogik.Surgewave.Client --version 0.1.5
```

**Container — pre-built (Docker Hub, multi-arch linux/amd64 + arm64):**
```bash
docker pull kuestenlogik/surgewave-broker:0.1.5
docker pull kuestenlogik/surgewave-control:0.1.5
# …oder :latest
```

**Container — alternative registry (GHCR):**
```bash
docker pull ghcr.io/kuestenlogik/surgewave-broker:0.1.5
docker pull ghcr.io/kuestenlogik/surgewave-control:0.1.5
```

**Container — build from source:**
```bash
dotnet publish src/Kuestenlogik.Surgewave.Broker -c Release /t:PublishContainer
dotnet publish src/Kuestenlogik.Surgewave.Control -c Release /t:PublishContainer
```

**Windows MSI (silent install):**
```powershell
msiexec /i surgewave-0.1.5-win-x64.msi /qn
```

**Linux:**
```bash
sudo bash install.sh
```

**Helm:**
```bash
helm install surgewave deploy/helm/surgewave/ --set broker.image.tag=0.1.5
```


**Full Changelog**: https://github.com/Kuestenlogik/Surgewave/compare/v0.1.4...v0.1.5

---
## v0.1.4 — 2026-05-26

## Surgewave v0.1.4

### Installation

**NuGet:**
```bash
dotnet add package Kuestenlogik.Surgewave.Client --version 0.1.4
```

**Container — pre-built (Docker Hub, multi-arch linux/amd64 + arm64):**
```bash
docker pull kuestenlogik/surgewave-broker:0.1.4
docker pull kuestenlogik/surgewave-control:0.1.4
# …oder :latest
```

**Container — alternative registry (GHCR):**
```bash
docker pull ghcr.io/kuestenlogik/surgewave-broker:0.1.4
docker pull ghcr.io/kuestenlogik/surgewave-control:0.1.4
```

**Container — build from source:**
```bash
dotnet publish src/Kuestenlogik.Surgewave.Broker -c Release /t:PublishContainer
dotnet publish src/Kuestenlogik.Surgewave.Control -c Release /t:PublishContainer
```

**Windows MSI (silent install):**
```powershell
msiexec /i surgewave-0.1.4-win-x64.msi /qn
```

**Linux:**
```bash
sudo bash install.sh
```

**Helm:**
```bash
helm install surgewave deploy/helm/surgewave/ --set broker.image.tag=0.1.4
```


**Full Changelog**: https://github.com/Kuestenlogik/Surgewave/compare/v0.1.3...v0.1.4

---
## v0.1.3 — 2026-05-26

## Surgewave v0.1.3

### Installation

**NuGet:**
```bash
dotnet add package Kuestenlogik.Surgewave.Client --version 0.1.3
```

**Container — pre-built (Docker Hub, multi-arch linux/amd64 + arm64):**
```bash
docker pull kuestenlogik/surgewave-broker:0.1.3
docker pull kuestenlogik/surgewave-control:0.1.3
# …oder :latest
```

**Container — alternative registry (GHCR):**
```bash
docker pull ghcr.io/kuestenlogik/surgewave-broker:0.1.3
docker pull ghcr.io/kuestenlogik/surgewave-control:0.1.3
```

**Container — build from source:**
```bash
dotnet publish src/Kuestenlogik.Surgewave.Broker -c Release /t:PublishContainer
dotnet publish src/Kuestenlogik.Surgewave.Control -c Release /t:PublishContainer
```

**Windows MSI (silent install):**
```powershell
msiexec /i surgewave-0.1.3-win-x64.msi /qn
```

**Linux:**
```bash
sudo bash install.sh
```

**Helm:**
```bash
helm install surgewave deploy/helm/surgewave/ --set broker.image.tag=0.1.3
```


**Full Changelog**: https://github.com/Kuestenlogik/Surgewave/compare/v0.1.2...v0.1.3

---
## v0.1.2 — 2026-05-26

## Surgewave v0.1.2

### Installation

**NuGet:**
```bash
dotnet add package Kuestenlogik.Surgewave.Client --version 0.1.2
```

**Container — pre-built (Docker Hub, multi-arch linux/amd64 + arm64):**
```bash
docker pull kuestenlogik/surgewave-broker:0.1.2
docker pull kuestenlogik/surgewave-control:0.1.2
# …oder :latest
```

**Container — alternative registry (GHCR):**
```bash
docker pull ghcr.io/kuestenlogik/surgewave-broker:0.1.2
docker pull ghcr.io/kuestenlogik/surgewave-control:0.1.2
```

**Container — build from source:**
```bash
dotnet publish src/Kuestenlogik.Surgewave.Broker -c Release /t:PublishContainer
dotnet publish src/Kuestenlogik.Surgewave.Control -c Release /t:PublishContainer
```

**Windows MSI (silent install):**
```powershell
msiexec /i surgewave-0.1.2-win-x64.msi /qn
```

**Linux:**
```bash
sudo bash install.sh
```

**Helm:**
```bash
helm install surgewave deploy/helm/surgewave/ --set broker.image.tag=0.1.2
```


**Full Changelog**: https://github.com/Kuestenlogik/Surgewave/compare/v0.1.1...v0.1.2

---
## v0.1.1 — 2026-05-26

## Surgewave v0.1.1

### Installation

**NuGet:**
```bash
dotnet add package Kuestenlogik.Surgewave.Client --version 0.1.1
```

**Container — pre-built (Docker Hub, multi-arch linux/amd64 + arm64):**
```bash
docker pull kuestenlogik/surgewave-broker:0.1.1
docker pull kuestenlogik/surgewave-control:0.1.1
# …oder :latest
```

**Container — alternative registry (GHCR):**
```bash
docker pull ghcr.io/kuestenlogik/surgewave-broker:0.1.1
docker pull ghcr.io/kuestenlogik/surgewave-control:0.1.1
```

**Container — build from source:**
```bash
dotnet publish src/Kuestenlogik.Surgewave.Broker -c Release /t:PublishContainer
dotnet publish src/Kuestenlogik.Surgewave.Control -c Release /t:PublishContainer
```

**Windows MSI (silent install):**
```powershell
msiexec /i surgewave-0.1.1-win-x64.msi /qn
```

**Linux:**
```bash
sudo bash install.sh
```

**Helm:**
```bash
helm install surgewave deploy/helm/surgewave/ --set broker.image.tag=0.1.1
```


**Full Changelog**: https://github.com/Kuestenlogik/Surgewave/compare/v0.1.0...v0.1.1

---
## v0.1.0 — 2026-05-26

## Surgewave v0.1.0

### Installation

**NuGet:**
```bash
dotnet add package Kuestenlogik.Surgewave.Client --version 0.1.0
```

**Container:**
```bash
dotnet publish src/Kuestenlogik.Surgewave.Broker -c Release /t:PublishContainer
```

**Windows MSI (silent install):**
```powershell
msiexec /i surgewave-0.1.0-win-x64.msi /qn
```

**Linux:**
```bash
sudo bash install.sh
```

**Helm:**
```bash
helm install surgewave deploy/helm/surgewave/ --set broker.image.tag=0.1.0
```


**Full Changelog**: https://github.com/Kuestenlogik/Surgewave/commits/v0.1.0

---
