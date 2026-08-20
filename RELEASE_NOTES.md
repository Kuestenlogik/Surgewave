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
## v0.5.0 — 2026-08-20 — AI-friendly broker

0.5 makes Surgewave the natural home for embedding-heavy AI pipelines: schemas understand
vectors and name exactly what an incompatible change breaks, and pipelines become
reviewable, testable C# code — deployable from a compiled library without a running broker.

## Highlights

<!-- Add a section per landed feature as the work merges. Pattern:
### <headline> (#issue)
<2-4 sentences>
-->

### Schema changes name what they break (#13)

Schema evolution used to answer "incompatible" and leave you to guess who cares. The registry
now walks the broker's live control-plane lineage — consumer groups from committed offsets,
Streams applications from their submitted topologies, Connect pipelines with their sink
topics — and a rejected registration (409) names the affected pipelines and the topics
transitively going stale behind them. `POST /subjects/{subject}/impact` runs the same
analysis without registering, as a CI / pre-deploy check (`compatible: false` fails the
build); `?force=true` on the register call is the emergency override — it skips the
compatibility gate, still validates the schema format, and writes the impact to the broker
log as a warning.

Nothing touches the produce/fetch hot path: lineage is assembled on demand from state the
control plane already holds, and a missing or failing lineage source only shortens the name
list — the compatibility verdict never depends on it.

### Vectors are a first-class schema primitive (#14)

Embeddings now declare their shape in the schema instead of hiding in an opaque array: Avro
carries `"logicalType": "vector", "dim": 768` on an array of float/double, JSON Schema uses
`"format": "vector"` with `x-vector-dim`/`x-vector-dtype`, and Protobuf annotates a repeated
float/double field with `[(surgewave.vector).dim = 768]`. The registry validates the
declaration at registration (42201 on a missing or non-positive dim, an unknown dtype, or the
wrong underlying type) and enforces evolution: a vector field's dim and dtype must never
change, and vector-ness must not appear on or disappear from an existing field — rejected as
incompatible (409) in every mode, in both directions. That strictness is the point: 768→1536
passes every classic type check and then silently corrupts every consumer that indexes or
allocates against the declared dimension.

Producers declare it once on the .NET type — `[SurgewaveVector(768)] float[] Embedding` (in
`Kuestenlogik.Surgewave.Schema.Registry.Client`) — and the Avro and JSON serdes stamp the
annotation into the generated schema automatically.

### Pipeline as code: a C# DSL next to the visual editor (#12)

Connect pipelines can now be written as C# — `Pipeline.From<OrderEvent>("orders")
.Filter(o => o.Amount > 1000).Map(...).To("orders-high-value")` — in the new
`Kuestenlogik.Surgewave.Pipelines` package. Predicates translate into the broker's condition
syntax at build time (`&&` becomes chained filter nodes), building needs no running broker,
and the result is a plain definition you can unit-test and export as deterministic,
editor-compatible JSON that diffs cleanly in git. Deployment goes through the new
`surgewave pipelines` CLI group — `deploy` takes an export file, a compiled library (every
`ISurgewavePipeline` implementation is discovered), or a project directory, and `--watch`
rebuilds and redeploys on save — or programmatically through `PipelinePublisher`
(create or replace-by-name, optional start). The visual editor stays for prototyping;
DSL-built pipelines open there with auto-assigned layout.

Two runtime gaps this surfaced are fixed as well: the orchestrator now wires processor
nodes (Filter, Map, If, …) to their pipelines' internal connection topics — previously only
source and sink connectors were wired, so processor chains needed hand-written topic configs —
and the worker now actually produces the records processor nodes emit, which outside dry-run
were buffered and dropped. Explicit topic configs always win over the automatic wiring. The
pipeline export format additionally carries parameters, schedule, per-node retry policies,
and connection types — error-routing edges used to flatten to normal data edges on
export/import (older exports import unchanged).

## Fixes

### Processor pipelines: exactly-once output, timer emissions, and real error routing (#148, #149)

Three follow-ups to the pipeline-as-code work close the remaining gaps in the processor
runtime. Under `exactly.once`, processor nodes now produce their output inside the batch
transaction — output and consumed offsets commit atomically; previously the transactional
sink runner never drained the emit buffer at all, so an exactly-once processor pipeline
produced nothing. Timer-driven emissions (a Deduplicate window expiring with
`dedup.strategy=last`, Retry backoff firing) now flush on idle consumer ticks instead of
waiting for the next input record on a possibly quiet stream. And a record that fails JSON
parsing in a transform node is now reported to the node's wired error output and lands in
the dead-letter topic — before, error edges on Filter/Map/Cast/… never received anything
because parse failures were dropped silently. Without an error connection the behavior is
unchanged (silent drop), so existing pipelines keep their semantics.

### A multi-batch produce section is refused as `InvalidRecord`, not `CorruptMessage` (#125)

A produce request carries exactly one record batch per partition — Kafka enforces it at parse time
and its own producer cannot build anything else. Surgewave already refused such a section, but by
accident: it reached the append, whose CRC is computed over the whole concatenation and cannot
match the first batch's CRC field, so the answer was `CorruptMessage` (2). That blames transport
for a request the protocol does not permit, and it sent anyone debugging it looking for a network
fault that was not there.

The refusal is now explicit and carries Kafka's own code, `InvalidRecord` (87). A section too short
to hold a batch header, or one whose first batch overruns it, is refused the same way instead of
surfacing as a generic `Unknown`. Nothing about accepted traffic changes: the check is two integer
reads on the produce path, and for a conforming single-batch request the answer is immediate.

## Breaking changes

### Storage-engine packages no longer sit on top of the broker

`Kuestenlogik.Surgewave.Storage.Engine.{Lmdb,RocksDb,S3,Sqlite}` each referenced `Runtime` for
one fluent extension file — which placed a 4-file storage engine *above* the entire broker in
the dependency graph. The engines now bind against `IStorageConfigurableBuilder` (new, in
`Storage.Engine`), which `SurgewaveRuntimeBuilder` implements. Call sites are source-compatible
— `builder.WithLmdbStorage()` reads exactly as before — but the extension methods are now
generic, so code compiled against the old packages must recompile. The engines' own closure
shrinks from ~43 projects to their storage dependencies.

Related fixes in the same sweep: the `Kuestenlogik.Surgewave.Sdk` meta-package now actually
delivers the `.swpkg` build tasks transitively (`buildTransitive/` was missing from the Build
package, so the tasks never imported through the meta-package) and no longer pulls
`Testing` — and with it the whole embedded broker — into every plugin's compile closure; add
`Kuestenlogik.Surgewave.Testing` to your test project instead. The four schema-handler
packages (Avro/FlatBuffers/Json/Protobuf) each declared an identical
`Handlers.ServiceCollectionExtensions` class, so in a host loading several of them a
fully-qualified call was resolved by assembly order; they now follow the
`{Format}ServiceCollectionExtensions` naming the other handlers already used. Extension-method
call sites are unaffected.

### The broker is now a library; the server moved to `Broker.App`

`Kuestenlogik.Surgewave.Broker` used to be the runnable server *and* the engine in one
executable project — and because the published `Runtime` and `Hosting` packages reference it,
every embedded-broker consumer transitively compiled against the whole host: ASP.NET REST APIs,
gRPC server, OpenTelemetry, JWT auth, Bowire, and ~40 projects behind them.

The project is now split along the framework line. `Kuestenlogik.Surgewave.Broker` is a plain
library holding the engine — topics, partitions, coordinators, quotas, dedup, TTL/delay, the
native control plane — with no ASP.NET, gRPC, or OTel anywhere in its closure. The server lives
in the new unpackaged host project `src/Kuestenlogik.Surgewave.Broker.App` (`Program.cs`, REST
APIs, startup wiring, the built-in AutoTuning/CruiseControl/AdaptiveCompression plugins), still
building the same `surgewave-broker` binary and container image.

What changes for consumers of the NuGet package: the assembly inside
`Kuestenlogik.Surgewave.Broker` is now named `Kuestenlogik.Surgewave.Broker.dll` instead of
`surgewave-broker.dll`, and the host-only types (REST APIs, `Program`) are no longer in it.
Namespaces are unchanged — code that embeds the broker through `Runtime`/`Hosting` recompiles
without edits. What changes for operators: `dotnet run --project src/Kuestenlogik.Surgewave.Broker`
becomes `dotnet run --project src/Kuestenlogik.Surgewave.Broker.App`; published artifacts,
binary name, ports, and configuration are identical.

### `SnapshotNotFound` moves from error code 87 to 98 (wire-visible)

Surgewave's `SnapshotNotFound` was numbered 87, which is Kafka's `INVALID_RECORD`; the real
`SNAPSHOT_NOT_FOUND` is 98 and was unused. The Raft snapshot-fetch path put 87 on the wire, so a
Kafka-compatible client decoded "this record failed validation on the broker" from a response about
a missing snapshot.

`SnapshotNotFound` is now 98 and `InvalidRecord` occupies 87, matching
`org.apache.kafka.common.protocol.Errors`.

**Rolling upgrades:** brokers of mixed versions disagree about what 87 means on the Raft
fetch-snapshot path for the duration of the roll. The exchange is broker-to-broker and the code is
informational there — a follower that cannot get a snapshot retries either way — so the practical
effect is a misleading log line during the window, not a stall. Upgrading all brokers in one roll
avoids it entirely.

### Kafka produce now validates the producer CRC instead of overwriting it (#85)

The broker used to recompute every incoming batch's CRC-32C and overwrite the field, which
silently healed corrupt bytes into the log. Produce over the Kafka wire now validates the CRC the
producer sent and answers a mismatch with `CorruptMessage` (error code 2), the same as Kafka.

This costs nothing: it is the same single pass the append already made, plus a four-byte compare.
Real clients (librdkafka, the Java client, Confluent.Kafka) all write a correct CRC32C and are
unaffected. Hand-rolled clients that send a zero or stale CRC and relied on the broker fixing it
will now get `CorruptMessage`.

Note that a produce request must carry one record batch per partition, not several concatenated
ones — the broker has always assumed this when parsing the batch header, and validation now makes
it visible. Every mainstream client sends a single batch.

### `NativeCompressionCodec.CompressWithHeader` replaced by `TryCompressWithHeader` (#86)

The old method allocated up to three arrays per compressed frame and copied the
payload even when compression was rejected. It is replaced by

```csharp
bool TryCompressWithHeader(ReadOnlySpan<byte> data, out byte[]? pooledBuffer, out int frameLength)
```

which compresses into a single pooled buffer and rents nothing at all when the
payload is incompressible. Migration: on `true`, the frame is
`pooledBuffer[0..frameLength)` and **you own the rent** — return it to
`ArrayPool<byte>.Shared` once the bytes are on the wire (a `finally` around the
write). On `false`, send the original payload unchanged; `pooledBuffer` is null.
The wire format is byte-identical, so old and new peers interoperate.

## Acknowledgements

<!-- Optional. -->

<details>
<summary><b>Installation</b> — NuGet, Container, MSI, Linux, Helm</summary>

**NuGet:**
```bash
dotnet add package Kuestenlogik.Surgewave.Client --version 0.5.0
```

**Container — pre-built (Docker Hub, multi-arch linux/amd64 + arm64):**
```bash
docker pull kuestenlogik/surgewave-broker:0.5.0
docker pull kuestenlogik/surgewave-control:0.5.0
# …oder :latest
```

**Container — alternative registry (GHCR):**
```bash
docker pull ghcr.io/kuestenlogik/surgewave-broker:0.5.0
docker pull ghcr.io/kuestenlogik/surgewave-control:0.5.0
```

**Container — build from source:**
```bash
dotnet publish src/Kuestenlogik.Surgewave.Broker.App -c Release /t:PublishContainer
dotnet publish src/Kuestenlogik.Surgewave.Control -c Release /t:PublishContainer
```

**Windows MSI (silent install):**
```powershell
msiexec /i surgewave-0.5.0-win-x64.msi /qn
```

**Linux:**
```bash
sudo bash install.sh
```

**Helm:**
```bash
helm install surgewave deploy/helm/surgewave/ --set broker.image.tag=0.5.0
```

</details>

---

_The full commit list, contributors, and compare-URL diff are auto-generated below._



## What's Changed
* test: coverage push for the weakest assemblies by @thomas-stegemann in https://github.com/Kuestenlogik/Surgewave/pull/90
* perf: hot-path allocation and copy removal (#79, #83, #84, #85, #86) by @thomas-stegemann in https://github.com/Kuestenlogik/Surgewave/pull/91
* chore: bump floor to 0.4.0-dev (after v0.4.0) by @thomas-stegemann in https://github.com/Kuestenlogik/Surgewave/pull/89
* ci(benchmarks): fix the regression gate — report, speed, NA-robustness by @thomas-stegemann in https://github.com/Kuestenlogik/Surgewave/pull/95
* fix(clustering): split concatenated records section on follower ingest (#92, #93) by @thomas-stegemann in https://github.com/Kuestenlogik/Surgewave/pull/96
* fix(storage): DATA LOSS — closed-segment reads return empty on rolled logs (#99) by @thomas-stegemann in https://github.com/Kuestenlogik/Surgewave/pull/100
* perf(fetch): record set as ReadOnlyMemory — one copy fewer per partition (#78 stage 1) by @thomas-stegemann in https://github.com/Kuestenlogik/Surgewave/pull/98
* perf(client): decode batch once per fetch, drop per-produce alloc (#80 C1+P1) by @thomas-stegemann in https://github.com/Kuestenlogik/Surgewave/pull/101
* perf(replication): leader fetch reads contiguous — drop List<byte[]> + per-batch copies (#82 S3) by @thomas-stegemann in https://github.com/Kuestenlogik/Surgewave/pull/103
* perf(replication): pool the leader response frame from ArrayPool (#82 S1) by @thomas-stegemann in https://github.com/Kuestenlogik/Surgewave/pull/104
* perf(replication): pool the follower fetch-response body from ArrayPool (#82 S2) by @thomas-stegemann in https://github.com/Kuestenlogik/Surgewave/pull/105
* ci: keep the ci package version valid when the short sha is all-numeric by @thomas-stegemann in https://github.com/Kuestenlogik/Surgewave/pull/107
* perf(replication): follower appends fetched records by slice, no per-partition copy (#82 S4) by @thomas-stegemann in https://github.com/Kuestenlogik/Surgewave/pull/106
* test(replication): end-to-end allocation proof for the #82 pooled fetch path by @thomas-stegemann in https://github.com/Kuestenlogik/Surgewave/pull/109
* perf(replication): two-pass exact-size fetch-request serialization (#82 S5) by @thomas-stegemann in https://github.com/Kuestenlogik/Surgewave/pull/108
* test(replication): make the #82 fetch-allocation ceiling robust under coverage by @thomas-stegemann in https://github.com/Kuestenlogik/Surgewave/pull/111
* bench(replication): gated MemoryDiagnoser bench for the #82 follower split-append by @thomas-stegemann in https://github.com/Kuestenlogik/Surgewave/pull/110
* chore(core): remove the dead SimdVarIntScanner and SimdBufferCopy helpers (#85 S3) by @thomas-stegemann in https://github.com/Kuestenlogik/Surgewave/pull/113
* perf(core): 3-way interleaved CRC32C for large buffers (#85 S2) by @thomas-stegemann in https://github.com/Kuestenlogik/Surgewave/pull/114
* chore(deps): bump the actions group across 1 directory with 2 updates by @dependabot[bot] in https://github.com/Kuestenlogik/Surgewave/pull/112
* fix(clustering): a dead leader no longer stalls replication from the healthy ones by @thomas-stegemann in https://github.com/Kuestenlogik/Surgewave/pull/141
* fix(clustering): acks=all waits for replication instead of reporting the leader append by @thomas-stegemann in https://github.com/Kuestenlogik/Surgewave/pull/140
* ci(coverage): exclude IntegrationTests by project, not by test name by @thomas-stegemann in https://github.com/Kuestenlogik/Surgewave/pull/139
* ci: split test assemblies into a parallel and a serial lane by @thomas-stegemann in https://github.com/Kuestenlogik/Surgewave/pull/138
* chore(deps): bump actions/cache from 4 to 6 in the actions group by @dependabot[bot] in https://github.com/Kuestenlogik/Surgewave/pull/142


**Full Changelog**: https://github.com/Kuestenlogik/Surgewave/compare/v0.4.0...v0.5.0

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
