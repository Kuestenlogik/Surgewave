---
title: <what this delivery is about, as a sentence — it is the release's heading, not its version>
version: <X.Y.Z, no leading v>
---

<One-sentence summary of what the release is about. Sets the frame
before Highlights drops into specifics.>

## Highlights

### Metadata is a replicated log, and only that (#163)

The controller no longer pushes partition state to brokers. A leadership
change, an ISR change and a topic creation are committed metadata-log
entries that every broker applies, which is the model Kafka moved to when
it removed `UpdateMetadataRequest` and ZooKeeper in 4.0. A broker now has
a *position* in the metadata, so "is this broker up to date" has an
answer; controller failover and broker recovery resume from that position
instead of rebuilding the whole state.

The cost is stated plainly: a push was one hop and waited for nobody, a
commit waits for a majority. Metadata changes — leader elections, ISR
changes — get slower; the data path is untouched. Recovery and failover
get faster.

### A broker's epoch is its place in the metadata log (#171)

Registration now goes through the metadata log over both wires, so a
broker's epoch is the committed index of its own registration entry —
Kafka's `registerBrokerRecordOffset`. That makes "is this broker up to
date" answerable for every broker rather than only the controller, and
Kafka's unfence rule applies as written: a broker is unfenced once it has
consumed the log up to and including its own registration.

It also closes a gap the Kafka-wire registration API left open. That path
was deliberately un-gated, so any broker answering it wrote the
registration store and could hand out an epoch that no other broker
agreed to. Only the controller can commit a log entry, so the gate is now
structural.

### The metadata quorum can be smaller than the cluster (#167, #168)

Until now every broker voted on metadata, so a metadata write waited for a
majority of *all* brokers and adding brokers made it slower. Two new
settings follow Kafka's shape: `ProcessRoles` (`broker`, `controller`, or
both) and `ControllerQuorumVoters` (`id@host:port`). Nodes outside the
voter list become observers — they receive and serve the metadata log
without voting on it, so the quorum stays the size you chose. See
[Roles and the Controller Quorum](../clustering/raft.md#roles-and-the-controller-quorum).

Combined mode stays the default and nothing changes for a single broker,
an embedded host, or an existing cluster: leave both settings alone and
every node keeps voting exactly as before.

### Transactions V2, end to end with a Java client (#190)

Surgewave announced Kafka's `transaction.version` 2 but did not implement
it; a Java client could not even read the answer to `commitTransaction()`.
Now every EndTxn bumps the producer epoch and returns it, a retry is
recognised by the epoch below, the producer id rotates when the epoch runs
out, and produce v12 / TxnOffsetCommit v5 add the partition or group to the
transaction themselves. EndTxn answers as Kafka does under V1 and V2:
a retried EndTxn of a completed transaction succeeds, an EndTxn while the
markers are written is `CONCURRENT_TRANSACTIONS`, another epoch is
`PRODUCER_FENCED` — also for a commit that raced the coordinator's timeout
abort (KAFKA-20785). The timeout abort fences the producer and is persisted,
and a transaction left prepared by a restart is completed. Checked with the
Java 4.1 client: 2000 records in transactions, all 2000 read back
`read_committed`.

## Fixes

### Java 4.x clients: produce, fetch, groups and admin requests on the wire

Running a current Java client against the broker found:
- **Produce v13 wrote every record into a topic with an empty name** and
  answered success: v13 names the topic by id, and the id was not resolved.
- Finalized features were written min-before-max, so clients read
  `transaction.version` as 0 and `kafka-features.sh` refused the answer.
- Twenty advertised APIs were missing from the flexible-version table, so
  their request header was misread (DescribeTransactions,
  DescribeTopicPartitions, DescribeCluster among them).
- AddPartitionsToTxn v3, Fetch v17/v18 and JoinGroup v8/v9 lacked fields
  or tagged fields the Java client sends or expects.

### Creating a topic while its first produce arrives no longer fails with an I/O error

With auto-create, the topic creation and the first produce opened the same
segment file; on Windows the second open failed and the produce got an
unknown error.

### A broker with `Host` set to an interface address starts

The Connect worker connected to `localhost`, which such a broker does not
listen on, and the broker exited.

### An idempotent producer's records are no longer lost when its first requests race a new partition (#191)

A producer whose requests were reordered while a partition was being
created could get its later batch appended first; every retry of the
earlier batch was then out of order, and its records were lost despite
`enable.idempotence`. On a partition that has never had a record, a
producer without state must now start at sequence 0; any other first
sequence is refused with the retriable `OUT_OF_ORDER_SEQUENCE_NUMBER`, and
the batches land in order. Partitions that have had records accept a
stateless producer at any sequence as before. Kafka made the same change in
KAFKA-15591.

### Broker REST auth: a token's roles claim gives the caller the role

JwtBearer mapped the inbound claims, so `roles` arrived as the long
`ClaimTypes.Role` URI and `RestApiAuth:RolesClaim` never matched: with REST
auth enabled, every mutation was refused with 403, also for callers that
hold `RequiredRole`. The claims are now kept as the token names them.

### One coordinator per consumer group in a cluster (#204)

Every broker answered FindCoordinator with itself and coordinated every
group, so consumers of one group that bootstrapped at different brokers met
in separate copies of it, with separate offsets. A group's coordinator is
now the leader of its `__consumer_offsets` partition, as in Kafka: the
controller creates the compacted topic once the cluster can carry it
(`OffsetsTopicNumPartitions`, `OffsetsTopicReplicationFactor`, capped at the
configured cluster size), FindCoordinator names that leader for the Kafka
and the native protocol alike, and a broker that is not the coordinator
answers the group requests with `NOT_COORDINATOR`. A controller that loses
its leadership while creating a topic no longer reports it as created, and
the next controller assigns the partitions of `__consumer_offsets` that were
left without replicas.

Two Raft fixes came with it. A proposal now replicates at once, each peer on
its own, so a dead broker no longer holds up every replication round and a
controller handling a failure is done in seconds rather than minutes. A
pre-vote whose answers arrive after a leader was elected no longer starts an
election that deposes that leader.

### At most one controller per Raft term (#213)

A Raft node that became follower within its term, either a candidate hearing
from that term's leader or a leader stepping down, forgot the vote it had cast
in the term and could cast a second one. Two brokers could then lead one term,
and one of them reported itself as leader while naming the other. A vote now
holds for its whole term. A leader that hears from another leader of its own
term logs the violation as an error and follows that leader.

### A group keeps its offsets when its coordinator changes (#205)

Committed offsets lived in a file on the broker that coordinated the group,
so a broker that took a group over after a failover knew none of them, and
the group read its partitions again from the start. A group's offsets and
the generation it reached are now records in its partition of
`__consumer_offsets`, in Kafka's OffsetCommit and GroupMetadata shapes, and
compacted. A broker that becomes leader of a partition loads its groups from
the log and answers `COORDINATOR_LOAD_IN_PROGRESS` until it has; one that
loses the leadership lets them go and answers `NOT_COORDINATOR`. A commit is
answered once the in-sync replicas of the partition have it. A broker on its
own keeps `__consumer_offsets` too and leads all of its partitions.

### Consumers of one group share the partitions (#216)

The classic group protocol never rebalanced: a member that joined later was
added to the running generation without anyone being told, so the first
member kept every partition and the later one got none. The coordinator now
runs Kafka's rebalance cycle. A join, a leave or a missed session timeout
starts a rebalance; heartbeats answer `REBALANCE_IN_PROGRESS`; JoinGroup is
held until every member has rejoined or the rebalance timeout has passed;
a follower's SyncGroup waits for the leader's assignment; and a request of
an older generation gets `ILLEGAL_GENERATION`.

### Topics created over Kafka survive a restart of the shipped broker (#212)

The broker host never gave the Kafka CreateTopics handler the cluster
controller, so a topic went into the local log only, past the metadata log,
and was gone after a restart while its data stayed on disk. The host also
never turned committed partition assignments into leaders and followers.
Both are wired as in the embedded runtime.

### The Connect REST routes answer with Connect enabled (#211)

With `Connect:Enabled`, the host and the Connect plugin both mapped the
Connect gRPC service, so every transcoded Connect route matched twice and
answered 500. The host maps it only when the plugin does not.

### Broker shutdown no longer throws from the transaction coordinator (#209)

The container disposes the transaction timeout manager once for each
contract it is registered under; the second dispose threw
`ObjectDisposedException`. Dispose is now idempotent.

### The AI plugin recommendation points at a package that exists (#137)

`surgewave plugins recommend` and the Control banner advertised
`Kuestenlogik.Surgewave.Ai.Basics` from nuget.org, which does not exist.
They now name `Kuestenlogik.Surgewave.AI` from the new default plugin source
`surgewave-ai`, the GitHub releases of the Surgewave.Ai repository.

## Also in this release

### .NET 10.0.14 servicing packages

The Microsoft packages move from 10.0.13 to 10.0.14.
`Microsoft.AspNetCore.Components.WebAssembly.Server` 10.0.13, which the
Control UI uses, has a remote code execution vulnerability
(CVE-2026-102870, GHSA-g27p-qf6j-c96c).

### Everything in English, and a guard that keeps it so (#181)

The remaining German in docs, comments, XML docs, build files, workflows,
installer files and scripts is translated. Visible strings that changed:
the client's retry log message ("Surgewave broker not reachable … attempt
…"), a Control natural-language query suggestion ("Latest 5 messages from
events"), the assistant's help example, and five chart `aria-label`s on
the site. `docs/upstream/kafka-welle-3-low-priority-verify.md` is now
`docs/upstream/kafka-wave-3-low-priority-verify.md`.
`scripts/check-english.mjs` runs in the CI build job and fails with
`file:line` on German outside `.github/language-allowlist.txt`.

### Release pages carry the notes and the change list

A release is named by its version alone, its notes open with the
front-matter `title:` as the heading, and the commits since the previous
tag follow, grouped by kind (`scripts/ci/release-body.mjs`,
`scripts/ci/generate-changelog.mjs`). `CHANGELOG.md` is now a pointer to the
releases. The unused `ModelContextProtocol` 0.2.0-preview.1 pins are gone
from `Directory.Packages.props`.

### One generated logo set, named surgewave_* (#210)

Every logo file in `images/` is now generated by `scripts/branding/generate-logos.mjs`, the generator
Terracle, Modelzero and Sealbolt share, from one drawing, `scripts/branding/surgewave_mark_source.svg`,
configured in `scripts/branding/brand.json`: `surgewave_mark_{colored,black,white,adaptive}`, the lockups
`surgewave_lockup_{h,v}_*` with the wordmark outlined from Inter Bold, `favicon.svg`, `favicon.ico`,
`apple-touch-icon.png` and `og-image`. The generator copies the tab and home-screen icons to the website,
the docs theme and the Control UI, which now shows a tab icon, and `og-image.png` to the website, whose
`og:image` and `twitter:image` it now is (1200×630, with alt text). `images/mark_*` are now
`images/surgewave_mark_*`; the site's copies of the marks and lockups, its fourteen `favicon-<N>.png`, the
old docs `logo.svg`, the designed link-preview card and the scripts `generate-logo-assets.ps1`,
`render-favicons.js`, `build-favicon-ico.js`, `generate-og-image.js` with `og-image-template.html` (and
the `og-image` npm script) are gone.

### The drawing is edited in the Kuestenlogik repository (Kuestenlogik/Kuestenlogik#10)

Branding tool 1.1.0 vendors `brand/products/surgewave` of the Kuestenlogik repository (the drawing,
`brand.json`, `wordmark.json`) into `scripts/branding/` with the tool, and `pin.json` pins their hashes too
(`brand.from`, `brand.files`); `tests/ci/branding-tool.test.mjs` fails when one of them is edited here. A logo
change is made there and brought here with `node tools/branding/vendor.mjs surgewave <checkout>`, then the
generator (`BRAND.md`). The outputs are unchanged.

### The shared branding tool; WebP, package icon and manifest icon (Kuestenlogik/Kuestenlogik#10)

The generator is now the shared tool of every Kuestenlogik product, vendored as a pinned copy in
`scripts/branding/tool/` (`pin.json`: version, commit, each file's hash); `scripts/branding/` keeps only
`brand.json`, `wordmark.json` and the drawing, and `tests/ci/branding-tool.test.mjs` fails when the copy is
edited in place. Every PNG in `images/` now has a lossless WebP twin. The NuGet package icon is
`images/icon.png` (128 px, the mark on the light background, generated like every other logo file); the root
`icon.png` with its black disc, `scripts/icon.svg`, `scripts/generate-icon.js`, `scripts/measure-icon.js`,
`scripts/measure-mark.js` and the `sharp` dev dependency are gone. The Chocolatey `iconUrl` and the MSI
installer's icon use `images/icon.png`; the web manifest's 512 px icon is `icon-512.png` (was
`surgewave_mark_colored.png`). `generate-logos.mjs --check` fails when `images/`, its copies or the brand
files differ from the drawing.

## Breaking changes

### Group offsets are not carried over from an upgrade (#205)

Committed offsets were kept in `.metadata/groups/*.json` under the data
directory. They are now kept in `__consumer_offsets`, and the files are
neither read nor migrated. A group without offsets starts where its
`auto.offset.reset` says. Before 1.0 there is no migration; reset the
offsets of a group that must not start over, after the upgrade.

### `UseRaftConsensus` is gone; there is no non-Raft mode (#163)

Every broker runs on the metadata log. The setting, the
`SurgewaveRuntimeBuilder.WithRaft()` method, and the `UseRaftConsensus`
field of the native `ClusterInfo` payload have all been removed. Remove
the setting from your configuration — it is now ignored, and it named a
mode that no longer exists. A single broker and an embedded host run as a
one-node quorum, which needs no configuration.

### Topic metadata is not carried over from an upgrade (#163)

Topics used to be persisted to `data/.metadata/topics.json` when Raft was
off. The metadata log is now the only source of truth for them, and that
file is no longer read. **A broker upgraded from an earlier version starts
with no topics**; the partition segments stay on disk but nothing refers to
them. Recreate the topics, or start from a clean data directory. There is
deliberately no migration: Surgewave makes no compatibility promise before
1.0.

### The controller role is taken by election, not by convention (#163)

The lowest-id broker no longer becomes controller synchronously at
startup. Raft leadership is the controller role, so a freshly started
broker is not the controller until an election completes — a matter of
milliseconds, but no longer instantaneous. Code that assumed a broker was
the controller immediately after `StartAsync` has to wait for it.

### A cluster of several brokers must declare its controller quorum (#172)

`ClusterNodes` naming other brokers without `ControllerQuorumVoters` is now
a startup error. Brokers that derived the voter set from the brokers they
knew held different sets, and a three-broker cluster could form three
controllers. Name the voters on every broker, as Kafka requires
`controller.quorum.voters`. A lone broker still needs no declaration. An
embedded cluster states both with `SurgewaveRuntimeBuilder.WithControllerQuorum`
and `WithProcessRoles`.

## Acknowledgements

<Optional. Short. Names contributors who exercised the rc series or
reported the bugs that landed as fixes.>
