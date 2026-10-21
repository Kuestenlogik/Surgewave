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

## Also in this release

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

## Breaking changes

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
