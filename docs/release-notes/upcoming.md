---
title: <short theme — keep it under 60 chars; matches the milestone title tail>
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

### <Headline of the second major change> (#issue)

<...>

## Fixes

### <Grouped fix headline> (#issue, #issue)

<1-3 sentences per group.>

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

## Acknowledgements

<Optional. Short. Names contributors who exercised the rc series or
reported the bugs that landed as fixes.>
