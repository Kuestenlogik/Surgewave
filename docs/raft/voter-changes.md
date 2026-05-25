# KIP-853 Online Raft Voter Changes — Design Plan

## Status

**Not implemented.** The Kafka wire RPCs (`AddRaftVoter` API 80,
`RemoveRaftVoter` 81, `UpdateRaftVoter` 82) are bound to a
polite-rejection handler in `RaftApiHandler` that returns
`UnsupportedVersion (35)` with a stable, machine-readable message:
*"Online Raft voter reconfiguration (KIP-853) is not implemented in this
Surgewave release. Update the broker's voter configuration and restart
instead."* Pre-validation rejects malformed requests (negative voter id,
empty listener list) with `InvalidRequest (42)` ahead of the
not-supported reply so admin tools see a precise error instead of a
misleading "feature off" message.

This document captures the implementation plan for a future session and
records the building blocks already in place.

## Why we have not shipped this yet

Voter-change protocols are research-grade-correctness exercises. The
canonical references are Diego Ongaro's PhD dissertation (2014) §4.2
single-server changes, and §4.3 joint consensus. Both have shipped with
subtle bugs in production systems despite reviews by Raft specialists:

- **etcd** carried a joint-consensus bug from 2014 to 2018 that allowed
  a split-brain during membership change overlapping with leader change
  (etcd-io/etcd#10165).
- **CockroachDB** found a single-server-change bug in 2019 that admitted
  divergent commits when the new voter was added concurrently with
  partition healing (cockroachdb/cockroach#42686).
- **MongoDB** v4.4 shipped a config-change bug that re-introduced a
  removed voter on the wrong half of a partition (SERVER-49526).

The lesson is that a "first cut" implementation that passes the obvious
tests is not enough — voter changes interact with leader election,
log replication, persistence, and partition-healing in subtle ways
that need a dedicated linearizability suite (Surgewave has
`Kuestenlogik.Surgewave.Testing.Chaos.Linearizability` for exactly this kind of
problem) plus a chaos run.

The polite-rejection shape is the right shipping behaviour until that
work lands. Operators reconfigure the static voter set and restart —
this is the standard ZooKeeper-era operational model and has been
proven safe for a decade.

## Building blocks already in place

### `RaftConfiguration` — `src/Kuestenlogik.Surgewave.Clustering/Raft/RaftConfiguration.cs`

Immutable record capturing the voter set as first-class state:

```csharp
public sealed record RaftConfiguration
{
    public required ImmutableArray<RaftVoter> Voters { get; init; }
    public required long ConfigurationSequence { get; init; }
    public int Majority => (Voters.Length / 2) + 1;

    public RaftConfiguration AddVoter(RaftVoter voter);
    public RaftConfiguration RemoveVoter(int nodeId);
    public RaftConfiguration UpdateVoter(RaftVoter voter);
    public bool ContainsVoter(int nodeId);
}
```

Pinned by `RaftConfigurationTests` (13 tests) covering majority math,
add/remove/update, duplicate-detection, and chained-mutation sequence
monotonicity. The implementation is the rules of single-server-change
in their simplest form — every mutation produces a fresh configuration
with `ConfigurationSequence + 1`. This is the ground truth for the
"only one diff in flight" invariant the leader will eventually enforce.

### `MetadataCommandType.VoterChange` — enum entry reserved at value 9

So a future implementation can replay history through the existing
`RaftNode.ProposeAsync` log machinery without a schema migration.

### Pre-validation in `RaftApiHandler`

Negative voter ids and empty listener lists are rejected with
`InvalidRequest` before the not-supported reply, so the same checks
keep applying once the underlying implementation lands.

## Implementation plan

### Phase 1: persist the voter set (1 day)

1. Add a `_currentConfiguration : RaftConfiguration` field to `RaftNode`,
   initialised from broker startup config.
2. Replace `_transport.GetPeerIds()` callsites with
   `_currentConfiguration.Voters.Select(v => v.NodeId)` so the dynamic
   voter set is the single source of truth at runtime.
3. Persist `_currentConfiguration` to disk (JSON, atomic write) every
   time it changes. Reload on startup. The static config becomes the
   bootstrap value when no on-disk configuration is found.
4. Add a `RaftConfigurationPersistenceTests` suite that round-trips a
   configuration, asserts the on-disk representation is forward-
   compatible (older brokers must be able to read a newer
   configuration), and verifies recovery from a torn write.

### Phase 2: log-replicate voter changes (2 days)

5. New `RaftConfigurationCommand` payload type (Protobuf or JSON,
   following the `MetadataCommandType.TopicCreated` precedent) carrying
   the new `RaftConfiguration`.
6. `RaftNode.ProposeVoterChangeAsync(RaftConfiguration newConfig)`:
   - Validates leader status; returns `NOT_LEADER_FOR_PARTITION` otherwise.
   - Validates the change is single-diff against `_currentConfiguration`
     — this is the safety property that matters most.
   - Validates `newConfig.ConfigurationSequence == _currentConfiguration.ConfigurationSequence + 1`.
   - Calls `ProposeAsync(MetadataCommandType.VoterChange, payload)`
     and waits for commit.
   - On commit, atomically swaps `_currentConfiguration`.
7. Apply on every replica: when `MetadataCommandType.VoterChange` reaches
   the state machine, deserialise the payload and swap the voter set.
8. The crucial single-server-change rule: while a voter-change log entry
   has been proposed but not yet committed, NO new voter-change may be
   proposed. Track this with an `_inFlightConfiguration` field;
   `ProposeVoterChangeAsync` rejects with `CONCURRENT_TRANSACTIONS`
   (or a new `RECONFIGURATION_IN_PROGRESS` error code) until commit.

### Phase 3: catch-up phase for new voters (1 day)

9. KIP-853 says a new voter must catch up to the leader's commit index
   before being added to the voting set, otherwise the new voter could
   prevent quorum until it has replayed the log. Surgewave's path: an
   AddVoter request adds the voter as a non-voting **observer** first,
   the leader replicates log entries as normal, and once the observer's
   match-index reaches the leader's commit-index minus a small buffer,
   a follow-up `VoterChange` log entry promotes the observer to voter.
10. Need a new `RaftRole.Observer` and the matching message-handling
    path so observers participate in `AppendEntries` but not in `Vote`.

### Phase 4: handler wiring (½ day)

11. `RaftApiHandler.HandleAddRaftVoter` calls `RaftNode.ProposeVoterChangeAsync(_currentConfiguration.AddVoter(...))`.
12. Same for Remove / Update.
13. Replace the polite-rejection responses with the real outcomes;
    update `RaftVoterChangeRejectionTests` to be the real
    success-path tests instead of the no-op-pinning tests they are now.

### Phase 5: correctness validation (1-2 days)

14. New `Kuestenlogik.Surgewave.Testing.Chaos` test that:
    - Stands up a 3-node cluster.
    - Issues an AddVoter while a network partition is healing.
    - Asserts via `Linearizability` that no committed entry is lost
      and no two leaders coexist.
15. Same with leader failure during voter-change.
16. Same with split-brain partition.

## Operational migration path

Once Phase 4 lands, the `KIP853NotSupportedMessage` text gets removed
and the wire surface starts succeeding. Existing operators are
unaffected — no broker config changes, no client changes. The CLI
gains a `surgewave raft add-voter / remove-voter / update-voter` command
group.

## Out of scope for KIP-853

- **Joint consensus** (Ongaro §4.3): adds tolerance for changing more
  than one voter at a time, at the cost of a much more complex state
  machine. Single-server change is sufficient for almost every
  operational scenario; joint consensus is a follow-up if real
  customer use-cases demand it.
- **Auto-rebalancing partitions** when a new voter joins: that is a
  partition-assignment concern, not a Raft-membership concern, and is
  tracked separately under the cluster-rebalance work.

## References

- Ongaro, D. (2014). *Consensus: Bridging Theory and Practice* — PhD
  dissertation. §4 covers cluster-membership changes in detail.
- KIP-853: KRaft Reconfiguration —
  https://cwiki.apache.org/confluence/display/KAFKA/KIP-853%3A+KRaft+Reconfiguration
- etcd post-mortem on the 2018 joint-consensus bug: see
  etcd-io/etcd#10165 and the linked design discussion.
