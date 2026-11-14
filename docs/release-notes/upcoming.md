---
title: <what this delivery is about, as a sentence — it is the release's heading, not its version>
version: <X.Y.Z, no leading v>
---

<One-sentence summary of what the release is about. Sets the frame
before Highlights drops into specifics.>

## Highlights

### <Headline of the first major change> (#issue)

<2-4 sentences. What changed, why it matters to the user, where to
look in the docs / settings.>

### <Headline of the second major change> (#issue)

<...>

## Fixes

### The replication listener binds the address you name (#285)

`Surgewave:ReplicationBindAddress` says which address the replication listener binds. Unset it is every IPv4
interface, as before, which is what a cluster on several hosts needs. A broker whose peers all run on its own
host sets `127.0.0.1` and is then not reachable on that port from outside. The embedded broker
(`SurgewaveRuntime`) does this by itself whenever its client listener is on loopback, so a cluster started in a
test or on a developer's machine no longer listens on every interface, which on Windows raised a firewall prompt
for each new checkout.

### Acknowledged metadata survives a power loss (#260, #261, #262)

The metadata log and the Raft state file were written and left in the operating system's cache. A topic a client
had been told was created survived the broker process and not a power loss, and neither did the vote a controller
had cast. Both are now flushed to disk before they count: a follower answers the leader once the entries are on its
disk, a vote is answered once it is, and a leader counts and sends only entries it has on its own disk. Writes that
arrive while a flush is under way share the next one, so a burst costs one flush rather than one each. Measured on
an NVMe disk under Windows, one metadata change written alone takes about 3.3 ms instead of 0.7 ms, and 5000 made
at once take the same time as before; the data path is not touched.

Every log entry carries a checksum. A write the last run did not complete is cut off the end of the log when the
broker starts, with a warning that says how many bytes. A log that is damaged anywhere else, a state file or a
snapshot that cannot be read is an error that names the file, and the broker does not start: it used to skip the
entry, or start with term 0 and no vote, on which a node votes a second time in a term it has voted in. Nothing is
repaired by itself.

Proposals made at the same time could reach the log file in the opposite order to their indexes. They are written
in the order of their indexes now.

### Live brokers are no longer reported failed while one peer does not answer (#251)

A broker sent its heartbeats in rounds and began the next round when every peer had answered or failed. One peer
that did not answer — a host that drops packets instead of refusing the connection, a broker that accepts and stays
silent — held the round, and the peers that had answered got their next heartbeat later than the timeout they are
given. They were reported failed, and the controller took live brokers out of the ISRs and moved their
leaderships. Each peer now has its own heartbeat; no setting changes.

### A new controller handles the brokers that failed before it took office (#252)

A broker failure is reported once, and only the controller acts on it. A broker that noticed the old controller's
failure before it was elected itself had let the report go by then, and the dead broker stayed in every ISR and
kept its leaderships. A controller now looks at the peers it holds as failed when it takes office.

### A broker runs one controller loop, and only while it is the controller (#250)

The loop that looks at the leader balance and the cluster balance was started with each election a broker won and
never stopped when it lost the office, so a broker ran one more with every re-election, and went on rebalancing as
a controller it no longer was.

### A fetch sent to a broker that does not lead the partition is refused (#287)

A Kafka Fetch for a partition another broker leads was answered from whatever the asked broker held of it —
nothing, where it has no replica — and without an error, so a consumer with stale metadata went on polling that
broker with no end. It is answered `NOT_LEADER_OR_FOLLOWER` now, as a produce is, and the consumer refreshes its
metadata. The same answer goes to a produce and a fetch for a partition that has no leader.

### `surgewave cluster` and `/v3/metadata` describe the cluster, whichever broker is asked (#272)

`surgewave cluster status`, `cluster nodes` and `cluster partitions` showed the broker they asked as the
controller, the only node, and the leader and only replica of every partition; a topic the broker held no replica
of did not exist for it. gRPC `GetMetadata` (`/v3/metadata`) and `GetClusterInfo` (`/v3/cluster`) did the same,
under a cluster id made of the broker's own. They answer from the cluster's state now, as the Kafka Metadata does:
the controller and the id of the cluster, every broker with the health the asked broker holds it in, and each
partition's leader, replicas and in-sync replicas. A broker that runs alone answers as before.

### Native error answers say what went wrong (#290)

A native request whose payload the broker could not read was answered `InternalError`, as if the broker had
failed, and logged as an error with a stack trace — once per request, so a client could fill the broker's log.
It is answered `InvalidRequest` with what could not be read, and logged at debug level. A topic creation no
controller took, as while the office changes hands, was `InternalError` too; it is answered with the new error
code `NotController` (23), which says that nothing was done and asking again may work. A client that does not
know the code reads it as an unknown error.

### The native client notices a broker that is gone, and its consumers end with the connection (#289)

A broker that closes its connection says so. A host that loses power, or a network that drops its packets, says
nothing, and the native client waited on such a connection for as long as it ran. Its connections are now kept
alive: over TCP the operating system probes a connection that carries nothing, over QUIC the connection has an
idle timeout and is kept alive while the broker answers. With the defaults a broker that vanished is noticed
after about 30 seconds; `TransportOptions.KeepAliveTime`, `KeepAliveInterval` and `KeepAliveRetryCount` set the
times, and `KeepAliveTime = Timeout.InfiniteTimeSpan` turns it off.

`SurgewavePrefetchingConsumer` retried every failed fetch every 100 ms, also on a connection that was lost, and
`ConsumeAsync` then waited for a message with no end. It hands out what it had fetched and then fails with the
`IOException` the connection ended with. A streaming consumer no longer drops the records the broker pushes
before the answer to its Subscribe is read. And with pipelining off, a request cancelled while its answer was
awaited left that answer for the next request, which failed with "Request ID mismatch" or waited behind it; the
connection is closed instead, and the next request says so.

## Breaking changes

### `ISurgewaveTransport.ConnectionEnded` has no default (#289)

A transport says when its connection ends: the member had a default that never completed, on which a streaming
consumer waited on a lost connection for as long as it lived. A transport outside this repository implements
`Task<Exception> ConnectionEnded`, completed with what ended the connection.

`SurgewavePrefetchingConsumer.Consume`, `ConsumeAsync`, `ConsumeBatch` and `ConsumeBatchAsync` throw the
`IOException` the connection ended with once it is lost and what was fetched has been read. They returned `null`
or did not return.

### A partition whose last in-sync replica fails has no leader (#253)

When the only in-sync replica of a partition failed, the controller emptied the ISR and elected any live replica.
That replica was not in sync, so what the failed leader had acknowledged and it had not fetched was lost — while
the broker reported `unclean.leader.election.enable=false`. The controller now does what that setting says, and
what Kafka does: the failed broker stays in the ISR, the partition has no leader, and Metadata answers
`LEADER_NOT_AVAILABLE` for it until the broker is back, when it is elected again. A cluster that relied on the
old behaviour to stay available sets `unclean.leader.election.enable=true` on the topic, or sends an `ElectLeaders`
request of the unclean type for the partitions it wants back at once; see
[Failover](../clustering/failover.md). A preferred `ElectLeaders`, a rebalance and a reassignment no longer fall
back to a replica that is not in sync.

"No leader" is a `LeaderChanged` entry with leader `-1` in the metadata log, which a 0.6.0 broker reads as a
request to elect the first ISR member; a cluster is upgraded as a whole.

### The metadata log has a new format on disk (#261)

`raft-log.bin` starts with the name of its format and holds a length and a checksum before each entry. A log
written by 0.6.0 has neither and is refused at start with an error that says so; it is not converted. A cluster
keeps its metadata only if it is created anew with this release. `raft-state.json` is unchanged.

`RaftPersistence.AppendEntriesAsync` and `TruncateLogAsync` no longer take a cancellation token: the write is
queued when the call returns and completes when it is on disk.

## Acknowledgements

<Optional. Short. Names contributors who exercised the rc series or
reported the bugs that landed as fixes.>
