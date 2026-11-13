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
## v0.6.0 — 2026-11-13 — 0.6.0

# One metadata log, and a cluster that agrees with itself

0.6.0 is about a cluster being right rather than fast. Metadata is now one replicated log that
every broker applies, so leadership, topics, the cluster id and a broker's own epoch have a
single answer. Around that, the release closes gaps that showed only with real clients and
real failures: Java 4.x clients and transactions V2 end to end, a broker whose disk fails
handing its partitions over, and a Kafka wire client that produces and consumes. It carries
breaking changes; an existing cluster is upgraded as a whole, not broker by broker.

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

### A broker whose disk fails hands its partitions over (#162)

A broker with a failing disk kept heartbeating, so session expiry, the
caught-up check and the failure detector all saw a healthy broker, and it
kept its leaderships while every produce and fetch against it failed. Now
an I/O failure a partition log cannot recover from — a segment that cannot
be written, read, rolled, truncated or opened — takes the broker's log
directory offline, as Kafka's `LogDirFailureChannel` does. The broker stops
serving its partitions (`KAFKA_STORAGE_ERROR` on the Kafka wire, `NotLeader`
on the native one), stops replicating into the directory, and reports it in
its next heartbeat, sent at once: Kafka's `OfflineLogDirs` (KIP-858), which
the native heartbeat now carries too, so both wires report it alike. The
controller moves every leadership off the broker to an in-sync replica,
takes it out of the ISRs, and neither elects it nor lets it back into an
ISR while the directory is offline — what Kafka's controller does with the
same field. A partition with no other in-sync replica stays where it is
rather than moving uncleanly.

A log directory has a stable id now, kept in `meta.properties` under
`directory.id` as in Kafka. A directory that has failed stays offline
until the broker restarts.

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

### A partition leader reports an ISR change until the controller has it (#230)

A leader added a caught-up follower to its own view of the ISR first. It
then reported the change to the controller once, without waiting for an
answer. A lost report was never sent again: right after a cluster formed,
when no controller was known yet, or with the controller unreachable. The
controller then kept the leader alone in the ISR, and a controlled shutdown
of that leader found no in-sync successor and elected uncleanly. Seen on the
coordinators' internal topics. A leader now reports again on the next fetch
report or ISR check until the controller has taken the change.

### At most one controller per Raft term (#213)

A Raft node that became follower within its term, either a candidate hearing
from that term's leader or a leader stepping down, forgot the vote it had cast
in the term and could cast a second one. Two brokers could then lead one term,
and one of them reported itself as leader while naming the other. A vote now
holds for its whole term. A leader that hears from another leader of its own
term logs the violation as an error and follows that leader.

### The gRPC and REST offset commit and fetch go through the group's coordinator (#221)

`ConsumerService.Commit` and `FetchOffsets` (and their REST routes) wrote
and read the offset store directly, past the group's coordinator: no
member or generation check, and on a broker that does not coordinate the
group only a retriable error, with nothing saying where the coordinator
is. They now go through the coordinator as Kafka's OffsetCommit and
OffsetFetch do. A broker that does not coordinate the group answers
`NOT_COORDINATOR` and names the coordinator in the response's new
`coordinator` field. A commit can carry `member_id` and `generation_id`,
which the coordinator checks; without a generation it is a simple
consumer's commit. A coordinator still loading the group answers the new
`COORDINATOR_LOAD_IN_PROGRESS`.

### The shipped broker refuses a produce for a partition another broker leads (#227)

The embedded runtime answered a produce for a partition another broker
leads with `NOT_LEADER_OR_FOLLOWER` since #164, so a client with stale
metadata refreshes it. The shipped broker never wired that check and
appended the records to a follower's copy, where nobody would read them.
It now answers as the runtime does, over the Kafka and the native
protocol.

### A topic is created whole or not at all (#217)

Creating a topic wrote the topic and then the replicas of each partition as
separate entries of the metadata log. A controller that lost its leadership
between them left a topic whose partitions no broker led, and a retried
CreateTopics was answered `TOPIC_ALREADY_EXISTS`. One entry now carries the
topic with the replicas of every partition: it commits whole, or the topic
is not there and the retry creates it.

### CreateTopics honours a replica assignment (#202)

A CreateTopics request that named each partition's replicas, as
`kafka-topics --create --replica-assignment 3:1,1:3` sends it, got the
controller's own placement and no error: the assignment was read, counted
for the partition limit and dropped. The topic now has the replicas as
asked, in their order, the first the leader. The assignment is checked as
Kafka's controller checks it, with Kafka's codes and messages:
`INVALID_REQUEST` when NumPartitions or ReplicationFactor is not -1 beside
it; `INVALID_REPLICA_ASSIGNMENT` for a partition assigned twice, an empty
replica list, a broker twice in one partition, partitions with different
replication factors, partition indexes not numbered from 0 on, or a broker
the cluster has not registered.

### `kafka-topics --describe` works (#240)

`kafka-topics --describe` failed against a Surgewave cluster, for two
reasons:

- **APIs advertised and not answered:** the embedded broker
  (`SurgewaveRuntime`) advertised the cluster admin APIs — ElectLeaders,
  AlterPartitionReassignments, ListPartitionReassignments — and the quota
  and delegation token APIs, but had no handler for them, and answered
  with a frame that holds no body. The tool lists the partition
  reassignments of the topic it describes and stopped with "Buffer
  underflow while parsing response". The embedded broker now answers
  them as the broker host does, with or without a cluster, and a test
  holds that every API it advertises has a handler.
- **DescribeConfigs on a broker without a replica of the topic:** the
  tool asks any broker for the topic's configs, and a broker that hosts
  none of its replicas answered `UNKNOWN_TOPIC_OR_PARTITION`. It now
  describes every topic of the cluster, from the metadata log.

The ListPartitionReassignments and AlterPartitionReassignments requests
the Kafka wire client writes carried the client id as a compact string;
it is a classic string in every request header version.

### A cluster that lost one of three voters elects a leader at once (#225)

An election waited for every voter's answer, the answer of a stopped broker
included, which came only once its connect attempts gave up, after
seconds. The two voters still up campaigned in step, each voting for
itself, round after round: after a broker stopped, the cluster could stay
without a controller for minutes. An election is now decided as the answers
arrive: on a majority, once a majority can no longer be reached, or on a
newer term.

### Producer ids are unique in the cluster and over restarts (#223)

Every broker handed out producer ids from a counter that started at 1 on each
broker and at every start. Producers on two brokers, or before and after a
restart, could share an id, and a partition leader then mixed their sequences.
Ids are now `brokerId · 2^40 + n`, and `n` comes from blocks of 1000. A broker
writes each block to its data directory before it uses an id from it.

### A broker that did not stop cleanly keeps its records (#222)

A file segment reopened after a broker was killed or crashed took its end
from the offset index, which is written behind the log and flushed in
batches. A segment whose index had not been flushed came back empty: its
records could no longer be read, and new appends were given their offsets
again. On reopen the log now decides. Every complete batch after the last
indexed one is indexed, a batch cut short by the stop is truncated, and the
segment ends where its last complete batch ends.

### A transactional id has one coordinator, and its state outlives it (#197)

Every broker coordinated every transactional id it was asked about, from a
file of its own. A producer that came back after a failover met a broker
that knew nothing of its id, and the producer that had used the id before
was not fenced. Kafka puts transaction state in `__transaction_state`, and
so does Surgewave now:

- **One coordinator per id:** the controller creates the compacted topic
  (`TransactionStateLogNumPartitions`, `TransactionStateLogReplicationFactor`),
  and FindCoordinator(TRANSACTION) names the leader of the id's partition.
- **Durable before the answer:** a transaction's state is a record in that
  partition, and a change is answered once the in-sync replicas have it.
- **Taking over:** a broker that takes the partition over reads its
  transactions from the log. It fences the previous producer and completes
  what was left prepared.
- **Errors:** a broker that does not coordinate the id answers
  `NOT_COORDINATOR` (KAFKA-21181), and one still reading the partition
  answers `COORDINATOR_LOAD_IN_PROGRESS`.

### A transaction spans partitions led by any broker (#197)

A transaction could only write to partitions its coordinator led. A
partition's leader asked its own broker about the transaction, was told
`NOT_COORDINATOR`, and the batch was never appended. Now, as in Kafka:

- **Verification through the coordinator:** the leader asks the
  transactional id's coordinator over the inter-broker channel. Kafka does
  this with AddPartitionsToTxn v4+. Under transactions V1 the leader checks
  that the partition was added; under V2 the coordinator adds it.
- **Markers on the leaders:** each commit or abort marker is written by the
  partition's leader and counts once its in-sync replicas have it. The
  coordinator used to write every marker into its own log, which for a
  partition led elsewhere was only a follower's copy, and send the markers
  on best-effort.
- **Complete once every marker is written:** a transaction whose leader
  does not take its marker stays prepared. The coordinator writes the
  missing markers on its next pass, and the producer is answered
  `CONCURRENT_TRANSACTIONS` meanwhile. EndTxn is still answered once the
  outcome is durable.
- **Offsets through the group's coordinator:** TxnOffsetCommit writes the
  offsets into the group's partition of `__consumer_offsets` as a batch of
  the transaction. The offsets count once the transaction's commit marker
  lands there, and an abort marker drops them. AddOffsetsToTxn puts that
  partition into the transaction, as Kafka does. The offsets used to be
  staged with the transaction's coordinator and written into its own store,
  so a group coordinated elsewhere never saw them.

### A topic created through any broker is a topic of the cluster (#207, #208)

A broker that was not the controller created a topic, from CreateTopics or
from a Metadata request that auto-creates, on itself alone. There was no
metadata log entry and there were no replicas, and the client was told it
worked. It now hands the creation to the controller over the inter-broker
channel. Until the topic arrives through the metadata log, the client gets a
retriable error: `NOT_CONTROLLER` for CreateTopics, `LEADER_NOT_AVAILABLE`
for Metadata. Metadata also takes a topic's partitions from the cluster's
view. A broker hosting one partition of two used to report a topic of one
partition, and a group leader that asked it assigned only that one.

### A topic created through any entry point survives a restart (#214)

Only Kafka's CreateTopics and auto-creation went through the controller.
Topics created over gRPC (and REST, MCP and the CLI on top of it), the
native protocol, GraphQL and the intent API, and the internal topics of
DLQs, audit, telemetry, CDC, KV buckets, queues and mirror topics, were
created in the local log manager only. They were missing from the metadata
log and gone after a restart. Every creation now goes through the
controller into the metadata log, whatever the entry point, and completes
once the topic has reached the broker. A topic that no controller takes now
is a retriable error (`LEADER_NOT_AVAILABLE` over gRPC). A topic's
configuration from the metadata log now reaches the brokers that host its
replicas, at creation and on every restart. A topic created through the
controller used to run there with the broker's defaults for retention,
cleanup policy and segment size. A broker that has not yet applied a topic
the cluster already has no longer creates it again under a new topic id.
Kafka's CreateTopics and auto-creation now go the same way and wait for
the topic to arrive. Before, auto-creation answered `LEADER_NOT_AVAILABLE`
until the topic arrived.

### A broker just started takes topic creations before its first election (#235)

A topic creation asked of a broker before its first election, or before it had registered itself, found no
controller and failed at once. The Connect worker creating its topics at the broker's start failed that way on a
slower machine, and the start hung. A creation or config change now waits until a controller is known and the
broker is one of the cluster's. A Kafka Metadata request now names this broker as controller while none is elected,
as Kafka's brokers name any live broker: told -1, an admin client waited for a controller until its request timed
out. A topic that Metadata would auto-create is not created before the election; the client is told to ask again
(`LEADER_NOT_AVAILABLE`).

### A broker names its controller the moment its Raft node knows the leader (#180)

The controller a broker reports was carried over from its Raft node by a loop that looked every 100 ms. Until the
next look a broker that had already heard from the leader still reported none: a Metadata request answered -1 (and
since #235 names the asked broker itself), on which an admin client waits for a controller until its request times
out — `Failed while waiting for controller: Local: Timed out` on a cluster that had elected its controller. A new
leader was the controller only up to 100 ms after its election and answered `NOT_CONTROLLER` until then, and a
leadership lost and won again between two looks was not noticed, so the new term got no controller epoch. The Raft
node now announces every change of its leadership (`RaftNode.LeadershipChanged`), and the controller role and the
reported controller id follow it on that announcement: a follower answers the leader's first AppendEntries only
after it names that leader.

### ISR reports reach the controller, and a stopping controller stops (#234)

- **ISR reports:** a partition leader's ISR reports to the controller timed out in bursts of ten seconds. The
  inter-broker connection pool counted live connections, not leased ones. A caller that found no idle connection
  waited for a permit, and a returned connection never gave one back, so the caller waited until its own timeout
  while the connection sat idle in the pool. Right after a cluster started, dozens of reports were lost that way.
  The ISRs of the internal topics then stayed at the leader alone, and stopping a broker found no in-sync
  successor for its partitions. The pool now counts leases, and a caller waiting for a connection gets the one
  returned meanwhile. A Kafka-wire report that timed out mid-exchange also closes its connection, instead of
  pooling it with the late response still in it.
- **Stopping a controller:** a controller handled a broker's failure partition by partition, waiting for each
  decision's commit, without regard to its own shutdown. A controller that had lost its quorum waited five seconds
  per partition, and a stopping broker hung for minutes in its heartbeat monitor. The handling now ends when the
  controller stops; the next controller handles the failure from the metadata log.

### A broker without topic auto-creation starts with Connect (#215)

With `AutoCreateTopics=false` and Connect enabled, which is the default,
the broker's start hung in the Connect worker without an error. The worker
waited for a config topic that nobody created. Like Kafka Connect, the
worker now creates its config, offsets and status topics itself, compacted.
`OffsetsTopicPartitions` (25), `StatusTopicPartitions` (5) and
`InternalTopicReplicationFactor` (1) set them up. A config topic that is
still missing is an error naming it. The client's `ConsumerOptions` gained
`TopicDiscoveryTimeoutMs` for that: 0 fails at once and names the missing
topics, -1 waits for them.

### A topic's config change reaches every broker hosting it (#119)

A topic's config could be changed through Kafka's AlterConfigs and
IncrementalAlterConfigs, gRPC (and REST on it), the native protocol and
adaptive compression. The change was made only in the log manager of the
broker that got the request. The topic's other replicas never had it, a
broker that took a partition over ran it with its old config, and the
change was gone after a restart. A change now goes through the controller
into the metadata log as one `ConfigChanged` entry: the configs it sets and
those it removes. A broker that is not the controller hands it on over the
inter-broker channel. Every broker hosting the topic applies the change in
log order with its leadership changes, and again when it replays the log
at its start. The request completes once the receiving broker has the
change. A change that no controller takes now is a retriable error
(`NOT_CONTROLLER` over Kafka, `LEADER_NOT_AVAILABLE` over gRPC). An empty
value in IncrementalAlterConfigs' DELETE now removes the config. Before, it
set the config to an empty string.

Any broker takes the change, as a Kafka admin client asks any. AlterConfigs
and IncrementalAlterConfigs answered `UNKNOWN_TOPIC_OR_PARTITION` ("Topic
does not exist") on a broker that hosts no replica of the topic, because
they looked the topic up in that broker's own logs: with fewer replicas
than brokers a change failed or not depending on the broker the client
picked. They look it up in the cluster's metadata now, as DescribeConfigs
does (#240).

### A commit after an abort is visible to read-committed consumers

An aborted transaction hid every later batch of its producer from
read-committed consumers, not only its own batches, because its aborted
range had no end. A producer that committed after an abort stayed
invisible. The range now ends at the abort marker.

### Committing or aborting an empty native transaction succeeds (#218)

The native client's `TransactionBuilder` sent an EndTxn for a transaction
that had added nothing. The broker refuses that with `INVALID_TXN_STATE`, as
Kafka does. Such a transaction now completes on the client without a
request, as with the Java producer.

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
own keeps `__consumer_offsets` too and leads all of its partitions. A consumer that
assigns its partitions itself can commit offsets for a group nobody joined,
outside any generation, as Kafka allows.

### A KIP-848 consumer group outlives its coordinator (#219)

A KIP-848 consumer group's members, epochs and assignments lived only in
the memory of the broker that coordinated it. A coordinator that took the
group over knew none of it, and a restart lost it. They are now Kafka's
ConsumerGroup records in the group's partition of `__consumer_offsets`:
the group epoch, each member's metadata, the target assignment and each
member's current assignment. A heartbeat is answered once the changes it
made are on the in-sync replicas. A broker that takes the partition over
reads the group from them, and every member gets a session from then on.
A broker that does not coordinate the group answers `NOT_COORDINATOR`, and
one still reading the partition answers `COORDINATOR_LOAD_IN_PROGRESS`.

### Share fetches are served per group by the partition's leader (#228)

A share group's records were handed out by one queue per topic, shared by
every share group of the topic: two groups took records from each other.
The queue counted a batch of several records as one record, and every
broker served it, a follower from its copy. A share partition is now one
per group and partition, held by the partition's leader, as in KIP-932:
- **Record states:** each record is available, acquired by a member until
  its lock runs out (`ShareRecordLockDurationMs`, Kafka's
  `share.record.lock.duration.ms`), acknowledged or archived. The start
  offset moves past the records that are done.
- **Group limits:** a record delivered as often as the group allows
  (`share.delivery.count.limit`) is archived instead of offered again, and
  the group holds at most `share.partition.max.record.locks` records.
- **Who acknowledges:** only the member holding a record can acknowledge
  it; anything else is `INVALID_RECORD_STATE`.
- **Leaders only:** a broker that does not lead the partition answers
  `NOT_LEADER_OR_FOLLOWER` and names the leader.
- **Start offsets:** a start offset set with AlterShareGroupOffsets is
  where the group's share partition starts.
- **Durable state:** a share partition's state is in `__share_group_state`,
  kept by the share coordinator, which is the leader of the key's partition
  of that topic (`FindCoordinator` key type 2). The partition's leader reads
  the state when it first serves the group, and writes it after each
  acknowledgement and delivery before it answers. A leader that takes the
  partition over goes on where the last one stopped. A record that a member
  held at that point is delivered again. Leaders reach another broker's share
  coordinator over the inter-broker port. The topic's size is set by
  `ShareStateTopicNumPartitions` and `ShareStateTopicReplicationFactor`.
- **Offsets with the share coordinator:** AlterShareGroupOffsets starts a
  share partition afresh under a new state epoch. A leader still holding the
  old state has its next write refused, and it then reads the new start.
  DeleteShareGroupOffsets removes the state, and DescribeShareGroupOffsets
  reads it.
- **Group configs in the metadata log:** a share group's KIP-1240 configs
  (`share.delivery.count.limit`, `share.partition.max.record.locks`,
  `share.renew.acknowledge.enable`) are entries of the metadata log, as
  Kafka's group configs are. Every partition leader applies the group's
  limits; a leader that was not the group's coordinator used to run the
  group with Kafka's defaults. Any broker takes an IncrementalAlterConfigs
  for a group and hands it to the controller. Setting a config no longer
  creates the group, and a DELETE removes the config so the group runs with
  the default again.

- **Java share consumers:** a Java 4.x share consumer works across three
  brokers. Two defects kept it from receiving anything at all:
  - **Empty records:** a ShareFetch answer carried null records for a
    partition with nothing to deliver. That is allowed in v0 only, and the
    client failed every fetch.
  - **Share sessions:** the broker had no share sessions (KIP-932). A member
    opens one with epoch 0. Later ShareFetch and ShareAcknowledge requests
    carry the next epoch and name only the partitions that join or leave,
    and epoch -1 closes the session. The broker served only the partitions a
    request named, so after its first fetch a client got nothing. A wrong
    epoch is `INVALID_SHARE_SESSION_EPOCH`, an unknown session
    `SHARE_SESSION_NOT_FOUND`.
  - **Moving share coordinator:** while a share coordinator moves or loads,
    a fetch of its partitions now gets no records and no error, and the
    member fetches again, as from Kafka's brokers. A
    `COORDINATOR_NOT_AVAILABLE` there stopped Java's share consumer.
- **A fetch waits for records (#231):** a ShareFetch with nothing to
  deliver was answered at once, so an idle share consumer fetched in a
  tight loop. It now waits up to the request's `MaxWaitMs`. The wait ends
  when a record is appended to a partition of the session, a member
  releases or acknowledges records, or a member's lock runs out.
- **MinBytes (#232):** a fetch whose `MinBytes` is above one waits until
  the batches it would get reach that size. It measures before it takes
  any record, so while it waits the records stay with the group and other
  members can get them. At `MaxWaitMs` it gets what is there. A
  `MinBytes` of 0 is answered at once.
- **Broker defaults (#229):** a group that set no `share.delivery.count.limit`,
  `share.partition.max.record.locks` or `share.renew.acknowledge.enable` of
  its own runs with the broker's `ShareDeliveryCountLimit`,
  `SharePartitionMaxRecordLocks` and `ShareRenewAcknowledgeEnable`; they
  were Kafka's defaults, fixed in code.

Rejected records are archived, as in Kafka; they are no longer routed to a
dead-letter topic.

**Breaking:** the start offsets and the configs are no longer in the share
group's record of `__consumer_offsets`. Start offsets and configs set before
this release are gone.

### A share group outlives its coordinator (#219)

A share group's members and epochs lived only in the memory of the broker
that served it. They are now in the group's partition of
`__consumer_offsets`, as Kafka's ShareGroup records. A heartbeat is
answered once its changes are on the in-sync replicas, and a broker that
takes the partition over reads the group from them. A broker that does not
coordinate the group answers `NOT_COORDINATOR`. The start offsets are kept
with the share partitions' state in `__share_group_state`, and the configs
are kept in the metadata log (#228).

### A group taken over by another coordinator can be described (#224)

A broker that took a group's partition of `__consumer_offsets` over read
the group's offsets, but answered `INVALID_GROUP_ID` when asked to describe
the group, and left it out of the group list. `kafka-consumer-groups.sh
--describe` failed after a change of coordinator. Such a group is now
described and listed as Empty, with the protocol type of its metadata
record, as Kafka loads it.

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

### The client's Kafka wire produces and consumes (#236)

`UseKafkaProtocol()`, and the Kafka fallback of `UseAutoDetect()`, connected and then failed at
the first produce, against any broker, Surgewave's own included. The Kafka producer and consumer
parsed responses little-endian and wrote records in a format of their own. Both now use the
Kafka protocol as written: record batches v2 with headers, and response readers for Produce,
Fetch, ListOffsets and OffsetCommit next to the request types in Protocol.Kafka. The consumer
also resolves its starting offset with ListOffsets. `Latest`, the default, used to leave it at
-1, where a fetch returns nothing, so it never delivered a record. Tests produce and consume
over this wire and check each direction with the Confluent client.

Record batches parsed by the broker now keep their headers (in the native header-block form)
instead of dropping them.

### A consumer built with a constructor connects on its first use (#200)

The `SurgewaveConsumer` constructors connected synchronously, running the
client's connection retry loop on the caller's thread: over a minute against
an unreachable broker, in an ASP.NET application at startup. The first call
that needs the broker (`SubscribeAsync`, `ConsumeAsync`, the lag queries) now
connects, as the producer has since #133. Concurrent first calls share one
attempt, each waits only as long as its own token allows, and a failed
attempt is retried by the next call. `CreateAsync` still connects up front.

### `metadata.version` announces the levels the broker implements (#199)

ApiVersions offered `metadata.version` 14–22 and finalized 22, although the broker has the
transactions V2 and share groups that levels 24 and 28 gate. It now offers 14–29 and finalizes
29; 30 and above gate cordoned log directories, CIDR ACLs and controller unregistration, which
Surgewave does not have. ApiVersions and the broker registration read one table of feature
levels, and a test holds it to Kafka's dependencies between levels. The broker registration
carries `metadata.version` next to `inter.broker.protocol`. The unused Kafka-wire
`BrokerLifecycleManager`, which announced 1–20, is gone.

### One cluster id per cluster (#203)

Without a configured `ClusterId`, every broker generated and persisted an id of its own, so a cluster had none
it could tell clients: the brokers reported the literal `surgewave-cluster` instead, and a broker's registration
was never checked against an id. Now the first controller of a cluster writes the id to the metadata log, as
Kafka writes it once when the storage is formatted: the configured id, or a generated one in Kafka's format.
Every broker adopts it from the log and persists it in `cluster.id`, and Metadata, DescribeCluster, ApiVersions
(KIP-1242) and the `cluster.id` config report it — the same id on every broker and across restarts. A broker
that carries another cluster's id is refused at registration with `INCONSISTENT_CLUSTER_ID`, as Kafka's
controller refuses it; a configured id that differs from the persisted one stops the broker at startup. A
single broker and an embedded host get a generated, persisted id too, and the embedded host now passes
`Cluster.ClusterId` through.

### A broker watches every peer it sends heartbeats to

A broker kept a health record only for the peers it knew when its heartbeat manager started, and for those it
had exchanged a heartbeat with since. A broker that learned of a peer later — the embedded host reads its
cluster nodes after that start — and never heard from it sent it heartbeats for good, counted none of the
failures and never reported it failed. As the controller it left the dead broker in every ISR and leading its
partitions: a controller promoted within its first heartbeat interval never handled the failure of the
controller before it. Every broker a heartbeat is sent to is now watched from the first heartbeat on, and is
alive until it has not answered for the heartbeat timeout — as Kafka's controller gives every registered broker
one session timeout from the moment it takes office. `HeartbeatManager.HealthChanged` and the embedded host's
`PeerHealthChanged` complete at the next change of a peer's health.

### The controller reports the ISR of the partitions it leads at every inter-broker protocol level

A partition leader reports an ISR change to the controller over the wire the cluster is finalized to. The
controller's own report goes on no wire, and only the native client hands it to the metadata log; while the
cluster was finalized to the Kafka wire — as it is until every broker named in the cluster nodes has
registered — the report was dropped each time, and on every other broker the ISR of a partition the controller
leads stayed the controller alone. A controller that then shut down found no in-sync successor for those
partitions. The controller's own report now reaches the metadata log whatever the finalized level is.

### The last broker of a cluster stops without hanging in its heartbeat monitor

A broker stopping after its peers could hang in its own shutdown for minutes. Its heartbeat monitor goes through
the watched peers in one health check and awaits the controller's handling of each failure. The shutdown ended the
handling of the first failed peer, and the check then went on to the next peer and raised its failure too — after
the controller had stopped, if the broker's shutdown had got that far. A stopped controller handled that failure
with no end to it: one five-second wait for a commit per partition the failed broker was in the ISR of, which a
controller without a quorum never gets, while the shutdown waited for the monitor. A stopped controller now
handles no failure and writes nothing to the metadata log, and a stopped heartbeat monitor marks no further peer
failed.

### A broker reports ready, and answers clients, only once it has caught up with its metadata log (#250)

A restarted broker holds its metadata log and has applied none of it until its Raft node is elected, or hears from
the leader. It reported ready and answered clients without waiting for that, so a request that arrived first was
answered from a state that did not have the log yet: a topic created before the restart "does not exist", or —
while the log was being applied — is described without a config change made after its creation. On a single
broker the election comes an election timeout after the start, which the rest of the startup usually outlasts and
on a fast start does not; in a cluster the window lasted until the leader's first entries arrived.

The broker now waits for a startup handshake: it has caught up when it has applied what the leader of the current
term had committed — on a single broker: it is elected and has applied its own log — and has taken up the
partitions that gives it. Until then

- the Kafka and native listener is bound and accepts no connection: a client that connects waits and is answered
  once the broker has caught up;
- gRPC and REST requests are answered `503` (`UNAVAILABLE`), while `/health/live` and `/metrics` are answered as
  before;
- `/health` and `/health/ready` report unhealthy, so a readiness probe sends the broker no traffic;
- the line "Surgewave broker ready to accept connections" is not logged.

A broker that reaches no quorum stays in that state for as long as it lasts, as a Kafka broker in KRaft mode does,
and logs every election it loses. Ready therefore takes a majority of the controller voters: of three brokers the
first one alone is live and not ready, both are ready as soon as a second one runs, and the third starts or
rejoins at any time. The brokers do not have to start at the same moment. What cannot work is an orchestrator that
waits for one broker to be ready before it starts the next:

- **Kubernetes:** a StatefulSet of brokers needs `podManagementPolicy: Parallel`; the default `OrderedReady` waits
  for the first pod, which never becomes ready alone. The headless service the brokers find each other through
  needs `publishNotReadyAddresses: true`, or the names of brokers that are not ready yet do not resolve and no
  quorum forms.
- **Liveness and readiness are different questions.** `/health/live` says the process runs and never fails for want
  of a quorum; it is the liveness probe. `/health/ready` is the readiness probe; a broker that fails it is taken
  out of the client service and must not be restarted for it. The HTTP endpoint is bound a few seconds after the
  process starts, whether or not another broker runs (see "A cluster started from nothing no longer waits 30
  seconds" below), so a startup probe on `/health/live` has the start of the process to cover and nothing else.
- **Docker Compose:** no broker may depend on another broker's `service_healthy`. Services that use the cluster may
  wait for a healthy broker.

The Helm chart, the Kubernetes manifests and `docker-compose.cluster.yml` are set up this way; see "The cluster
deployment manifests bring up a cluster" below.

An embedded broker (`SurgewaveRuntime`) that is its whole quorum is returned from `StartAsync` caught up. One that
is part of a larger quorum is returned as before, because the brokers of an embedding host are started one after
the other; the host waits on the new `SurgewaveRuntime.WaitForMetadataCaughtUpAsync` once it has started them.

### A stopped broker writes nothing more to its metadata log, and a log that cannot be read stops the start

A broker's Raft node and its controller went on writing to the metadata log after they had been stopped. Stopping
them cancelled their work, and a cancellation ends a wait, not a write: a controller that had just taken office
still registered itself and created the internal topics, and the node, which a broker disposes first, still
appended those entries to `raft-log.bin` — as it did with the entries a leader sent and the votes a candidate
asked for while the replication server was still up. A process that exits is not affected. A broker restarted in
the same process on the same directory is, which is what an embedding host and its tests do: the new broker read a
log the old one was still appending to, and ended up with entries it had not loaded behind the ones it had.

If the new broker opened the log at the moment the old one was writing, it could not read it at all. That was
logged as an error and treated as an empty log, so the broker started as the first of a new cluster: with a new
cluster id, without the topics, its entries numbered from 1 again behind the ones it had not read.

- Stopping the Raft node and stopping the controller now return only when their writes under way are done, and
  nothing is written after that: a proposal is refused as it is on a node that does not lead, an AppendEntries is
  answered as not accepted, a vote is not granted.
- A metadata log that exists and cannot be opened or read fails the broker's start with the I/O error. Truncating
  a follower's log, which reads it first, fails in the same way instead of rewriting it empty.

### The cluster deployment manifests bring up a cluster (#265)

None of the three cluster deployments under `deployments/` started a broker: the Helm chart did not render (its
templates included helpers under a name the chart did not define), the Kubernetes StatefulSet and the chart set
the broker id to the pod name, which is not a number, and `docker-compose.cluster.yml` built from a Dockerfile
that does not exist. Behind those, all three named `ClusterNodes` in a form the broker does not read and no
`ControllerQuorumVoters`, which the broker refuses; none overrode the HTTPS endpoints the image binds without a
certificate; the Compose file and the chart told clients a name the listener could not bind or the wildcard
`0.0.0.0`; and the health checks called `curl`, which the image does not hold. They are repaired and were brought
up, each with three brokers:

- **Helm chart** (`deployments/helm/surgewave`): the broker id is the pod's ordinal (the `pod-index` label,
  Kubernetes 1.28+), `Host` the pod's DNS name under the headless service, `ClusterNodes` and
  `ControllerQuorumVoters` are generated for `broker.replicaCount` brokers, all of which vote. A startup probe on
  `/health/live` holds liveness and readiness back until the endpoint is bound. Byte-sized values no longer render
  as `1.048576e+08`. The Control UI is given the brokers' REST endpoint (`Broker__ApiUrl`), the setting it reads.
- **Kubernetes manifests** (`deployments/kubernetes`): the same, written out for three brokers and independent of
  the namespace; the headless service publishes not-ready addresses, a PodDisruptionBudget allows one broker to be
  down, and the volume claim uses the cluster's default storage class.
- **`docker-compose.cluster.yml`**: the released image, the health check on `/health/ready`, the brokers started
  together. `docker-compose.yml` gets the same listener and endpoint settings and a health check for its single
  broker; the Control UI waits for it.

Two additions to the broker carry this:

- `Surgewave:BindAddress` — the address the client listener binds where that is not `Host`. `Host` was both the
  address bound and the name clients and the other brokers are told, so a broker in a container, which is known by
  a DNS name and binds no fixed address, could be configured for one or the other. With `BindAddress` set
  (`0.0.0.0`), `Host` is only the name. Without it nothing changes, except that a name in `Host` is now a
  configuration error that says so instead of "An invalid IP address was specified".
- `surgewave-broker health [ready|live]` — the broker executable asks the health endpoint of the broker running in
  the same container and exits 0 or 1. It is what a container health check runs, the image holding no HTTP
  client; the endpoint is taken from the broker's own configuration.

`.github/workflows/deployments.yml` lints, renders and validates the manifests and brings each of them up on every
change to them.

### A broker with the embedded Connect worker becomes ready in a cluster (#268)

With `Surgewave:Connect:Enabled`, which the image has on, two of three brokers of a new cluster never became ready
and never bound their HTTP port, and a broker that waited for a quorum answered no liveness probe. Three causes, all
in the start:

- **The worker's start was a step of the host's start.** The worker is a client of the broker it runs in, and since
  #250 a broker accepts clients only once it has caught up with its metadata log. The host waited for the worker
  before it bound its HTTP port, so a broker without a quorum waited inside the worker's connect, with `/health/live`
  unanswered, and whatever kept the worker from starting kept the broker from being ready. The worker now starts
  beside the host, on the listener's accept signal: the endpoints are mapped at once and answer 503 until the worker
  has started, the broker says ready and answers its probes without it, and a worker that cannot start stops the
  broker with a non-zero exit code.
- **A request for a topic the broker hosts no replica of was never answered.** With topic auto-creation on, which is
  the default, a broker asked over the native protocol for the offsets, the records or a produce of a topic it had
  no replica of took the topic for a missing one and went on to create it. The metadata log refuses that — the topic
  exists — and the exception ended the connection's requests without an answer. The worker of a broker that hosts no
  replica of the worker's config topic asks for the topic's offsets when it starts, and waited for the answer for as
  long as the broker ran: that is what kept brokers 2 and 3 of the cluster deployments from starting. Such a request
  is now answered `NotLeader`, as a produce for a partition another broker leads is.
- **A topic reported as created that no metadata log had.** A controller that lost its leadership with a topic's
  entry proposed and not yet on a majority has that entry cut off by the next leader, which writes its own entries
  from that index on. The commit index passes the index all the same, and the controller took that for its own
  entry: it answered "created", made itself the leader of the topic's partitions, and the broker that had asked
  waited for the topic to reach it for as long as it ran. Each broker's worker asks for its topics the moment its
  broker lets it in, which is while the first elections are still under way, so a cluster start ran into it now and
  then. A controller now takes an entry as committed only if it is the entry it proposed, for every decision it
  writes, not topics alone. A creation that finds the office changing hands — the controller deposed on the way, the
  broker asked saying it is not the controller any more — asks the next holder once this broker's Raft node knows
  it, instead of failing or answering for the old one. And the controller creates one topic at a time: two brokers
  that asked for the same topic at once could both find it missing and both write it, under two ids.

What a cluster does not have yet: the workers of its brokers do not form a group. Each broker's worker reads and
writes its stores through its own broker, so only the worker on the broker that leads the partitions of
`surgewave-connect-configs` and `_surgewave-pipelines` has the stored connectors and pipelines; on the other brokers
it starts without them and answers a change to a pipeline with 500. A broker of a cluster that runs the worker says
so in a warning at every start, and the cluster deployments under `deployments/` keep the worker off: run Connect as
its own worker against a cluster. A worker group across the brokers is planned for the next release.

### Two controller voters that split the vote elect a leader while a third one is down

Two brokers of a three-voter quorum, started together while the third was down, could go on without a leader: each
won the pre-vote, voted for itself and lost the election, term after term — ten terms in 55 seconds in the run
that showed it — and since a broker is ready only with a leader (#250), neither became ready.

A split vote is what Raft's randomised election timeout resolves: each candidate waits out its own and starts the
next election, and the one whose timeout is shorter asks first and wins. The node did not end the election at its
timeout. With one vote each, the third voter could still have decided the round, so both candidates waited for
its answer, and a voter that is down answers when the connect attempts to it give up: seconds later, and the same
seconds for both. Both then started the next election on their next tick, in step again, and the timeouts they had
drawn never came into it. The first election had no timeout of its own either: the timer ran from the node's
construction, so two brokers that had waited out the peer discovery together campaigned on their first tick.

- An election ends with its election timeout, and so does a pre-vote round. The node stays a candidate and starts
  the next election a newly drawn timeout later; requests still out are not waited for and their answers are not
  counted.
- The timeout is drawn anew each time the election timer restarts — on a leader's AppendEntries, on a vote
  granted, on each campaign — and the first election comes an election timeout after elections were enabled. A
  broker that is its whole quorum elects itself as early as before.
- A campaign whose pre-vote was refused is repeated an election timeout later, not on the next 50 ms tick.
- A voter that does not answer has one pre-vote and one vote request out at a time: it is not asked again while
  the earlier request is under way, and a voter with too few peers left to ask for a majority starts no round
  until one of them has answered or its connect attempts have given up — at the pace it campaigned at before.

### A cluster started from nothing no longer waits 30 seconds, and a broker alone is live at once (#270, #267)

Brokers started together each waited 30 seconds before they did anything: "Peer discovery timeout after 30s,
proceeding without peer confirmation". A broker's Raft node looked for a peer before it started its elections, by
connecting to every peer's replication port every 100 ms for up to `RaftPeerDiscoveryTimeoutSeconds`, and the
broker opened its own replication port only after that — so each waited for a port none of them had opened yet.
The broker's HTTP endpoint came after it too: a broker started alone answered `/health/live` only once the wait
was over, which is why the deployment manifests carry a startup probe that covers it.

- **A broker starts in the order: read the persisted Raft state, open the replication port, look for peers.** The
  start no longer waits for a peer at all; only the node's first election does. Measured on the shipped broker,
  three controller voters, as processes on one Windows machine: two brokers started together with the third down
  were ready 1 to 2 seconds after their start (32 to 38 seconds before), and a broker started alone answered
  `/health/live` with 200 and `/health/ready` with 503 after 2 seconds (33 to 35 seconds before).
- **Looking for a peer is a handshake, not a poll.** The node tries each peer once and is then told: by the first
  Raft request a voter sends it, or by the first broker heartbeat a voter sends to its replication port. Of two
  brokers the one that tries later finds the other's port open, starts its elections and asks the other for a
  pre-vote; a broker whose peers come up after its attempt hears their heartbeats.
  `RaftPeerDiscoveryTimeoutSeconds` still bounds the wait, and now delays only the first election of a broker that
  hears from nobody: it answers its peers' requests, and serves its HTTP endpoint, throughout.
- `ClusterController.LoadAsync` and `RaftNode.LoadAsync` are the first step for a host that opens the replication
  port itself; `StartAsync` alone still does both steps. `RaftNode.StartAsync` and `ClusterController.StartAsync`
  return once the elections are under way instead of after the peer discovery.
- An embedded broker (`SurgewaveRuntime`) starts in the same order. `SurgewaveRuntime.StartAsync` now fails with
  the error if the metadata log cannot be read or the replication port cannot be bound, and leaves nothing running;
  both used to surface when the runtime was disposed, as a logged warning, on a broker that never joined its
  cluster.

The startup probes of the Helm chart and the Kubernetes manifests, and the Compose health checks' start period,
are left as they are: they hold more time than the start needs now.

### A Raft request is answered from what the node persisted, and by its own answer (#275)

Three defects of the Raft node, found by reading the election code. Each was shown by a test before it was changed.

- **A request could reach a node that had not read its persisted state.** A node is constructed with term 0, no
  vote and an empty log, and reads what it persisted when it is started. The embedded runtime opened the
  replication port and started the node side by side, so a peer's request could arrive in between: a node that had
  voted in term 5 before it restarted granted a second vote in term 5 — two leaders of one term, if both
  candidates count it — granted the pre-vote of a candidate terms behind, and took the entries of a deposed leader,
  appending them to the log file behind the ones it had not read. The shipped broker opened the port after the
  node's start and was not affected. Both hosts now read the state before the port opens, and the node itself
  answers no request from the state it was constructed with: a request that arrives first waits for the one
  reading of the persisted state.
- **The answer to an abandoned request could be read as the answer to the next one.** A TCP connection to a peer
  carries one request after the other. A request that was written and not answered in its five seconds, on the
  last of its three attempts, or whose caller gave up, left the connection in place with its answer still to
  come, and the next request on that connection read it: a pre-vote granted for one term counted as the vote of
  another — the two answers have the same layout — or an earlier AppendEntries' match index taken for the current
  one. A connection is now closed with the request that was abandoned on it, and the answer's correlation id, which
  a broker has always returned and nothing read, is checked: an answer to another request is not taken, and the
  request is asked again on a new connection. The wire format is unchanged.
- **A leader that stepped down campaigned again within 50 ms.** Its election timer was not restarted on the way
  down and had long run out, so it asked for pre-votes on its next tick — of the peers it had just found
  unreachable, when it stepped down for the loss of its quorum. It now waits out an election timeout as any
  follower does.

### The broker image starts with no configuration and speaks Kafka (#269)

`docker run ghcr.io/kuestenlogik/surgewave-broker` did not give a broker, in 0.5.2 as before: without a volume
and on a fresh named volume it ended with "Access to the path '/app/data' is denied", because the image runs as
an unprivileged user and held no directory that user may write to; on a writable data directory it ended with
"Unable to configure HTTPS endpoint", because `appsettings.json` binds gRPC and REST over HTTPS with the ASP.NET
Core development certificate, which an image does not hold; and it had found no Kafka protocol plugin, so it
would have closed every Kafka client. The deployment manifests worked around the first two by running the broker
as root and overriding the endpoints.

- **The published broker carries the Kafka protocol plugin**, in `plugins/kuestenlogik.surgewave.protocol.kafka/`
  next to the executable, where the broker looks first. That holds for every `dotnet publish` of the broker, so
  for the container image, the self-contained archives, the Windows installer and the Linux packages alike; none
  of them held the plugin since it left the broker's own assemblies. A publish that finds no build of the plugin
  fails.
- **The image owns its data directories.** `/app/data`, `/app/data/raft`, `/app/data/plugins` and `/app/logs` exist
  in the image and belong to uid 1654, the user it runs as, and a fresh named volume mounted on one of them takes
  that owner. The image names that user itself, which makes the files under `/app` that user's as well.
- **The image serves its HTTP endpoints in cleartext**, as every published broker does now (see the next entry),
  and the broker says in a warning at every start that they are served without TLS and how to change that.
  [TLS in the container image](../deployment/tls.md#in-the-container-image) describes serving them over HTTPS.
- The image no longer sets `Surgewave__Storage__DataDirectory` and `Surgewave__Storage__LogDirectory`, which
  nothing read.
- **The linux-x64 and the linux-arm64 image are each built from their own files.** The release builds both in one
  publish, which built the broker for both architectures at the same time in the same directories: the build
  could fail on a file the other one was writing, and an image could hold files of the other architecture — the
  0.5.2 linux-arm64 image holds an x86-64 `surgewave-broker` executable next to the assembly it starts. The two
  are now built one after the other, each in directories of its own.

The Compose files no longer run the broker as root or override the endpoints, the Helm chart no longer overrides
what the image now sets, and `docker-compose.yml` mounts its plugin volume on `/app/data/plugins` instead of over
the directory the image ships its plugin in. `.github/workflows/deployments.yml` starts the image with no
configuration on a fresh named volume, waits for its health check and produces and consumes a record over the
Kafka wire protocol and over the native protocol (`scripts/ci/image-smoke.sh`).

### A published broker starts without a certificate, and serves gRPC without TLS (#276)

`appsettings.json` switched TLS on for the broker's two HTTP endpoints and relied on the ASP.NET Core development
certificate. Only a developer's machine holds one, so every published broker ended its start with "Unable to
configure HTTPS endpoint": the image, which 0.5.2 worked around in the manifests, and the self-contained archive
alike, run here with no configuration in a clean container. And where the manifests switched to cleartext, gRPC
did not work: without TLS, Kestrel serves HTTP/1.1 on an endpoint that names HTTP/1.1 and HTTP/2, so REST and
`/health` answered and every gRPC call ended with `HTTP_1_1_REQUIRED`.

- **`Surgewave:GrpcUseTls` is the one switch**, and `appsettings.json` sets it to `false`. It decides the scheme of
  both endpoints whatever scheme their URLs are written with, so `false` alone yields cleartext, as
  [the TLS guide](../deployment/tls.md) always said, and no image, manifest or test rewrites the endpoints to
  change it. `appsettings.Development.json` sets it to `true`: `dotnet run` and `scripts/stack.ps1` serve HTTPS
  with the development certificate as before.
- **Without TLS the second endpoint, port 9094, serves gRPC** over HTTP/2 with prior knowledge, which HTTP/3, its
  purpose with TLS, cannot be served without; port 9093 serves REST, `/health` and `/metrics` over HTTP/1.1. The
  broker names both ports in its warning and in its ready banner.
- **With TLS, `Surgewave:GrpcCertificatePath` is the certificate of both endpoints.** It was given to the first
  one only, so a broker with a certificate and the second endpoint on HTTPS ended with "Unable to configure
  HTTPS endpoint" as well.
- The image sets no environment variable about TLS any more, so TLS is switched on by an environment variable or
  by a mounted `appsettings.Production.json`, whichever a deployment prefers.
- The Control UI's `Broker:ApiUrl` comes from its `appsettings.json` (`http://localhost:9093`) and, in the
  Development environment, from `appsettings.Development.json` (`https://localhost:9093`).

### Every image and archive of a release holds the binaries of its own architecture (#276)

Every build of a project wrote to one obj, one bin and one publish directory whatever runtime it was for, and a
release builds several runtimes of the same project. What the broker image got in the previous entry held for
the rest:

- **The Control image.** The 0.5.2 linux-arm64 image holds the x86-64 `surgewave-control` executable; built here
  as the release builds it, the linux-x64 image held the aarch64 one.
- **The CLI archives.** `surgewave-cli-linux-arm64`, `-osx-x64` and `-osx-arm64` of 0.5.2 hold the linux-x64
  `surgewave` executable, so the CLI of those three archives does not start. The broker archives were right.
- **Image labels and environment values.** The SDK cut every value at its first colon on the way into the build
  of each architecture: the 0.5.2 images carry `org.opencontainers.image.source: https`, a creation time
  `2026-10-20T18`, and the Control image `ASPNETCORE_URLS=http`. They are whole now, for every image of the
  repository, and a build that would lose one fails.

A build for a runtime now has directories of its own (`artifacts/{obj,bin,pub}/<project>/<configuration>/<runtime>/`).
The release publishes the images with `scripts/ci/build-images.sh`, and `.github/workflows/deployments.yml` builds
them with the same script for both architectures, checks every native binary of each image against the image's
architecture (`scripts/ci/image-architecture.sh`), and starts the linux-arm64 broker under emulation until it
reports ready.

### A data directory the broker may not write to is said at the start (#276)

A broker on a data directory it could not write to ended with the stack trace of the first component that wrote
("Access to the path '/app/data/meta.properties.tmp' is denied") and exit code 139; where the directory was
writable and its contents were not, it logged a failed transition for every partition and never became ready.
It now ends its start with one line that names the directory, the user and the `chown` that mends it. The usual
cause is a named volume that a deployment before 0.6.0 wrote as root; [Docker Deployment](../deployment/docker.md#a-volume-written-as-root)
has the command, which was run here on a volume written by the 0.5.2 image as root: the broker started on it
and served the record written before.

### The Kafka plugin in the broker carries the release's version (#276)

The plugin's manifest in `plugins/kuestenlogik.surgewave.protocol.kafka/` said `0.1.0` in every release, the
version written by hand into `plugin.json`. The copy a broker carries now says the version of the build, as the
plugin's `.swpkg` already did.

### Quick start and Docker documentation (#276)

The README's quick start named an image that does not exist (`ghcr.io/kuestenlogik/surgewave`) and promised the
Control UI from it; it now starts the broker image and the Control image. The Dockerfile in
[Docker Setup](../setup/docker.md) started an assembly that does not exist and is replaced by the command that
builds the image, and the TLS guide describes the endpoints as they are.

### The deployment manifests carry the gRPC port (#265, #276)

Without TLS, as every deployment under `deployments/` serves the broker, gRPC is on the second HTTP endpoint,
and no deployment carried it. A gRPC call to the port the manifests named `grpc` ended with `HTTP_1_1_REQUIRED`,
that port serving REST over HTTP/1.1, and the port that answers gRPC was not published: the Compose files and
the Helm chart had it bound in the container and on no host port and no service, and the Kubernetes manifests
replace the image's `appsettings.json` with one that named the first endpoint alone, so their brokers served no
gRPC on any port. Each was run that way first.

- **Helm chart**: the new value `broker.ports.grpcHttp2` (`9094`) is the second endpoint's port. The chart binds
  the endpoint to it, names it as the container port `grpc-http2` and carries it on the headless, the client and
  the external service. `broker.ports.grpc` is what it was: REST, `/health` and `/metrics`, which the probes, the
  ServiceMonitor and the Control UI use.
- **Kubernetes manifests**: the ConfigMap names both endpoints and sets `GrpcUseTls` to `false`, and port `9094`
  is on the StatefulSet and on the three services as `grpc-http2`.
- **`docker-compose.yml`** publishes port `9094`. **`docker-compose.cluster.yml`** binds the second endpoint to
  port `9098` in every broker and publishes it on the host ports `9098`, `9099` and `9100`; the host ports it
  published before are unchanged.

With TLS switched on, both ports of each deployment serve gRPC and REST over HTTPS. On Kubernetes the probes
then have to ask over HTTPS, which the manifests take as an edit and the chart has no value for;
[the TLS guide](../deployment/tls.md#on-kubernetes) says what a deployment needs.
`scripts/ci/compose-cluster-smoke.sh` and `scripts/ci/kubernetes-cluster-smoke.sh` ask every broker for its
topics over gRPC and over REST, through the published ports and through the services, in every scenario.

### A native request is answered or its connection is closed, and a client fails what waited on a connection that went (#281)

A request over the native protocol could be left without an answer, and its caller then waited for as
long as the connection stood. That is how a refused topic creation hung the embedded Connect worker (#268
fixed two cases of it; this is the mechanism).

- **Broker.** Whatever a handler threw that it had not turned into an answer itself ended the connection's
  request loop, while the connection stayed open: the request was never answered, and the next request on
  the connection closed it. A request whose payload the handler could not read was one such case — 42 of
  the 155 op codes left a request of too few bytes unanswered. Every such request is now answered with the
  new error code `InternalError` (22) and a message that names the operation and the request id; the
  exception goes to the broker's log under that id, not to the client, and the connection takes the next
  request. A broker that stops does not answer request by request: it closes the connection. So does a
  broker that cannot write an answer.
- **Streaming.** `StreamAck` was taken without an answer, while the streaming consumer sends it as a request
  and waits for the answer before it sends the next: a consumer returned credit once. It is answered now —
  `SubscriptionNotFound` for a subscription the connection does not have. And a `Subscribe` at "latest"
  (`-1`, the client's default) or "earliest" (`-2`) was never answered and stopped every later request on
  its connection: the offset was read at as it was instead of being resolved, and the push loop went round
  inside the request without waiting.
- **Client.** A request that waits for its answer when the broker closes or resets the connection failed
  already, over TCP and over QUIC, with what the read had thrown ("Unable to read beyond the end of the
  stream"). It now fails with an `IOException` that names the broker and says the connection was closed or
  reset, as does every request made on that connection afterwards — over TCP such a request was written into
  the closed connection and waited for good. `SurgewaveStreamingConsumer.Records` ends with the same failure
  instead of waiting for the next record, and `IsConnected` is false from then on. An `IOException` is what
  the client's retry policies and the consumer's reconnect already take for a lost connection.
  `SurgewaveProtocolException.BrokerMessage` now carries what the broker said in an error answer; it was
  always null.
- **Kafka wire.** A request that failed in its handler was logged and not answered. A Kafka client matches
  answers to requests by their order, so it waited for the answer until its request timeout and read the
  answer to its next request as the answer to this one. The broker now closes the connection, as it does
  for a request it cannot read, which a client takes as a disconnect and retries; it does not build Kafka's
  per-API `UNKNOWN_SERVER_ERROR` response. Produce, Fetch and Metadata were checked for the auto-creation
  case of #268 — a topic of the cluster this broker hosts no replica of, taken for a missing one — and do
  not have it.
- **Logging.** The native data, key-value and streaming handlers wrote to a logger that discarded every
  line. They log under their own categories now, so "Auto-creating topic …" appears.

`InternalError` (22) is an addition to the native protocol's error codes; a client that does not know it
sees a non-zero code, which is a failed request as before.
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

### The Küstenlogik colour language; lockups as wide as their content (Kuestenlogik/Kuestenlogik#10)

The marks and wordmarks follow the shared Küstenlogik colours: navy
`#003e60` for the mark body and the first run of the wordmark (white on
dark), and Surgewave's signature colour Blue Bright `#66cfff` for the wave,
the crest and "wave" in the wordmark, on light and dark alike. The drawing
paints wave and crest in one colour, so the adaptive SVGs have one class,
`.surgewave-wave`; `.surgewave-crest` is gone. Black and white stay one
colour. Every lockup is now as wide as its content: the horizontal one is
769×200 instead of 1100×200, the vertical one 397×380 instead of 480×380.
Branding tool 1.2.0.

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

### The inter-broker topic creation carries a replica assignment (#202)

The request a broker sends the controller to create a topic now ends with
the replicas of each partition. A broker of the previous release cannot
read it, and the reverse, so a cluster creating topics through a broker
that is not the controller is upgraded as a whole, not broker by broker.

### Clients see the cluster's generated id, not `surgewave-cluster` (#203)

A broker without a configured `ClusterId` reported `surgewave-cluster` to clients. It now reports the id its
cluster generated, which differs per cluster; `BrokerConfig.DefaultClusterId` is gone, and
`IBrokerConfigView.ResolvedClusterId` is `null` on a broker that has not joined its cluster yet. A client that
pinned `surgewave-cluster` (KIP-1242) rebootstraps once; configure `ClusterId` to keep a fixed id. An existing
cluster gets its id from its controller after the upgrade, and every broker adopts it. A broker that has a
`cluster.id` file with another id — written for a configured id before the upgrade — is refused with
`INCONSISTENT_CLUSTER_ID`: configure that id on every broker, or remove the file. Registration answers a wrong
id with `INCONSISTENT_CLUSTER_ID` (104) instead of `CLUSTER_AUTHORIZATION_FAILED`.

### The JSON group-state files are gone (#219)

`JsonFileGroupStateStore` and its `.metadata/<subfolder>/<group>.json`
files are removed. KIP-848 and share groups keep their state in
`__consumer_offsets`. Neither host had wired the files, so nothing is
carried over.

### Transaction markers are Kafka's control records (#226)

A commit or abort marker's control record is now Kafka's: the key holds
the version and the marker type, the value the version and the
coordinator epoch. The type used to be in the value, so Kafka tooling that
dumps a log, and a client handed a marker, read no type from it. Markers
written before the upgrade keep the old layout; a transactional offset
commit in `__consumer_offsets` whose marker is in the old layout stays
pending.

### Transaction state is not carried over from an upgrade (#197)

Transaction state was kept in a file under the data directory. It is now
kept in `__transaction_state`, and the file is neither read nor migrated.
A transactional producer initialises afresh after the upgrade. Finish or
abort open transactions before upgrading.

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

### `SurgewaveClientBuilder.Build()` is gone; use `BuildAsync()` (#200)

`Build()` blocked the caller's thread on `BuildAsync()`, through the
connection retry loop. It cannot simply skip connecting: protocol
auto-detection is a connection. Replace
`SurgewaveClient.Create(servers)...Build()` with
`await SurgewaveClient.Create(servers)...BuildAsync()`.

### The native heartbeat carries offline log directories (#162)

The native inter-broker heartbeat payload has a new field, the broker's
offline log directories, so a broker of this release and one of an earlier
release cannot heartbeat each other natively. Upgrade the brokers of a
cluster together. `BrokerHeartbeatInput` gains the required
`OfflineLogDirs`, and `ClusterMembershipHandler` no longer takes a logger.

### Helm chart values removed and renamed (#265)

The chart's values changed with its repair; there are no aliases.

- `broker.config.useRaft` is gone, as the setting it rendered is (see above).
- `tieredStorage.gcp.projectId` is gone; the broker has no such setting.
- `livenessProbe.initialDelaySeconds` and `readinessProbe.initialDelaySeconds` are gone. The new `startupProbe`
  (`periodSeconds`, `timeoutSeconds`, `failureThreshold`) holds both probes back until the broker's endpoint is
  bound.
- `broker.image.repository` and `control.image.repository` default to the released images,
  `ghcr.io/kuestenlogik/surgewave-broker` and `ghcr.io/kuestenlogik/surgewave-control`; an empty `tag` is the
  chart's `appVersion`.
- `clusterDomain` is new (`cluster.local`): the brokers are told their names under it.

In the ConfigMap the tiered-storage keys carry the names the broker reads: `S3BucketName`, `S3Region`,
`AzureContainerName`, `GcpBucketName`.

### The broker image: its user, its endpoints and its plugin directory (#269)

- The image runs as uid 1654 as before, and the files under `/app` now belong to that user instead of root. A
  container that set `user: "0:0"` to write to a named volume no longer needs it; a named volume it wrote as root
  is not writable for uid 1654 until it is given to that user (`chown -R 1654 /app/data`, once, as root).
- The image serves its HTTP endpoints in cleartext until it is given a certificate; see the next entry.
- The environment variables `Surgewave__Storage__DataDirectory` and `Surgewave__Storage__LogDirectory` are gone
  from the image. They were never read; the data directory is `Surgewave__DataDirectory`.
- `/app/plugins` holds the Kafka protocol plugin. A volume mounted over it hides the plugin, or keeps the one of
  the image the volume was first filled from: mount plugin volumes on `/app/data/plugins` and do not set
  `Surgewave__PluginsDirectory`, which makes the broker search that one directory only.

### TLS is off outside the Development environment, and gRPC without TLS is on port 9094 (#276)

- `Surgewave:GrpcUseTls` is `false` in `appsettings.json` and `true` in `appsettings.Development.json`. A broker
  started outside the Development environment without the setting served HTTPS with the development certificate
  where the machine held one; it serves cleartext now and says so. Set `Surgewave:GrpcUseTls=true` and
  `Surgewave:GrpcCertificatePath` to keep HTTPS.
- `Surgewave:GrpcUseTls=false` yields cleartext on both endpoints even where their `Kestrel` URLs say `https`,
  and `true` yields HTTPS on both even where they say `http`. A configuration that named `https` URLs and a
  certificate in the `Kestrel` section and set `GrpcUseTls=false` beside them was served over HTTPS and is not
  any more; without the setting the `Kestrel` section stands as written.
- Without TLS, port 9093 serves HTTP/1.1 only (REST, `/health`, `/metrics`) and port 9094 serves gRPC over
  HTTP/2 with prior knowledge on every interface. Port 9094 served nothing a client could use without TLS before.
  With TLS nothing moves: gRPC and REST are on port 9093, and port 9094 adds HTTP/3.
- With TLS and `Surgewave:GrpcCertificatePath`, the second endpoint uses that certificate unless it names one of
  its own under `Kestrel:Endpoints:GrpcHttp3:Certificate`.
- The Control UI's `Broker:ApiUrl` is `http://localhost:9093` unless it is set, `https://localhost:9093` in the
  Development environment. It was `https://localhost:9093` in every environment.

### Build output of a runtime-specific build moved (#276)

`dotnet build` and `dotnet publish` with `-r <runtime>` write to `artifacts/obj`, `artifacts/bin` and
`artifacts/pub` under `<project>/<configuration>/<runtime>/`, no longer to `<project>/<configuration>/`. A publish
with `-o` goes where it is told, as before.

### What a native client and a host see of a failed or lost request (#281)

- A call on a `SurgewaveNativeClient` whose connection was lost throws an `IOException` that says so. Before, it waited for an answer or threw
  `InvalidOperationException` ("Client is not connected. Call ConnectAsync first."), depending on what the socket
  had noticed.
- `ISurgewaveTransport` has a new member, `ConnectionEnded`. It has a default that never completes, so a
  transport of your own compiles unchanged, and a streaming consumer on it does not notice a lost connection
  until the transport implements it.
- `SurgewaveNativeHandler` takes the host's `ILoggerFactory` instead of an `ILogger<SurgewaveNativeHandler>`.
- Over the Kafka wire, a request that fails in its handler or cannot be read closes the connection; the
  requests after it on that connection were answered before.

<details>
<summary><b>Installation</b> — NuGet, Container, MSI, Linux, Helm</summary>

**NuGet:**
```bash
dotnet add package Kuestenlogik.Surgewave.Client --version 0.6.0
```

**Container — pre-built (Docker Hub, multi-arch linux/amd64 + arm64):**
```bash
docker pull kuestenlogik/surgewave-broker:0.6.0
docker pull kuestenlogik/surgewave-control:0.6.0
# …or :latest
```

**Container — alternative registry (GHCR):**
```bash
docker pull ghcr.io/kuestenlogik/surgewave-broker:0.6.0
docker pull ghcr.io/kuestenlogik/surgewave-control:0.6.0
```

**Container — build from source:**
```bash
dotnet publish src/Kuestenlogik.Surgewave.Broker.App -c Release /t:PublishContainer
dotnet publish src/Kuestenlogik.Surgewave.Control -c Release /t:PublishContainer
```

**Windows MSI (silent install):**
```powershell
msiexec /i surgewave-0.6.0-win-x64.msi /qn
```

**Linux:**
```bash
sudo bash install.sh
```

**Helm:**
```bash
helm install surgewave deploy/helm/surgewave/ --set broker.image.tag=0.6.0
```

</details>

---

_The full commit list, contributors, and compare-URL diff are auto-generated below._

---
## v0.5.2 — 2026-10-20 — Dependency security fixes

0.5.2 is a security patch for 0.5.1 and changes nothing else. Several published packages pulled dependencies with known advisories into every project that referenced them — most of the Streams family and the SQLite storage engine a native SQLite build with a high-severity advisory, the MessagePack serde a MessagePack with a dozen. 0.5.2 raises each of them to a fixed version and lets the restore fail on any advisory from now on. Upgrading is recommended for every 0.5.1 installation and every project that references the packages below. It is built from 0.5.1 with the dependency changes only: the metadata-log work on `main`, which changes the cluster protocol, is not in it.

## Fixes

### SQLite no longer ships with a known advisory (#183)

`Microsoft.Data.Sqlite` 10.0.8 brings `SQLitePCLRaw.lib.e_sqlite3` 2.1.11, the native SQLite build covered by [GHSA-2m69-gcr7-jv3q](https://github.com/advisories/GHSA-2m69-gcr7-jv3q) (high). 0.5.1 restored only because the build suppressed that advisory, waiting for an upstream fix; `Microsoft.Data.Sqlite` 10.0.13 brings SQLitePCLRaw 2.1.13 with the fix, and the suppression is gone. Affected in 0.5.1, and fixed in 0.5.2:

- `Kuestenlogik.Surgewave.Streams`, `Kuestenlogik.Surgewave.Streams.Linq`, `Kuestenlogik.Surgewave.Streams.InteractiveQueries`, `Kuestenlogik.Surgewave.Streams.InteractiveQueries.Hosting`
- `Kuestenlogik.Surgewave.Compatibility.Confluent.Kafka.Streams`
- `Kuestenlogik.Surgewave.Storage.Engine.Sqlite`
- `Kuestenlogik.Surgewave.Protocol.PostgreSql`

### MessagePack 3.1.12 for the MessagePack serde (#183)

`Kuestenlogik.Surgewave.Schema.Registry.Serdes.MessagePack` depended on `MessagePack` 3.1.6, which twelve advisories cover, among them [GHSA-hv8m-jj95-wg3x](https://github.com/advisories/GHSA-hv8m-jj95-wg3x) (high) and [GHSA-382j-8mxh-c7x2](https://github.com/advisories/GHSA-382j-8mxh-c7x2). It now depends on 3.1.12. The serialised format is unchanged.

### The rest of the graph moves to patched versions (#183)

Every `Microsoft.AspNetCore.*`, `Microsoft.Extensions.*`, `Microsoft.Data.Sqlite` and `System.*` 10.0.x dependency moves from 10.0.8 to 10.0.13, the current runtime patch; this takes `Microsoft.AspNetCore.Components.WebAssembly.Server` in the Control host past [GHSA-qjpx-jqm5-h2c4](https://github.com/advisories/GHSA-qjpx-jqm5-h2c4) and [GHSA-g27p-qf6j-c96c](https://github.com/advisories/GHSA-g27p-qf6j-c96c). `Microsoft.OpenApi` follows the lower bound `Microsoft.AspNetCore.OpenApi` 10.0.13 sets (2.12.2), `Microsoft.IdentityModel.*` the one the 10.0.13 authentication packages set (8.22.0). `SSH.NET` moves to 2026.0.0 ([GHSA-mggc-4xg6-vcxf](https://github.com/advisories/GHSA-mggc-4xg6-vcxf), [GHSA-q939-rpr3-3284](https://github.com/advisories/GHSA-q939-rpr3-3284)), and the build-time `Microsoft.SourceLink.GitHub` to 10.0.402 ([GHSA-23fw-v26w-5fgq](https://github.com/advisories/GHSA-23fw-v26w-5fgq) in `Microsoft.Build.Tasks.Git`). The broker, Control and CLI binaries, containers and installers are built against the same versions.

### The restore fails on a known advisory

The advisories above went unnoticed because the build suppressed NuGet's audit warnings. It no longer does: NuGet audit checks the whole graph, direct and transitive, at every severity, and an advisory fails the restore. `dotnet list package --vulnerable --include-transitive` reports nothing for 0.5.2, and every published package was checked on its own the way a consumer restores it.

## Breaking changes

None. Only dependency versions change; the public API and the wire protocols are those of 0.5.1.

<details>
<summary><b>Installation</b> — NuGet, Container, MSI, Linux, Helm</summary>

**NuGet:**
```bash
dotnet add package Kuestenlogik.Surgewave.Client --version 0.5.2
```

**Container — pre-built (Docker Hub, multi-arch linux/amd64 + arm64):**
```bash
docker pull kuestenlogik/surgewave-broker:0.5.2
docker pull kuestenlogik/surgewave-control:0.5.2
# …oder :latest
```

**Container — alternative registry (GHCR):**
```bash
docker pull ghcr.io/kuestenlogik/surgewave-broker:0.5.2
docker pull ghcr.io/kuestenlogik/surgewave-control:0.5.2
```

**Container — build from source:**
```bash
dotnet publish src/Kuestenlogik.Surgewave.Broker.App -c Release /t:PublishContainer
dotnet publish src/Kuestenlogik.Surgewave.Control -c Release /t:PublishContainer
```

**Windows MSI (silent install):**
```powershell
msiexec /i surgewave-0.5.2-win-x64.msi /qn
```

**Linux:**
```bash
sudo bash install.sh
```

**Helm:**
```bash
helm install surgewave deploy/helm/surgewave/ --set broker.image.tag=0.5.2
```

</details>

---

_The full commit list, contributors, and compare-URL diff are auto-generated below._



**Full Changelog**: https://github.com/Kuestenlogik/Surgewave/compare/v0.5.1...v0.5.2

---
## v0.5.1 — 2026-08-26 — Plugins move out of the install directory

0.5.1 settles where a plugin comes from and where it lives. The Bowire workbench leaves the
broker and becomes something you install, and installed plugins stop living inside the
installation directory — so an upgrade no longer overwrites them, and a service reads the
same directory the operator wrote to.

## Highlights

### The Bowire workbench is a plugin now (#154)

The broker no longer serves `/bowire`. The workbench ships as
[Surgewave.Diagnostics.Bowire](https://github.com/Kuestenlogik/Surgewave.Diagnostics.Bowire),
a `.swpkg` carrying both Bowire and the `surgewave://` protocol adapter, so one install gives
you the workbench speaking the native protocol.

The reason is not tidiness. The adapter is built against `Surgewave.Client`, which this
repository produces, so a broker that embedded the workbench could only ever be built against
an already-published adapter — against an older state of its own client. There was no
ordering of the two builds that resolved it. As a plugin, the adapter is built after both
products, like every other extension.

It also puts a debugging surface under deployment control rather than under a configuration
flag. Setting `Enabled=false` left the endpoints, the embedded resources and the dependencies
in the image, one misconfiguration away from being reachable. Not installing the plugin leaves
nothing to reach.

### Installed plugins are data, not part of the installation (#157, #158)

"The plugins directory" was resolved independently in the broker, the CLI, Control, the
Connect worker and the marketplace, and the first two disagreed: the CLI wrote to `plugins`
relative to the working directory while the broker read `plugins` next to its own executable.
Installing reported success, `plugins list` confirmed it, and the broker never saw the
plugin — with no error anywhere, because discovery only reports a directory missing entirely,
and next to the executable one usually exists.

Three scopes now, resolved once and shared by every process:

| Scope | Windows | Linux / macOS | Who put it there |
|---|---|---|---|
| installation | `<install>/plugins` | `<install>/plugins` | shipped with the artefact; replaced on upgrade |
| machine | `%ProgramData%\Surgewave\plugins` | `/var/lib/surgewave/plugins` | an operator, for this host |
| user | `~/.surgewave/plugins` | `~/.surgewave/plugins` | one account, for a broker it runs itself |

`surgewave plugins install` writes to the machine scope, because a broker usually runs under
a service account and a plugin in someone's home directory is invisible to it. When that
directory is not writable the command fails and names it rather than falling back to the user
scope — a silent fallback would report success for an install the broker could never see.
`plugins list` now shows each plugin's scope and names every directory it searched.

Connectors follow the same scopes. `plugins install --from-nuget` used to write to
`~/.surgewave/connectors` while the Connect plugin scanned a working-directory path; those two
have never named the same place.

### An administrator can refuse plugins from user directories (#158)

`Surgewave:Plugins:AllowUserScope=false` drops the per-user directory from the search order. A
plugin there is code the broker executes that an unprivileged account was able to place on
disk — the machine scope needs elevation, a home directory does not. The broker says at
startup when it has refused the scope, rather than leaving the plugin silently absent.

Put it in the installation's `appsettings.json` or in machine-scope configuration: the setting
binds someone who does not start the process, and anyone who does could equally point
`Surgewave:PluginsDirectory` elsewhere.

### Several brokers on one machine (#158)

`Surgewave:Instance` (or `SURGEWAVE_INSTANCE`) adds one path segment to the machine and user
scopes, the way `PGDATA` and `GEOSERVER_DATA_DIR` do. Unset means the scope is the root
itself, so a single-instance host keeps the short paths and naming an instance later moves
only that one. It is deliberately not the cluster id — two nodes of one cluster on one host
share that, and they are exactly the pair that must not share a directory.

`SURGEWAVE_DATA_DIR` moves every scope under one root, for tests and throwaway environments:
one tree to create, one to delete, instead of state scattered across `bin/Debug`, `bin` or the
repository root depending on how the process was started.

## Fixes

### The workbench was mapped without ever being initialised (#154)

`MapBowire()` maps endpoints and registers nothing. Without the matching `AddBowire()` the
recording, schema-change-log and plugin-update surfaces resolved their services with
`GetRequiredService` and threw on first use rather than at startup. The plugin now does both.

### Plugin configuration changed which settings applied, not which plugins existed (#158)

`Surgewave:PluginsDirectory` was honoured when layering plugin default settings and when
building the signer, but assembly loading had `plugins/` next to the executable hardcoded.
Pointing the setting somewhere therefore changed a plugin's configuration without changing
which plugins were loaded.

### Config inspection fell back to the CLI's own directory (#158)

`surgewave config view` resolves plugins relative to the config file being inspected, which is
right — it may be describing an installation on another host. Its last resort was the
directory next to `surgewave.exe`, which describes nothing; an unaccompanied config file was
inspected against whatever happened to sit there. It now falls back to the resolved scopes.

### Untagged builds published themselves as 0.1.0

CI derived its prerelease version from a hardcoded `0.1.0` rather than from the version floor,
so every branch build of a 0.5.x product published `0.1.0-ci.N` — a version sorting below
every release it superseded. The floor also now names the next patch (`0.5.1-dev` after
v0.5.0) rather than the released one, which under SemVer sorted a development build below the
release it contained.

## Breaking changes

**The broker no longer serves `/bowire`.** Install
`Kuestenlogik.Surgewave.Diagnostics.Bowire` to get it back. Deployments relying on the
endpoint will see 404 until they do; deployments that disabled it to keep it out of production
now need no setting at all.

**`surgewave plugins install` writes elsewhere.** The default was `plugins` relative to the
working directory; it is now the machine scope, and the command fails rather than falling back
when that is not writable. Scripts relying on the old default should pass `--directory`
explicitly, or `--scope user` where a per-user install is what was meant. Plugins installed
under the old default are not migrated — the broker never read that directory either, so
anything there was already inert.

**`Surgewave:Connect:PluginsDirectory` defaults to empty** rather than to `plugins`, meaning
all three scopes are scanned. Set it explicitly to restrict the search to one directory.

**`PluginManifest` is a record** rather than a class, so it gains value equality. Source
compatible; recompile anything comparing instances by reference.

## Also in this release

The test stack moved to xunit.v3 4.0 on Microsoft.Testing.Platform, and VSTest is gone from
every project. Coverage runs through `Microsoft.Testing.Extensions.CodeCoverage` — coverlet
does not work under that runner at all, which is worth knowing before reaching for it.

`PackPluginTask` takes the build's version instead of the one written in `plugin.json`, so a
tagged release can no longer publish NuGet packages as one version next to a `.swpkg` calling
itself another. `surgewave plugins pack --version` exposes the same override.

<details>
<summary><b>Installation</b> — NuGet, Container, MSI, Linux, Helm</summary>

**NuGet:**
```bash
dotnet add package Kuestenlogik.Surgewave.Client --version 0.5.1
```

**Container — pre-built (Docker Hub, multi-arch linux/amd64 + arm64):**
```bash
docker pull kuestenlogik/surgewave-broker:0.5.1
docker pull kuestenlogik/surgewave-control:0.5.1
# …oder :latest
```

**Container — alternative registry (GHCR):**
```bash
docker pull ghcr.io/kuestenlogik/surgewave-broker:0.5.1
docker pull ghcr.io/kuestenlogik/surgewave-control:0.5.1
```

**Container — build from source:**
```bash
dotnet publish src/Kuestenlogik.Surgewave.Broker.App -c Release /t:PublishContainer
dotnet publish src/Kuestenlogik.Surgewave.Control -c Release /t:PublishContainer
```

**Windows MSI (silent install):**
```powershell
msiexec /i surgewave-0.5.1-win-x64.msi /qn
```

**Linux:**
```bash
sudo bash install.sh
```

**Helm:**
```bash
helm install surgewave deploy/helm/surgewave/ --set broker.image.tag=0.5.1
```

</details>

---

_The full commit list, contributors, and compare-URL diff are auto-generated below._



## What's Changed
* chore: bump floor to 0.5.0-dev (after v0.5.0) by @thomas-stegemann in https://github.com/Kuestenlogik/Surgewave/pull/152


**Full Changelog**: https://github.com/Kuestenlogik/Surgewave/compare/v0.5.0...v0.5.1

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

An audit of the zero-copy/pooling paths across seven subsystems surfaced five real defects, all fixed in this release: a span-hash intern-cache collision that could route produced records to the **wrong topic**; two pooled-buffer leaks (decompression rent on the native fetch path, LOH-sized rent on trimmed storage reads); a flush path whose fsync **never reached the disk** (durability was page-cache-only); and a replication size-prefix short-read that desynced the connection.

### Packaging & CI hygiene (#54, #55, #56)

Vulnerable transitives are pinned (Microsoft.OpenApi 2.9.0; the unpatched SQLitePCLRaw advisory is suppressed and tracked in #88), the Broker NuGet no longer references phantom static-web-assets, and the flaky Release-CI replication test is stabilized.

## Acknowledgements

The perf-audit fixes in this release were found by a review of the transport, codec, storage, SIMD and client layers — the remaining (non-correctness) findings are tracked as #78–#87 for v0.8.

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
