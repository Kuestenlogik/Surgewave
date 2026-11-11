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

## Acknowledgements

<Optional. Short. Names contributors who exercised the rc series or
reported the bugs that landed as fixes.>
