using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Runtime;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging;
using Xunit;
using SwTopicPartition = Kuestenlogik.Surgewave.Core.Models.TopicPartition;

namespace Kuestenlogik.Surgewave.IntegrationTests;

/// <summary>
/// The end-to-end scenario behind #92/#93, which unit tests could not reach (#97): a follower
/// catches up over a fetch carrying MANY batches at once, then becomes leader, and a consumer that
/// checks CRCs reads the partition.
///
/// <para><b>Why this needs an E2E test at all.</b> The corruption from #92 — one CRC computed over
/// a concatenation of batches and stamped into the first batch's header — is invisible to the
/// broker that wrote it: read-side CRC validation is off, so the follower serves those bytes
/// happily. It only becomes an error at the far end of the chain, when the follower is promoted and
/// a client with <c>check.crcs</c> enabled reads batch 1. Every link in that chain has to be real
/// for the test to mean anything, which is why this test spends two brokers and a failover.</para>
///
/// <para><b>Two corrections to the issue text</b>, both of which would otherwise make this test
/// pass without proving anything. Confluent.Kafka 2.14.0 defaults <c>check.crcs</c> to
/// <see langword="false"/> (librdkafka's default; the Java client is the one that defaults to true),
/// so <see cref="ConsumerConfig.CheckCrcs"/> is set explicitly below. And there is no
/// <c>CorruptRecordException</c> in this client — a CRC mismatch surfaces as
/// <see cref="ConsumeException"/> with <see cref="ErrorCode.Local_BadMsg"/>, a broker-reported one
/// as <see cref="ErrorCode.InvalidMsg"/>.</para>
/// </summary>
[Trait("Category", TestCategories.Integration)]
[Collection(nameof(BrokerSpawningCollection))]
public sealed class ReplicationFailoverConsistencyTests : IAsyncLifetime
{
    // Defaults (3s/10s) trip on loaded Linux CI runners — same reasoning as ReplicationTests.
    private const int HeartbeatIntervalMs = 5_000;
    private const int HeartbeatTimeoutMs = 30_000;

    /// <summary>
    /// One awaited produce = one record batch on the leader, so this is also the number of batches
    /// the follower has to ingest from a single fetch. Small enough to stay far below the fetcher's
    /// 1 MB cap, large enough that a single-batch-per-fetch implementation cannot fake it.
    /// </summary>
    private const int MessageCount = 200;

    private readonly ITestOutputHelper _output;
    private readonly ILoggerFactory _loggerFactory;

    // What the fetcher published, collected the way a monitoring system would collect it.
    private readonly ConcurrentQueue<(string Topic, int Partition, long Offsets)> _fetchSpans = new();
    private readonly MeterListener _fetchMeter = new();

    private SurgewaveRuntime? _leader;
    private SurgewaveRuntime? _follower;
    private bool _leaderDisposed;

    public ReplicationFailoverConsistencyTests(ITestOutputHelper output)
    {
        _output = output;
        _loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddProvider(new XunitLoggerProvider(output));
            builder.SetMinimumLevel(LogLevel.Information);
        });

        // The replication fetcher publishes how many offsets each fetch ingested; this listens to
        // that instrument the way a monitoring system would. Nothing here reads a log line.
        _fetchMeter.InstrumentPublished = (instrument, listener) =>
        {
            if (instrument.Meter.Name == BrokerMetrics.MeterName &&
                instrument.Name == "surgewave_replication_fetch_offsets")
            {
                listener.EnableMeasurementEvents(instrument);
            }
        };
        _fetchMeter.SetMeasurementEventCallback<long>((_, offsets, tags, _) =>
        {
            string? topic = null;
            var partition = -1;
            foreach (var tag in tags)
            {
                if (tag.Key == "topic") topic = tag.Value as string;
                else if (tag.Key == "partition" && tag.Value is int index) partition = index;
            }

            if (topic is not null)
                _fetchSpans.Enqueue((topic, partition, offsets));
        });
        _fetchMeter.Start();
    }

    public async ValueTask InitializeAsync()
    {
        _leader = await BuildBrokerAsync(brokerId: 1);
        // Broker 2 must know broker 1 from the start: with an empty cluster list it would compute
        // itself as the lowest id and both brokers would claim the controller role.
        _follower = await BuildBrokerAsync(brokerId: 2,
            $"1:{_leader.Host}:{_leader.Port}:{_leader.ReplicationPort}");

        // Full-mesh stitch, mandatory for dynamic ports: a peer learned only through LeaderAndIsr
        // carries client host/port, and the follower would fall back to the Port+1000 replication
        // port convention and never reach the leader (#69, same as ReplicationTests).
        StitchMesh(_leader, _follower);
        StitchMesh(_follower, _leader);

        Assert.True(
            await TestWaitHelpers.WaitForClusterStabilizationAsync([_leader, _follower], output: _output),
            "the two-broker cluster did not stabilise");
    }

    public async ValueTask DisposeAsync()
    {
        if (_leader is not null && !_leaderDisposed)
        {
            try { await _leader.DisposeAsync(); } catch (Exception ex) { _output.WriteLine($"leader dispose: {ex.Message}"); }
        }

        if (_follower is not null)
        {
            try { await _follower.DisposeAsync(); } catch (Exception ex) { _output.WriteLine($"follower dispose: {ex.Message}"); }
        }

        _loggerFactory.Dispose();
        _fetchMeter.Dispose();
    }

    [Fact(Timeout = 180_000)]
    public async Task FollowerPromotedAfterMultiBatchCatchUp_ServesCrcValidContiguousRecords()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var leader = _leader!;
        var follower = _follower!;
        var topic = $"issue97-{Guid.NewGuid():N}";
        var tp = new SwTopicPartition { Topic = topic, Partition = 0 };

        // ── Stall the follower BEFORE it ever connects ────────────────────────────────────────
        // Point its view of the leader's replication endpoint at a port nothing listens on. The
        // ordering is the whole trick: the fetcher opens its connection when it starts following a
        // partition and then CACHES it, so rewriting the endpoint afterwards reaches nothing — an
        // earlier version of this test did exactly that and watched the follower replicate 186 of
        // 200 records "while stalled". Faking the endpoint before the topic exists means the
        // connection is never established in the first place, and the backlog piles up on the
        // leader instead of trickling over one batch per round trip.
        SetLeaderReplicationEndpoint(follower, leader, replicationPort: 1);
        _output.WriteLine("follower stalled: leader replication endpoint pointed at a dead port");

        await CreateReplicatedTopicAsync(leader, topic);
        await ProduceAsync(leader, topic, MessageCount, cancellationToken);

        // Oracle 1: the stall held. Without this the test could pass while the follower had
        // streamed the backlog one batch per fetch — never touching the concatenated path.
        Assert.Equal(MessageCount, leader.LogManager.GetLog(tp)!.NextOffset);
        Assert.Equal(0L, follower.LogManager.GetLog(tp)?.NextOffset ?? 0L);

        // ── Repair, and let one fetch carry the whole backlog ─────────────────────────────────
        SetLeaderReplicationEndpoint(follower, leader, leader.ReplicationPort);
        _output.WriteLine("follower endpoint repaired; waiting for catch-up");

        Assert.True(
            await TestWaitHelpers.WaitForConditionAsync(
                () => follower.LogManager.GetLog(tp)?.NextOffset == MessageCount,
                timeout: TimeSpan.FromSeconds(60), pollInterval: TimeSpan.FromMilliseconds(100),
                ct: cancellationToken, output: _output),
            $"follower never caught up (LEO {follower.LogManager.GetLog(tp)?.NextOffset} of {MessageCount})");

        // Oracle 2: the follower stored N separate, individually CRC-valid batches at the leader's
        // own offsets. This is the assertion #92 would fail: a whole-blob append writes one CRC
        // over the concatenation into batch 1's header, and #93 would show up as a short log.
        var stored = await follower.LogManager.ReadBatchesAsync(tp, 0, maxBytes: 8 * 1024 * 1024, cancellationToken: cancellationToken);
        Assert.Equal(MessageCount, stored.Count);
        for (var i = 0; i < stored.Count; i++)
        {
            Assert.True(RecordBatchValidator.ValidateCrc(stored[i]),
                $"batch {i} on the follower has a CRC that does not match its own bytes (#92)");
            Assert.Equal(i, RecordBatchValidator.GetBaseOffset(stored[i]));
        }

        // Oracle 3: at least one fetch really did carry several batches — the span the fetcher
        // reports for a single round trip, one message per batch here.
        var batchesInLargestFetch = LargestFetchSpan(topic, partition: 0);
        _output.WriteLine($"largest replication fetch carried {batchesInLargestFetch} batches");
        Assert.True(batchesInLargestFetch >= 2,
            $"no replication fetch carried more than one batch — the concatenated-section path from " +
            $"#92 was never exercised, so this test would prove nothing (largest span {batchesInLargestFetch})");

        // ── Fail the leader over ──────────────────────────────────────────────────────────────
        // The follower has to be in the ISR first: graceful shutdown hands leadership to an ISR
        // member and silently skips the partition otherwise.
        Assert.True(
            await TestWaitHelpers.WaitForConditionAsync(
                () => leader.ClusterState!.GetIsrSnapshot(tp).Contains(2),
                timeout: TimeSpan.FromSeconds(60), ct: cancellationToken, output: _output),
            "the follower never joined the ISR, so leadership could not transfer");

        // Explicitly, not via DisposeAsync: that path halves the configured timeout with integer
        // division and can end up with a zero-second budget for the transfer.
        var transferred = await leader.GracefulShutdownAsync(TimeSpan.FromSeconds(20), cancellationToken);
        _output.WriteLine($"graceful shutdown transferred leadership: {transferred}");

        await leader.DisposeAsync();
        _leaderDisposed = true;

        Assert.True(
            await TestWaitHelpers.WaitForConditionAsync(
                () => follower.ClusterState!.GetPartitionState(tp)?.LeaderBrokerId == 2,
                timeout: TimeSpan.FromSeconds(60), ct: cancellationToken, output: _output),
            "the surviving broker never took leadership of the partition");

        Assert.Equal(MessageCount, follower.LogManager.GetLog(tp)!.NextOffset);

        // ── A client asks the promoted broker for metadata ────────────────────────────────────
        // This step is not decoration. A broker that only ever hosted a replica has the partition
        // log but no topic METADATA — replication creates the log directly and registers nothing —
        // so with auto-create enabled this request creates the topic on the spot. Creating a topic
        // used to overwrite the partition log entry with a fresh empty one, and the promoted
        // broker's records became unreachable: the consumer below read zero of 200 records, and
        // nothing anywhere reported an error (#97).
        using (var admin = new AdminClientBuilder(
            new AdminClientConfig { BootstrapServers = follower.BootstrapServers }).Build())
        {
            var metadata = admin.GetMetadata(topic, TimeSpan.FromSeconds(30));
            var partition = metadata.Topics.SelectMany(t => t.Partitions).FirstOrDefault();
            _output.WriteLine($"metadata from the new leader: partition leader={partition?.Leader}");
        }

        Assert.Equal(MessageCount, follower.LogManager.GetLog(tp)!.NextOffset);

        // ── Read it back through a CRC-checking client ────────────────────────────────────────
        var (consumed, errors) = ConsumeFromNewLeader(follower, topic, cancellationToken);
        foreach (var error in errors.Take(5))
            _output.WriteLine($"consumer error (informational): {error.Code} {error.Reason}");

        // A CRC mismatch is a client-local error; a broker-reported corrupt record is InvalidMsg.
        // Everything else — transport failures against the broker we just killed, which stays in
        // metadata because only the Raft path removes brokers — is noise and must not fail the test.
        Assert.DoesNotContain(errors, e => e.Code is ErrorCode.Local_BadMsg or ErrorCode.InvalidMsg);

        // Contiguous: no gaps, no duplicates (#93), in order.
        Assert.Equal(
            Enumerable.Range(0, MessageCount).Select(i => (long)i).ToList(),
            consumed.Select(r => r.Offset.Value).ToList());

        // And the payloads are the ones that were produced — offsets alone would not catch a batch
        // served twice with renumbered offsets.
        Assert.Equal(
            Enumerable.Range(0, MessageCount).ToList(),
            consumed.Select(r => BitConverter.ToInt32(r.Message.Value)).ToList());
    }

    [Fact(Timeout = 180_000)]
    public async Task PromotedBroker_AdvertisesTheTopicsRealShape_NotBrokerDefaults()
    {
        // #118. A broker that only ever hosted replicas has the partition logs but no topic
        // metadata of its own — replication creates the logs directly and registers nothing. Once
        // it is promoted, a client metadata request used to take the auto-create branch and invent
        // the topic from broker defaults: a three-partition topic came back as one.
        var cancellationToken = TestContext.Current.CancellationToken;
        var leader = _leader!;
        var follower = _follower!;
        var topic = $"issue118-{Guid.NewGuid():N}";
        const int PartitionCount = 3;

        using (var admin = new AdminClientBuilder(
            new AdminClientConfig { BootstrapServers = leader.BootstrapServers }).Build())
        {
            await admin.CreateTopicsAsync([
                new TopicSpecification { Name = topic, NumPartitions = PartitionCount, ReplicationFactor = 2 }
            ]);
        }

        // The follower has to have been told about the partitions before it can report them.
        Assert.True(
            await TestWaitHelpers.WaitForConditionAsync(
                () => Enumerable.Range(0, PartitionCount).All(p =>
                    follower.ClusterState!.GetPartitionState(new SwTopicPartition { Topic = topic, Partition = p }) is not null),
                timeout: TimeSpan.FromSeconds(60), ct: cancellationToken, output: _output),
            "the follower never learned all partitions of the topic");

        var tp = new SwTopicPartition { Topic = topic, Partition = 0 };
        // Leadership only transfers to an ISR member: without this wait GracefulShutdownAsync finds
        // no eligible leader, logs it and moves on, and the partition is left leaderless. It passes
        // in isolation because the ISR forms quickly — under a loaded suite it does not.
        Assert.True(
            await TestWaitHelpers.WaitForConditionAsync(
                () => leader.ClusterState!.GetIsrSnapshot(tp).Contains(2),
                timeout: TimeSpan.FromSeconds(60), ct: cancellationToken, output: _output),
            "the follower never joined the ISR, so leadership could not transfer");

        await leader.GracefulShutdownAsync(TimeSpan.FromSeconds(20), cancellationToken);
        await leader.DisposeAsync();
        _leaderDisposed = true;

        Assert.True(
            await TestWaitHelpers.WaitForConditionAsync(
                () => follower.ClusterState!.GetPartitionState(new SwTopicPartition { Topic = topic, Partition = 0 })?.LeaderBrokerId == 2,
                timeout: TimeSpan.FromSeconds(60), ct: cancellationToken, output: _output),
            "the surviving broker never took leadership");

        using var promotedAdmin = new AdminClientBuilder(
            new AdminClientConfig { BootstrapServers = follower.BootstrapServers }).Build();
        var metadata = promotedAdmin.GetMetadata(topic, TimeSpan.FromSeconds(30));

        var advertised = Assert.Single(metadata.Topics);
        _output.WriteLine($"promoted broker advertises {advertised.Partitions.Count} partition(s) for {topic}");

        // Before the fix this was 1 — Surgewave:DefaultNumPartitions — for a topic created with 3.
        Assert.Equal(PartitionCount, advertised.Partitions.Count);
        Assert.Equal(
            Enumerable.Range(0, PartitionCount).ToList(),
            advertised.Partitions.Select(p => p.PartitionId).OrderBy(id => id).ToList());
    }

    [Fact(Timeout = 180_000)]
    public async Task PromotedBroker_KeepsTheTopicId_SoAssignmentsCanStillBeResolved()
    {
        // The half of #118 that was left open: a broker learns a topic's identity from nowhere —
        // replication creates the partition log directly — so the promoted broker used to answer
        // metadata with an id it had invented (or none at all), and a consumer that maps an
        // assignment back by topic id could not.
        var cancellationToken = TestContext.Current.CancellationToken;
        var leader = _leader!;
        var follower = _follower!;
        var topic = $"issue118id-{Guid.NewGuid():N}";

        using (var admin = new AdminClientBuilder(
            new AdminClientConfig { BootstrapServers = leader.BootstrapServers }).Build())
        {
            await admin.CreateTopicsAsync([
                new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 2 }
            ]);
        }

        var tp = new SwTopicPartition { Topic = topic, Partition = 0 };

        Assert.True(
            await TestWaitHelpers.WaitForConditionAsync(
                () => follower.ClusterState!.GetPartitionState(tp) is not null,
                timeout: TimeSpan.FromSeconds(60), ct: cancellationToken, output: _output),
            "the follower never learned the partition");

        var originalId = leader.ClusterState!.GetTopic(topic)?.TopicId ?? Guid.Empty;
        Assert.NotEqual(Guid.Empty, originalId);

        // The id has to reach the follower over whichever inter-broker wire this cluster settled on.
        Assert.True(
            await TestWaitHelpers.WaitForConditionAsync(
                () => follower.LogManager.GetTopicMetadata(topic)?.TopicId == originalId,
                timeout: TimeSpan.FromSeconds(60), ct: cancellationToken, output: _output),
            $"the follower never learned the topic id (got {follower.LogManager.GetTopicMetadata(topic)?.TopicId}, expected {originalId})");

        // Leadership only transfers to an ISR member: without this wait GracefulShutdownAsync finds
        // no eligible leader, logs it and moves on, and the partition is left leaderless. It passes
        // in isolation because the ISR forms quickly — under a loaded suite it does not.
        Assert.True(
            await TestWaitHelpers.WaitForConditionAsync(
                () => leader.ClusterState!.GetIsrSnapshot(tp).Contains(2),
                timeout: TimeSpan.FromSeconds(60), ct: cancellationToken, output: _output),
            "the follower never joined the ISR, so leadership could not transfer");

        await leader.GracefulShutdownAsync(TimeSpan.FromSeconds(20), cancellationToken);
        await leader.DisposeAsync();
        _leaderDisposed = true;

        Assert.True(
            await TestWaitHelpers.WaitForConditionAsync(
                () => follower.ClusterState!.GetPartitionState(tp)?.LeaderBrokerId == 2,
                timeout: TimeSpan.FromSeconds(60), ct: cancellationToken, output: _output),
            "the surviving broker never took leadership");

        // Same id after promotion, and resolvable by id — that is what a next-gen consumer needs to
        // map its assignment back to a topic name.
        Assert.Equal(originalId, follower.LogManager.GetTopicMetadata(topic)?.TopicId);
        Assert.Equal(topic, follower.LogManager.GetTopicMetadataById(originalId)?.Name);
    }

    [Fact(Timeout = 180_000)]
    public async Task ClusteredCreateTopics_KeepsTheClientsConfiguration()
    {
        // Not a transport gap at all: in cluster mode the config never reached the controller,
        // because IClusterTopicCreator.CreateTopicAsync had no parameter for it. The client was told
        // the settings had been applied — the response echoes them back — while they were dropped
        // between the handler and the controller (#118 follow-up).
        var cancellationToken = TestContext.Current.CancellationToken;
        var leader = _leader!;
        var topic = $"issue118cfg-{Guid.NewGuid():N}";

        using var admin = new AdminClientBuilder(
            new AdminClientConfig { BootstrapServers = leader.BootstrapServers }).Build();

        await admin.CreateTopicsAsync([
            new TopicSpecification
            {
                Name = topic,
                NumPartitions = 1,
                ReplicationFactor = 2,
                Configs = new Dictionary<string, string>
                {
                    ["cleanup.policy"] = "compact",
                    ["segment.bytes"] = "1048576"
                }
            }
        ]);

        Assert.True(
            await TestWaitHelpers.WaitForConditionAsync(
                () => leader.ClusterState!.GetTopic(topic) is not null,
                timeout: TimeSpan.FromSeconds(60), ct: cancellationToken, output: _output),
            "the controller never registered the topic");

        var stored = leader.ClusterState!.GetTopic(topic)!;
        _output.WriteLine($"controller stored config: [{string.Join(", ", stored.Config.Select(kv => $"{kv.Key}={kv.Value}"))}]");

        Assert.Equal("compact", stored.Config["cleanup.policy"]);
        Assert.Equal("1048576", stored.Config["segment.bytes"]);
    }

    /// <summary>
    /// Broker 1 is the whole controller quorum; anything joining it is an observer (#172).
    /// </summary>
    /// <remarks>
    /// A declared quorum needs known addresses, and in a dynamic-port fixture only the first
    /// broker's are known before the rest start — so the quorum is the one node that already
    /// exists. That is a supported KRaft topology, and the partition replication these tests are
    /// about does not depend on who votes.
    /// </remarks>
    private async Task<SurgewaveRuntime> BuildBrokerAsync(int brokerId, params string[] clusterNodes)
    {
        var builder = SurgewaveRuntime.CreateBuilder()
            .WithBrokerId(brokerId)
            .WithPort(0)
            .WithReplicationPort(0)
            .WithCluster(clusterNodes);

        if (clusterNodes.Length > 0)
        {
            builder = builder
                .WithControllerQuorum($"1@{_leader!.Host}:{_leader.ReplicationPort}")
                .WithProcessRoles("broker");
        }

        return await builder
            .WithPartitions(1)
            .WithReplicationFactor(2)
            .WithAutoCreateTopics()
            .WithStorageEngine(StorageEngines.Memory)
            .WithLogging(_loggerFactory)
            // Not 3: DisposeAsync halves this with integer division, and the explicit
            // GracefulShutdownAsync above needs a budget that survives a loaded runner.
            .WithShutdownTimeout(20)
            .WithHeartbeatInterval(HeartbeatIntervalMs)
            .WithHeartbeatTimeout(HeartbeatTimeoutMs)
            .Build()
            .StartAsync();
    }

    private static void StitchMesh(SurgewaveRuntime self, SurgewaveRuntime peer)
        => self.ClusterState!.AddBroker(new BrokerNode
        {
            BrokerId = peer.BrokerId,
            Host = peer.Host,
            Port = peer.Port,
            ReplicationPort = peer.ReplicationPort
        });

    /// <summary>
    /// Rewrites what <paramref name="follower"/> believes the leader's replication port to be. The
    /// fetcher dials exactly this value, so a bogus port stalls replication and the real one
    /// resumes it — the only lever a test has, since the fetch interval is a hard-coded field and
    /// the fetcher instance is not reachable from outside.
    /// </summary>
    private static void SetLeaderReplicationEndpoint(SurgewaveRuntime follower, SurgewaveRuntime leader, int replicationPort)
        => follower.ClusterState!.AddBroker(new BrokerNode
        {
            BrokerId = leader.BrokerId,
            Host = leader.Host,
            Port = leader.Port,
            ReplicationPort = replicationPort
        });

    private static async Task CreateReplicatedTopicAsync(SurgewaveRuntime controller, string topic)
    {
        using var admin = new AdminClientBuilder(
            new AdminClientConfig { BootstrapServers = controller.BootstrapServers }).Build();

        await admin.CreateTopicsAsync([
            new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 2 }
        ]);
    }

    /// <summary>
    /// Produces one message per awaited call, which is one record batch per message on the broker —
    /// the batch count the follower later has to split apart. Pinned to the leader: nothing in the
    /// broker rejects a produce aimed at a follower, and a misrouted one would self-assign offsets
    /// into the replica log.
    /// </summary>
    private async Task ProduceAsync(SurgewaveRuntime leader, string topic, int count, CancellationToken cancellationToken)
    {
        using var producer = new ProducerBuilder<Null, byte[]>(new ProducerConfig
        {
            BootstrapServers = leader.BootstrapServers,
            LingerMs = 0,
            BatchNumMessages = 1,
            EnableIdempotence = false,
            MessageTimeoutMs = 30_000
        }).Build();

        for (var i = 0; i < count; i++)
        {
            await producer.ProduceAsync(
                new TopicPartition(topic, new Partition(0)),
                new Message<Null, byte[]> { Value = BitConverter.GetBytes(i) },
                cancellationToken);
        }

        producer.Flush(TimeSpan.FromSeconds(30));
        _output.WriteLine($"produced {count} messages to the leader");
    }

    private (List<ConsumeResult<Ignore, byte[]>> Consumed, List<Error> Errors) ConsumeFromNewLeader(
        SurgewaveRuntime newLeader, string topic, CancellationToken cancellationToken)
    {
        var errors = new ConcurrentQueue<Error>();

        using var consumer = new ConsumerBuilder<Ignore, byte[]>(new ConsumerConfig
        {
            BootstrapServers = newLeader.BootstrapServers,
            GroupId = $"issue97-{Guid.NewGuid():N}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            // Explicit on purpose: librdkafka defaults this to false, so leaving it out would make
            // the whole test vacuous — a batch with a mismatched CRC would be consumed silently.
            CheckCrcs = true
        })
        .SetErrorHandler((_, e) => errors.Enqueue(e))
        .Build();

        // Assign rather than Subscribe: no group coordination, no rebalance, and the read starts at
        // offset 0 regardless of committed state.
        consumer.Assign(new TopicPartitionOffset(topic, new Partition(0), new Offset(0)));

        var consumed = new List<ConsumeResult<Ignore, byte[]>>();
        var deadline = DateTime.UtcNow.AddSeconds(60);

        // The token is the loop's second exit: a cancelled test leaves this poll within one 500 ms
        // Consume rather than sitting out the remaining read budget.
        while (consumed.Count < MessageCount && DateTime.UtcNow < deadline && !cancellationToken.IsCancellationRequested)
        {
            try
            {
                var result = consumer.Consume(TimeSpan.FromMilliseconds(500));
                if (result is null || result.IsPartitionEOF)
                    continue;

                consumed.Add(result);
            }
            catch (ConsumeException ex)
            {
                // A CRC failure lands here. Fail immediately with the code, rather than letting the
                // loop time out and report a count mismatch that hides the real cause.
                Assert.Fail($"consume failed with {ex.Error.Code}: {ex.Error.Reason}");
            }
        }

        consumer.Close();
        _output.WriteLine($"consumed {consumed.Count} records from the new leader");

        return (consumed, [.. errors]);
    }

    /// <summary>
    /// How many offsets the largest single replication fetch ingested, read from the
    /// <c>surgewave_replication_fetch_offsets</c> metric the fetcher publishes.
    /// </summary>
    /// <remarks>
    /// This used to be reconstructed from the fetcher's Debug log line, which made the verdict of
    /// a correctness test depend on a log level: raise it in production and the check goes blind,
    /// lower it and it starts reporting. Logs are for people to read. A test asks the same
    /// instrument an operator would (#177 follow-up).
    /// </remarks>
    private long LargestFetchSpan(string topic, int partition)
    {
        var spans = _fetchSpans
            .Where(f => f.Topic == topic && f.Partition == partition)
            .Select(f => f.Offsets)
            .ToList();

        Assert.NotEmpty(spans);

        // Written out, not just asserted: when this fails, the shape of the fetches is the first
        // thing worth seeing — one big span means the stall worked, many small ones mean the
        // follower kept pace and the multi-batch path was never entered.
        _output.WriteLine(
            $"replication fetches: {spans.Count}, spans [{string.Join(", ", spans.Take(8))}] offsets");

        return spans.Max();
    }
}