using System.Collections.Concurrent;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
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
    private readonly ReplicationFetchProbe _probe = new();

    private SurgewaveRuntime? _leader;
    private SurgewaveRuntime? _follower;
    private bool _leaderDisposed;

    public ReplicationFailoverConsistencyTests(ITestOutputHelper output)
    {
        _output = output;
        _loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddProvider(new XunitLoggerProvider(output));
            builder.AddProvider(_probe);
            // The probe needs Debug (the fetch log line is Debug), the xunit sink does not — without
            // the filter every replication fetch floods the test output.
            builder.SetMinimumLevel(LogLevel.Debug);
            builder.AddFilter<XunitLoggerProvider>((_, level) => level >= LogLevel.Information);
        });
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
        _probe.Dispose();
    }

    [Fact(Timeout = 180_000)]
    public async Task FollowerPromotedAfterMultiBatchCatchUp_ServesCrcValidContiguousRecords()
    {
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
        await ProduceAsync(leader, topic, MessageCount);

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
                timeout: TimeSpan.FromSeconds(60), pollInterval: TimeSpan.FromMilliseconds(100), output: _output),
            $"follower never caught up (LEO {follower.LogManager.GetLog(tp)?.NextOffset} of {MessageCount})");

        // Oracle 2: the follower stored N separate, individually CRC-valid batches at the leader's
        // own offsets. This is the assertion #92 would fail: a whole-blob append writes one CRC
        // over the concatenation into batch 1's header, and #93 would show up as a short log.
        var stored = await follower.LogManager.ReadBatchesAsync(tp, 0, maxBytes: 8 * 1024 * 1024);
        Assert.Equal(MessageCount, stored.Count);
        for (var i = 0; i < stored.Count; i++)
        {
            Assert.True(RecordBatchValidator.ValidateCrc(stored[i]),
                $"batch {i} on the follower has a CRC that does not match its own bytes (#92)");
            Assert.Equal(i, RecordBatchValidator.GetBaseOffset(stored[i]));
        }

        // Oracle 3: at least one fetch really did carry several batches. Nothing in the product
        // reports batches-per-fetch, so this is derived from the fetcher's own debug line: the gap
        // between consecutive fetch base offsets is how many offsets that fetch ingested.
        var batchesInLargestFetch = LargestFetchSpan(topic, partition: 0, followerLeo: MessageCount);
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
                timeout: TimeSpan.FromSeconds(60), output: _output),
            "the follower never joined the ISR, so leadership could not transfer");

        // Explicitly, not via DisposeAsync: that path halves the configured timeout with integer
        // division and can end up with a zero-second budget for the transfer.
        var transferred = await leader.GracefulShutdownAsync(TimeSpan.FromSeconds(20));
        _output.WriteLine($"graceful shutdown transferred leadership: {transferred}");

        await leader.DisposeAsync();
        _leaderDisposed = true;

        Assert.True(
            await TestWaitHelpers.WaitForConditionAsync(
                () => follower.ClusterState!.GetPartitionState(tp)?.LeaderBrokerId == 2,
                timeout: TimeSpan.FromSeconds(60), output: _output),
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
        var (consumed, errors) = ConsumeFromNewLeader(follower, topic);
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

    private async Task<SurgewaveRuntime> BuildBrokerAsync(int brokerId, params string[] clusterNodes)
        => await SurgewaveRuntime.CreateBuilder()
            .WithBrokerId(brokerId)
            .WithPort(0)
            .WithReplicationPort(0)
            .WithCluster(clusterNodes)
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
    private async Task ProduceAsync(SurgewaveRuntime leader, string topic, int count)
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
                new Message<Null, byte[]> { Value = BitConverter.GetBytes(i) });
        }

        producer.Flush(TimeSpan.FromSeconds(30));
        _output.WriteLine($"produced {count} messages to the leader");
    }

    private (List<ConsumeResult<Ignore, byte[]>> Consumed, List<Error> Errors) ConsumeFromNewLeader(
        SurgewaveRuntime newLeader, string topic)
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

        while (consumed.Count < MessageCount && DateTime.UtcNow < deadline)
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
    /// How many offsets the largest single replication fetch ingested, derived from the fetcher's
    /// debug line. Consecutive fetch base offsets bound each fetch; the last one is bounded by the
    /// follower's log end.
    /// </summary>
    private long LargestFetchSpan(string topic, int partition, long followerLeo)
    {
        var boundaries = _probe.Fetches
            .Where(f => f.Topic == topic && f.Partition == partition)
            .Select(f => f.BaseOffset)
            .OrderBy(offset => offset)
            .ToList();

        Assert.NotEmpty(boundaries);

        // Logged rather than only asserted: when this test fails, the shape of the fetches is the
        // first thing worth seeing — one big fetch means the stall worked, many small ones mean the
        // follower kept pace and the multi-batch path was never entered.
        var sizes = _probe.Fetches.Where(f => f.Topic == topic && f.Partition == partition)
            .Select(f => f.Size).Take(5).ToList();
        _output.WriteLine(
            $"replication fetches: {boundaries.Count}, first sizes [{string.Join(", ", sizes)}] bytes, " +
            $"base offsets [{string.Join(", ", boundaries.Take(8))}]");

        boundaries.Add(followerLeo);
        return boundaries.Zip(boundaries.Skip(1), (from, to) => to - from).Max();
    }

    /// <summary>
    /// Captures the replication fetcher's per-fetch debug line. There is no metric, counter or
    /// event that reports how many batches a fetch carried, and without that number this test
    /// cannot tell the multi-batch path from the single-batch one it is meant to cover.
    /// </summary>
    private sealed class ReplicationFetchProbe : ILoggerProvider
    {
        public ConcurrentQueue<(string Topic, int Partition, long BaseOffset, int Size)> Fetches { get; } = new();

        public ILogger CreateLogger(string categoryName) => new ProbeLogger(this);

        public void Dispose() { }

        private sealed class ProbeLogger(ReplicationFetchProbe owner) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(
                LogLevel logLevel, EventId eventId, TState state, Exception? exception,
                Func<TState, Exception?, string> formatter)
            {
                // The message text is the only unambiguous marker. The category is no help — the
                // fetcher logs through the ReplicaManager's logger — and matching on the structured
                // field names alone is worse than useless: the produce path logs "Stored RecordBatch
                // … baseOffset={BaseOffset}, size={Size}" with the very same keys, so an earlier
                // version of this probe counted 200 leader-side appends as replication fetches and
                // reported "no fetch carried more than one batch" while one fetch had carried all 200.
                if (!formatter(state, exception).StartsWith("Fetched ", StringComparison.Ordinal))
                    return;

                if (state is not IReadOnlyList<KeyValuePair<string, object?>> values)
                    return;

                string? topic = null;
                int? partition = null;
                long? baseOffset = null;
                int? size = null;

                foreach (var entry in values)
                {
                    switch (entry.Key)
                    {
                        case "Topic": topic = entry.Value as string; break;
                        case "Partition": partition = entry.Value as int?; break;
                        case "BaseOffset": baseOffset = entry.Value as long?; break;
                        case "Size": size = entry.Value as int?; break;
                    }
                }

                if (topic is not null && partition is { } p && baseOffset is { } offset && size is { } bytes)
                    owner.Fetches.Enqueue((topic, p, offset, bytes));
            }
        }
    }
}
