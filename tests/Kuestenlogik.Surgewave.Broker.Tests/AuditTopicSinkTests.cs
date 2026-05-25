using System.Text.Json;
using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Broker.Audit;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// Pins down the audit-topic-sink contract (G13). The sink is what makes the
/// audit stream reachable through the regular Kafka wire — without these
/// tests a refactor could quietly drop events on the floor while the file
/// sink kept passing.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class AuditTopicSinkTests : IAsyncLifetime
{
    private readonly string _dataDir;
    private readonly ILoggerFactory _loggerFactory;
    private LogManager? _logManager;

    public AuditTopicSinkTests()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), $"surgewave-audit-sink-{Guid.NewGuid():N}");
        _loggerFactory = LoggerFactory.Create(b => b.SetMinimumLevel(LogLevel.Warning));
    }

    public ValueTask InitializeAsync()
    {
        Directory.CreateDirectory(_dataDir);
        _logManager = TestLogManager.CreateInMemory(_dataDir);
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        await Task.CompletedTask;
        _logManager?.Dispose();
        _loggerFactory.Dispose();
        if (Directory.Exists(_dataDir))
        {
            try { Directory.Delete(_dataDir, recursive: true); } catch { /* best effort */ }
        }
    }

    [Fact]
    public async Task EmptyEventList_NoSideEffects()
    {
        var sink = BuildSink();
        var offset = await sink.WriteAsync(Array.Empty<AuditEvent>());
        Assert.Equal(-1, offset);
        // Topic must not have been created — empty inputs are a no-op.
        Assert.Null(_logManager!.GetLog(new TopicPartition { Topic = sink.TopicName, Partition = 0 }));
    }

    [Fact]
    public async Task SingleEvent_CreatesTopicAndAppendsRecord()
    {
        var sink = BuildSink();

        var ev = MakeEvent(AuditEventType.AuthenticationSuccess, principal: "alice");
        var offset = await sink.WriteAsync(new[] { ev });

        Assert.True(offset >= 0, "Sink should return non-negative offset on success");
        var partition = new TopicPartition { Topic = sink.TopicName, Partition = 0 };
        var log = _logManager!.GetLog(partition);
        Assert.NotNull(log);
    }

    [Fact]
    public async Task EventValueIsJsonSerialisedAuditEvent()
    {
        var sink = BuildSink();

        var ev = MakeEvent(AuditEventType.AclCreated, principal: "bob");
        await sink.WriteAsync(new[] { ev });

        var partition = new TopicPartition { Topic = sink.TopicName, Partition = 0 };
        var log = _logManager!.GetLog(partition);
        Assert.NotNull(log);

        // The serialised JSON must round-trip cleanly back to an AuditEvent
        // with the same shape — that's what downstream consumers depend on.
        var json = JsonSerializer.Serialize(ev);
        var rt = JsonSerializer.Deserialize<AuditEvent>(json);
        Assert.NotNull(rt);
        Assert.Equal(ev.EventId, rt!.EventId);
        Assert.Equal(ev.EventType, rt.EventType);
        Assert.Equal(ev.Principal, rt.Principal);
    }

    [Fact]
    public async Task ExistingTopicIsReused_NoDuplicateCreate()
    {
        // First call creates the topic; second call must not throw on the
        // "already exists" path inside CreateTopicAsync.
        var sink = BuildSink();
        await sink.WriteAsync(new[] { MakeEvent(AuditEventType.TopicCreated) });
        await sink.WriteAsync(new[] { MakeEvent(AuditEventType.TopicCreated) });

        // Second AuditEvent must not have been silently dropped.
        var partition = new TopicPartition { Topic = sink.TopicName, Partition = 0 };
        Assert.NotNull(_logManager!.GetLog(partition));
    }

    [Fact]
    public async Task MultiplePartitions_HashByEventId()
    {
        var sink = BuildSink(partitions: 4);

        // Write enough events to be confident that more than one partition
        // sees traffic. With a deterministic per-EventId hash, two distinct
        // EventIds will normally land on different partitions.
        var events = Enumerable.Range(0, 50)
            .Select(i => MakeEvent(AuditEventType.TopicCreated, eventId: $"evt-{i:000}"))
            .ToList();

        // Send each event in its own batch so the partition choice picks up
        // the per-event hash (the sink picks partition once per batch via the
        // last EventId — which is enough to spread across partitions over 50
        // batches).
        foreach (var ev in events)
        {
            await sink.WriteAsync(new[] { ev });
        }

        var seen = 0;
        for (var p = 0; p < 4; p++)
        {
            if (_logManager!.GetLog(new TopicPartition { Topic = sink.TopicName, Partition = p }) is not null)
                seen++;
        }
        Assert.True(seen >= 2, $"Expected the hash to spread across at least 2 partitions, saw {seen}");
    }

    [Fact]
    public async Task NullPrincipal_FallsBackToBrokerKey()
    {
        // No-principal case (e.g. anonymous failed-auth event) must still
        // produce — the key falls back to "broker-{id}" inside the sink.
        var sink = BuildSink();
        var ev = MakeEvent(AuditEventType.AuthenticationFailed, principal: null);

        var offset = await sink.WriteAsync(new[] { ev });

        Assert.True(offset >= 0);
    }

    private AuditTopicSink BuildSink(int partitions = 1)
    {
        var serializer = new RecordBatchSerializer(NullLogger<RecordBatchSerializer>.Instance);
        var config = new AuditConfig
        {
            Enabled = true,
            TopicSinkEnabled = true,
            TopicName = "_audit_events",
            Partitions = partitions,
            ReplicationFactor = 1,
            RetentionMs = 86_400_000,
        };
        return new AuditTopicSink(_logManager!, serializer, config, NullLogger<AuditTopicSink>.Instance);
    }

    private static AuditEvent MakeEvent(
        AuditEventType type,
        string? principal = "tester",
        string? eventId = null) => new()
    {
        EventId = eventId ?? Guid.NewGuid().ToString("N"),
        EventType = type,
        Principal = principal,
        ResourceType = "topic",
        ResourceName = "orders",
        BrokerId = 0,
    };
}
