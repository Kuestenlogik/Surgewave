using System.Text;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kuestenlogik.Surgewave.Cdc.Tests;

/// <summary>
/// Tests for <see cref="CdcService"/>. Pins the source registry semantics
/// (add/duplicate/remove, status snapshot mapping) and the ExecuteAsync paths that
/// need no live database: disabled service, enabled without a connection string,
/// and the fault path triggered by a malformed connection string.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class CdcServiceTests
{
    /// <summary>
    /// A connection string with an unknown keyword: Npgsql rejects it during
    /// connection setup without ever touching the network, giving a deterministic
    /// fault for capture-loop tests.
    /// </summary>
    private const string MalformedConnectionString = "ThisKeywordDoesNotExist=1";

    private static readonly TimeSpan ExecuteTimeout = TimeSpan.FromSeconds(30);

    private static ILoggerFactory CreateLoggerFactory()
        => LoggerFactory.Create(b => b.SetMinimumLevel(LogLevel.Warning));

    private static CdcService CreateService(CdcConfig config, ILoggerFactory loggerFactory, ICdcSink? sink = null)
        => new(config, sink ?? new RecordingSink(), loggerFactory.CreateLogger<CdcService>(), loggerFactory);

    /// <summary>Captures what the capture loop hands to the sink (#144).</summary>
    private sealed class RecordingSink : ICdcSink
    {
        public List<(string Topic, byte[]? Key, byte[] Value)> Writes { get; } = [];

        public ValueTask WriteAsync(
            string topic, ReadOnlyMemory<byte>? key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default)
        {
            lock (Writes)
                Writes.Add((topic, key?.ToArray(), value.ToArray()));
            return ValueTask.CompletedTask;
        }
    }

    [Fact]
    public void Constructor_NullConfig_Throws()
    {
        using var loggerFactory = CreateLoggerFactory();

        Assert.Throws<ArgumentNullException>(() =>
            new CdcService(null!, new RecordingSink(), loggerFactory.CreateLogger<CdcService>(), loggerFactory));
    }

    [Fact]
    public void Constructor_NullSink_Throws()
    {
        // Required, not optional: capture without a destination is the whole of
        // #144 — the loop serialised every event and dropped it, while the source
        // reported healthy and its event count rose.
        using var loggerFactory = CreateLoggerFactory();

        Assert.Throws<ArgumentNullException>(() =>
            new CdcService(new CdcConfig(), null!, loggerFactory.CreateLogger<CdcService>(), loggerFactory));
    }

    [Fact]
    public void Constructor_NullLogger_Throws()
    {
        using var loggerFactory = CreateLoggerFactory();

        Assert.Throws<ArgumentNullException>(() =>
            new CdcService(new CdcConfig(), new RecordingSink(), null!, loggerFactory));
    }

    [Fact]
    public void Constructor_NullLoggerFactory_Throws()
    {
        using var loggerFactory = CreateLoggerFactory();

        Assert.Throws<ArgumentNullException>(() =>
            new CdcService(new CdcConfig(), new RecordingSink(), loggerFactory.CreateLogger<CdcService>(), null!));
    }

    [Fact]
    public void AddSource_NewId_RegistersSourceWithStoppedStatus()
    {
        using var loggerFactory = CreateLoggerFactory();
        using var service = CreateService(new CdcConfig(), loggerFactory);
        var sourceConfig = new CdcConfig
        {
            ConnectionString = "Host=localhost;Database=db",
            SlotName = "slot_a",
            PublicationName = "pub_a",
            Tables = ["public.orders", "public.items"]
        };

        var added = service.AddSource("orders-db", sourceConfig);

        Assert.True(added);
        var status = service.GetSourceStatus("orders-db");
        Assert.NotNull(status);
        Assert.Equal("orders-db", status.Id);
        Assert.Equal("PostgreSQL", status.DatabaseType);
        Assert.Equal(CdcSourceState.Stopped, status.State);
        Assert.Equal("slot_a", status.SlotName);
        Assert.Equal("pub_a", status.PublicationName);
        Assert.Equal(2, status.TrackedTables);
        Assert.Equal(0, status.EventsCaptured);
        Assert.Equal(0, status.LastLsn);
        Assert.Null(status.LastEventTimestamp);
        Assert.Null(status.Error);
    }

    [Fact]
    public void AddSource_DuplicateId_ReturnsFalse()
    {
        using var loggerFactory = CreateLoggerFactory();
        using var service = CreateService(new CdcConfig(), loggerFactory);
        var config = new CdcConfig { ConnectionString = "Host=localhost" };

        Assert.True(service.AddSource("dup", config));
        Assert.False(service.AddSource("dup", config));
        Assert.Single(service.GetAllSourceStatuses());
    }

    [Fact]
    public void GetSourceStatus_UnknownId_ReturnsNull()
    {
        using var loggerFactory = CreateLoggerFactory();
        using var service = CreateService(new CdcConfig(), loggerFactory);

        Assert.Null(service.GetSourceStatus("does-not-exist"));
    }

    [Fact]
    public void GetAllSourceStatuses_NoSources_ReturnsEmpty()
    {
        using var loggerFactory = CreateLoggerFactory();
        using var service = CreateService(new CdcConfig(), loggerFactory);

        Assert.Empty(service.GetAllSourceStatuses());
    }

    [Fact]
    public void GetAllSourceStatuses_MultipleSources_ReturnsAll()
    {
        using var loggerFactory = CreateLoggerFactory();
        using var service = CreateService(new CdcConfig(), loggerFactory);
        service.AddSource("first", new CdcConfig { ConnectionString = "Host=a", SlotName = "slot_1" });
        service.AddSource("second", new CdcConfig { ConnectionString = "Host=b", SlotName = "slot_2" });

        var statuses = service.GetAllSourceStatuses();

        Assert.Equal(2, statuses.Count);
        Assert.Contains(statuses, s => s.Id == "first" && s.SlotName == "slot_1");
        Assert.Contains(statuses, s => s.Id == "second" && s.SlotName == "slot_2");
    }

    [Fact]
    public async Task RemoveSourceAsync_UnknownId_ReturnsFalse()
    {
        using var loggerFactory = CreateLoggerFactory();
        using var service = CreateService(new CdcConfig(), loggerFactory);

        Assert.False(await service.RemoveSourceAsync("does-not-exist"));
    }

    [Fact]
    public async Task RemoveSourceAsync_ExistingId_RemovesSource()
    {
        using var loggerFactory = CreateLoggerFactory();
        using var service = CreateService(new CdcConfig(), loggerFactory);
        service.AddSource("to-remove", new CdcConfig { ConnectionString = "Host=localhost" });

        var removed = await service.RemoveSourceAsync("to-remove");

        Assert.True(removed);
        Assert.Null(service.GetSourceStatus("to-remove"));
        Assert.Empty(service.GetAllSourceStatuses());
    }

    [Fact]
    public async Task AddSource_AfterRemoval_SameIdCanBeReused()
    {
        using var loggerFactory = CreateLoggerFactory();
        using var service = CreateService(new CdcConfig(), loggerFactory);
        service.AddSource("reusable", new CdcConfig { ConnectionString = "Host=localhost" });
        await service.RemoveSourceAsync("reusable");

        Assert.True(service.AddSource("reusable", new CdcConfig { ConnectionString = "Host=localhost" }));
        Assert.NotNull(service.GetSourceStatus("reusable"));
    }

    [Fact]
    public async Task CapturedEvents_ReachTheSink()
    {
        // The regression test for #144. Capture used to compute the topic name,
        // serialise the event and its key, and drop all three — the source stayed
        // "Streaming", the event count rose, and nothing was ever appended. Every
        // existing test here asserted on registry state or the fault path, so none
        // of them could see it.
        using var loggerFactory = CreateLoggerFactory();
        var sink = new RecordingSink();
        var config = new CdcConfig { Enabled = true, TopicPrefix = "cdc" };
        using var service = CreateService(config, loggerFactory, sink);

        var source = new ScriptedSource([
            new CdcEvent
            {
                Operation = CdcOperation.Insert,
                Schema = "public",
                Table = "orders",
                Lsn = 42,
                Timestamp = DateTimeOffset.UnixEpoch,
                After = new Dictionary<string, object?> { ["id"] = 7 },
            },
        ]);

        Assert.True(service.AddSource("scripted", source, config));

        await service.StartAsync(CancellationToken.None);
        Assert.NotNull(service.ExecuteTask);
        await service.ExecuteTask.WaitAsync(ExecuteTimeout);
        await service.StopAsync(CancellationToken.None);

        var write = Assert.Single(sink.Writes);
        Assert.Equal(CdcTopicNaming.GetTopicName(config, "public", "orders"), write.Topic);

        // The value is the serialised event and the key is the row — asserting on
        // content, not just on "something was written", because writing the wrong
        // thing would satisfy a count.
        Assert.Contains("orders", Encoding.UTF8.GetString(write.Value), StringComparison.Ordinal);
        Assert.NotNull(write.Key);
        Assert.Contains("\"id\"", Encoding.UTF8.GetString(write.Key!), StringComparison.Ordinal);

        var status = Assert.Single(service.GetAllSourceStatuses());
        Assert.Equal(1, status.EventsCaptured);
    }

    [Fact]
    public async Task AFailingSinkFaultsTheSourceRatherThanLosingTheChange()
    {
        // A CDC event cannot be replayed: it came from a replication slot that has
        // moved on. So a sink failure has to stop the source and be visible in its
        // status — swallowing it would put the pipeline back to losing changes
        // quietly, which is the defect one layer down.
        using var loggerFactory = CreateLoggerFactory();
        var config = new CdcConfig { Enabled = true, TopicPrefix = "cdc" };
        using var service = CreateService(config, loggerFactory, new ThrowingSink());

        var source = new ScriptedSource([
            new CdcEvent { Operation = CdcOperation.Insert, Schema = "public", Table = "orders", Lsn = 1 },
        ]);
        Assert.True(service.AddSource("scripted", source, config));

        await service.StartAsync(CancellationToken.None);
        Assert.NotNull(service.ExecuteTask);
        await service.ExecuteTask.WaitAsync(ExecuteTimeout);
        await service.StopAsync(CancellationToken.None);

        var status = Assert.Single(service.GetAllSourceStatuses());
        Assert.Equal(CdcSourceState.Faulted, status.State);
        Assert.Equal(0, status.EventsCaptured);
    }

    /// <summary>A source that yields a fixed script of events and then completes.</summary>
    private sealed class ScriptedSource(IReadOnlyList<CdcEvent> events) : ICdcSource
    {
        public string DatabaseType => "Scripted";

        public async IAsyncEnumerable<CdcEvent> CaptureChangesAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
        {
            foreach (var e in events)
            {
                ct.ThrowIfCancellationRequested();
                yield return e;
                await Task.Yield();
            }
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }

    private sealed class ThrowingSink : ICdcSink
    {
        public ValueTask WriteAsync(
            string topic, ReadOnlyMemory<byte>? key, ReadOnlyMemory<byte> value, CancellationToken cancellationToken = default)
            => throw new InvalidOperationException("the topic is unavailable");
    }

    [Fact]
    public async Task ExecuteAsync_Disabled_CompletesWithoutRegisteringSources()
    {
        using var loggerFactory = CreateLoggerFactory();
        var config = new CdcConfig { Enabled = false, ConnectionString = "Host=localhost" };
        using var service = CreateService(config, loggerFactory);

        await service.StartAsync(CancellationToken.None);
        Assert.NotNull(service.ExecuteTask);
        await service.ExecuteTask.WaitAsync(ExecuteTimeout);

        // Disabled service must not register the default source even though a
        // connection string is configured.
        Assert.Empty(service.GetAllSourceStatuses());
        await service.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task ExecuteAsync_EnabledWithoutConnectionString_DoesNotRegisterDefaultSource()
    {
        using var loggerFactory = CreateLoggerFactory();
        var config = new CdcConfig { Enabled = true, ConnectionString = "" };
        using var service = CreateService(config, loggerFactory);

        await service.StartAsync(CancellationToken.None);
        Assert.NotNull(service.ExecuteTask);
        await service.ExecuteTask.WaitAsync(ExecuteTimeout);

        Assert.Empty(service.GetAllSourceStatuses());
        await service.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task ExecuteAsync_EnabledWithMalformedConnectionString_RegistersDefaultSourceAsFaulted()
    {
        using var loggerFactory = CreateLoggerFactory();
        var config = new CdcConfig { Enabled = true, ConnectionString = MalformedConnectionString };
        using var service = CreateService(config, loggerFactory);

        await service.StartAsync(CancellationToken.None);
        Assert.NotNull(service.ExecuteTask);
        await service.ExecuteTask.WaitAsync(ExecuteTimeout);

        var status = service.GetSourceStatus("default");
        Assert.NotNull(status);
        Assert.Equal(CdcSourceState.Faulted, status.State);
        Assert.NotNull(status.Error);
        Assert.NotEqual(default, status.StartedAt);
        Assert.Equal(0, status.EventsCaptured);
        await service.StopAsync(CancellationToken.None);
    }

    [Fact]
    public async Task ExecuteAsync_PreRegisteredFaultingSource_ReportsFaultedAndIsRemovable()
    {
        using var loggerFactory = CreateLoggerFactory();
        // Service config itself has no connection string, so only the explicitly
        // added source participates in the capture loop.
        var config = new CdcConfig { Enabled = true, ConnectionString = "" };
        using var service = CreateService(config, loggerFactory);
        service.AddSource("bad-source", new CdcConfig { ConnectionString = MalformedConnectionString });

        await service.StartAsync(CancellationToken.None);
        Assert.NotNull(service.ExecuteTask);
        await service.ExecuteTask.WaitAsync(ExecuteTimeout);

        var status = service.GetSourceStatus("bad-source");
        Assert.NotNull(status);
        Assert.Equal(CdcSourceState.Faulted, status.State);
        Assert.NotNull(status.Error);

        Assert.True(await service.RemoveSourceAsync("bad-source"));
        Assert.Empty(service.GetAllSourceStatuses());
        await service.StopAsync(CancellationToken.None);
    }
}
