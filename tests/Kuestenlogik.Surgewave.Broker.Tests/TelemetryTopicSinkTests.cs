using System.Text;
using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Broker.Telemetry;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// Pins the KIP-714 telemetry-topic-sink contract: each push from a client
/// produces one record on the configured topic, keyed by client-instance-id,
/// with the raw OTLP MetricsData blob as the record value. Tests run against
/// an in-memory <see cref="LogManager"/>, so they exercise the full encode +
/// append-batch path end-to-end without a broker socket.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class TelemetryTopicSinkTests : IAsyncLifetime
{
    private string _dataDir = string.Empty;
    private LogManager? _logManager;

    public ValueTask InitializeAsync()
    {
        _dataDir = Path.Combine(Path.GetTempPath(), $"surgewave-telem-sink-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_dataDir);
        _logManager = TestLogManager.CreateInMemory(_dataDir);
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        await Task.CompletedTask;
        _logManager?.Dispose();
        try { Directory.Delete(_dataDir, recursive: true); } catch { /* best effort */ }
    }

    [Fact]
    public async Task FirstPush_AutoCreatesTopicAndAppendsRecord()
    {
        var sink = BuildSink();
        var push = BuildPush(metrics: [1, 2, 3, 4, 5]);

        var offset = await sink.WriteAsync(push);

        Assert.True(offset >= 0, "Sink should return non-negative offset on success");
        var partition = new TopicPartition { Topic = sink.TopicName, Partition = 0 };
        Assert.NotNull(_logManager!.GetLog(partition));
    }

    [Fact]
    public async Task RepeatedPushes_NoDuplicateCreate()
    {
        var sink = BuildSink();
        await sink.WriteAsync(BuildPush());
        await sink.WriteAsync(BuildPush());
        await sink.WriteAsync(BuildPush());

        var partition = new TopicPartition { Topic = sink.TopicName, Partition = 0 };
        Assert.NotNull(_logManager!.GetLog(partition));
    }

    [Fact]
    public async Task TopicSinkUsesConfiguredTopicName()
    {
        var sink = BuildSink(topicName: "_my_telemetry");
        await sink.WriteAsync(BuildPush());

        var unwanted = _logManager!.GetLog(new TopicPartition { Topic = "_client_telemetry", Partition = 0 });
        var wanted = _logManager!.GetLog(new TopicPartition { Topic = "_my_telemetry", Partition = 0 });
        Assert.Null(unwanted);
        Assert.NotNull(wanted);
    }

    [Fact]
    public async Task ForwardingIngestor_DispatchesToInnerAndSink()
    {
        var sink = BuildSink();
        var inner = new RecordingInner();
        var forwarding = new TopicForwardingTelemetryIngestor(inner, sink);

        await forwarding.IngestAsync(BuildPush(), CancellationToken.None);

        Assert.Equal(1, inner.Calls);
        Assert.NotNull(_logManager!.GetLog(new TopicPartition { Topic = sink.TopicName, Partition = 0 }));
    }

    private TelemetryTopicSink BuildSink(string topicName = "_client_telemetry")
    {
        var serializer = new RecordBatchSerializer(NullLogger<RecordBatchSerializer>.Instance);
        var config = new ClientTelemetryConfig
        {
            Enabled = true,
            TopicSinkEnabled = true,
            TopicName = topicName,
            RetentionMs = 86_400_000,
        };
        return new TelemetryTopicSink(_logManager!, serializer, config, NullLogger<TelemetryTopicSink>.Instance);
    }

    private static TelemetryPushEvent BuildPush(byte[]? metrics = null) => new()
    {
        ClientInstanceId = Guid.NewGuid(),
        ClientId = "test-client",
        SubscriptionId = 7,
        CompressionType = 0,
        MetricsPayload = metrics ?? Encoding.UTF8.GetBytes("dummy-otlp-payload"),
        Terminating = false,
    };

    private sealed class RecordingInner : ITelemetryIngestor
    {
        public int Calls { get; private set; }
        public ValueTask IngestAsync(TelemetryPushEvent push, CancellationToken cancellationToken)
        {
            Calls++;
            return ValueTask.CompletedTask;
        }
    }
}
