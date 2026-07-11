using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Protocol.Kafka.Handlers;
using Kuestenlogik.Surgewave.Broker.Security;
using Kuestenlogik.Surgewave.Broker.Telemetry;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// KIP-714 client-telemetry ingestion (G9 follow-up). Pins down two contracts:
/// <list type="bullet">
///   <item>When telemetry is disabled (the default), the broker advertises an
///         empty subscription with the long backoff and discards any pushes —
///         exactly the pre-G9 stub semantics so existing deployments don't
///         start receiving payloads after a Surgewave upgrade.</item>
///   <item>When enabled, GetTelemetrySubscriptions returns the configured
///         push interval / metric set, and PushTelemetry is delegated to the
///         registered ingestor with the right shape.</item>
/// </list>
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class TelemetryApiHandlerTests
{
    [Fact]
    public async Task GetSubscriptions_Disabled_ReturnsEmptySetWithLongBackoff()
    {
        var (handler, ingestor) = Build(enabled: false);

        var resp = (GetTelemetrySubscriptionsResponse)await handler.HandleAsync(
            new GetTelemetrySubscriptionsRequest
            {
                ApiKey = ApiKey.GetTelemetrySubscriptions,
                ApiVersion = 0,
                CorrelationId = 1,
                ClientId = "c1",
                ClientInstanceId = Guid.Empty, // first request — server assigns
            },
            BuildContext(),
            CancellationToken.None);

        Assert.Equal(ErrorCode.None, resp.ErrorCode);
        Assert.NotEqual(Guid.Empty, resp.ClientInstanceId);
        Assert.Equal(300_000, resp.PushIntervalMs);
        Assert.Empty(resp.RequestedMetrics);
        Assert.Equal(0, ingestor.Calls);
    }

    [Fact]
    public async Task GetSubscriptions_Enabled_ReturnsConfiguredIntervalAndMetricSet()
    {
        var (handler, _) = Build(
            enabled: true,
            pushIntervalMs: 5_000,
            requestedMetrics: ["org.apache.kafka.producer.*"]);

        var resp = (GetTelemetrySubscriptionsResponse)await handler.HandleAsync(
            new GetTelemetrySubscriptionsRequest
            {
                ApiKey = ApiKey.GetTelemetrySubscriptions,
                ApiVersion = 0,
                CorrelationId = 1,
                ClientId = "c1",
                ClientInstanceId = Guid.Empty,
            },
            BuildContext(),
            CancellationToken.None);

        Assert.Equal(ErrorCode.None, resp.ErrorCode);
        Assert.Equal(5_000, resp.PushIntervalMs);
        Assert.Equal(["org.apache.kafka.producer.*"], resp.RequestedMetrics);
    }

    [Fact]
    public async Task GetSubscriptions_PreservesClientInstanceIdAcrossRequests()
    {
        var (handler, _) = Build(enabled: true);
        var existing = Guid.NewGuid();

        var resp = (GetTelemetrySubscriptionsResponse)await handler.HandleAsync(
            new GetTelemetrySubscriptionsRequest
            {
                ApiKey = ApiKey.GetTelemetrySubscriptions,
                ApiVersion = 0,
                CorrelationId = 1,
                ClientId = "c1",
                ClientInstanceId = existing,
            },
            BuildContext(),
            CancellationToken.None);

        Assert.Equal(existing, resp.ClientInstanceId);
    }

    [Fact]
    public async Task PushTelemetry_Disabled_AcceptsAndDiscardsWithoutCallingIngestor()
    {
        var (handler, ingestor) = Build(enabled: false);

        var resp = (PushTelemetryResponse)await handler.HandleAsync(
            BuildPush(),
            BuildContext(),
            CancellationToken.None);

        Assert.Equal(ErrorCode.None, resp.ErrorCode);
        Assert.Equal(0, ingestor.Calls);
    }

    [Fact]
    public async Task PushTelemetry_Enabled_RoutesPayloadToIngestor()
    {
        var (handler, ingestor) = Build(enabled: true);

        var resp = (PushTelemetryResponse)await handler.HandleAsync(
            BuildPush(metrics: [1, 2, 3, 4]),
            BuildContext(),
            CancellationToken.None);

        Assert.Equal(ErrorCode.None, resp.ErrorCode);
        Assert.Equal(1, ingestor.Calls);
        Assert.Equal(4, ingestor.LastPayload.Length);
    }

    [Fact]
    public async Task PushTelemetry_PayloadOverMaxBytes_ReturnsMessageTooLarge()
    {
        var (handler, ingestor) = Build(enabled: true, maxBytes: 4);

        var resp = (PushTelemetryResponse)await handler.HandleAsync(
            BuildPush(metrics: new byte[10]),
            BuildContext(),
            CancellationToken.None);

        Assert.Equal(ErrorCode.MessageTooLarge, resp.ErrorCode);
        Assert.Equal(0, ingestor.Calls); // ingestor must NOT see oversized payloads
    }

    [Fact]
    public async Task PushTelemetry_IngestorThrows_DoesNotPropagateToWire()
    {
        var (handler, ingestor) = Build(enabled: true);
        ingestor.ShouldThrow = true;

        var resp = (PushTelemetryResponse)await handler.HandleAsync(
            BuildPush(),
            BuildContext(),
            CancellationToken.None);

        // Ingestor failures must never surface as SASL/wire errors — log and
        // return success so the client keeps producing.
        Assert.Equal(ErrorCode.None, resp.ErrorCode);
    }

    private static (TelemetryApiHandler handler, RecordingIngestor ingestor) Build(
        bool enabled,
        int pushIntervalMs = 30_000,
        int maxBytes = 1024 * 1024,
        List<string>? requestedMetrics = null)
    {
        var config = new ClientTelemetryConfig
        {
            Enabled = enabled,
            PushIntervalMs = pushIntervalMs,
            MaxBytes = maxBytes,
            RequestedMetrics = requestedMetrics ?? [],
        };
        var ingestor = new RecordingIngestor();
        var handler = new TelemetryApiHandler(NullLogger<TelemetryApiHandler>.Instance, config, ingestor);
        return (handler, ingestor);
    }

    private static RequestContext BuildContext() => new()
    {
        ConnectionState = new ConnectionState("test-host"),
        ClientId = "c1",
    };

    private static PushTelemetryRequest BuildPush(byte[]? metrics = null) => new()
    {
        ApiKey = ApiKey.PushTelemetry,
        ApiVersion = 0,
        CorrelationId = 7,
        ClientId = "c1",
        ClientInstanceId = Guid.NewGuid(),
        SubscriptionId = 0,
        Terminating = false,
        CompressionType = 0,
        Metrics = metrics ?? [1, 2, 3],
    };

    private sealed class RecordingIngestor : ITelemetryIngestor
    {
        public int Calls { get; private set; }
        public byte[] LastPayload { get; private set; } = [];
        public bool ShouldThrow { get; set; }

        public ValueTask IngestAsync(TelemetryPushEvent push, CancellationToken cancellationToken)
        {
            Calls++;
            LastPayload = push.MetricsPayload.ToArray();
            if (ShouldThrow) throw new InvalidOperationException("stub failure");
            return ValueTask.CompletedTask;
        }
    }
}
