using Kuestenlogik.Surgewave.Broker.Telemetry;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Handlers;

/// <summary>
/// Handler for Kafka client telemetry APIs (KIP-714):
/// <list type="bullet">
///   <item>GetTelemetrySubscriptions (API 71) — clients ask for the metric subscription set</item>
///   <item>PushTelemetry (API 72) — clients push metric data to the broker</item>
/// </list>
///
/// When <see cref="ClientTelemetryConfig.Enabled"/> is <c>false</c> (the default,
/// preserving the pre-G9 stub behaviour) the handler advertises an empty
/// subscription set with a long push interval — librdkafka treats this as
/// "broker says don't bother", and no payloads arrive. When enabled, the
/// handler returns the configured push interval and delegates each
/// <c>PushTelemetry</c> payload to the registered <see cref="ITelemetryIngestor"/>.
/// </summary>
public sealed partial class TelemetryApiHandler : IKafkaRequestHandler
{
    private readonly ILogger<TelemetryApiHandler> _logger;
    private readonly ClientTelemetryConfig _config;
    private readonly ITelemetryIngestor _ingestor;

    /// <summary>Backoff used when telemetry is disabled — librdkafka stops polling.</summary>
    private const int DisabledPushIntervalMs = 300_000;

    public TelemetryApiHandler(
        ILogger<TelemetryApiHandler> logger,
        ClientTelemetryConfig config,
        ITelemetryIngestor ingestor)
    {
        _logger = logger;
        _config = config;
        _ingestor = ingestor;
    }

    public IEnumerable<ApiKey> SupportedApiKeys =>
        [ApiKey.GetTelemetrySubscriptions, ApiKey.PushTelemetry, ApiKey.ListConfigResources];

    public async Task<KafkaResponse> HandleAsync(KafkaRequest request, RequestContext context, CancellationToken cancellationToken)
    {
        return request switch
        {
            GetTelemetrySubscriptionsRequest get => HandleGet(get),
            PushTelemetryRequest push => await HandlePushAsync(push, context, cancellationToken).ConfigureAwait(false),
            ListConfigResourcesRequest listResources => HandleListConfigResources(listResources),
            _ => throw new NotSupportedException($"Request type {request.ApiKey} not supported by TelemetryApiHandler")
        };
    }

    /// <summary>
    /// KIP-1106: enumerate the client-metrics subscription names the broker
    /// is currently advertising. When telemetry is disabled the response is
    /// empty (matching the empty subscription set advertised by
    /// <see cref="HandleGet"/>); when enabled, returns the configured
    /// metric prefixes from <see cref="ClientTelemetryConfig.RequestedMetrics"/>.
    /// </summary>
    private ListConfigResourcesResponse HandleListConfigResources(ListConfigResourcesRequest request)
    {
        var resources = new List<ListConfigResourcesResponse.ConfigResource>();
        if (_config.Enabled)
        {
            foreach (var metric in _config.RequestedMetrics)
            {
                if (string.IsNullOrEmpty(metric)) continue;
                resources.Add(new ListConfigResourcesResponse.ConfigResource { Name = metric });
            }
        }

        return new ListConfigResourcesResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.None,
            ConfigResources = resources,
        };
    }

    private GetTelemetrySubscriptionsResponse HandleGet(GetTelemetrySubscriptionsRequest request)
    {
        // If the client sent an all-zero UUID it's the very first request — assign one.
        var instanceId = request.ClientInstanceId == Guid.Empty
            ? Guid.NewGuid()
            : request.ClientInstanceId;

        if (!_config.Enabled)
        {
            LogIgnoringClient(instanceId);
            return new GetTelemetrySubscriptionsResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ThrottleTimeMs = 0,
                ErrorCode = ErrorCode.None,
                ClientInstanceId = instanceId,
                SubscriptionId = 0,
                AcceptedCompressionTypes = [0],
                PushIntervalMs = DisabledPushIntervalMs,
                TelemetryMaxBytes = _config.MaxBytes,
                DeltaTemporality = false,
                RequestedMetrics = []
            };
        }

        LogSubscribing(instanceId, _config.PushIntervalMs);
        return new GetTelemetrySubscriptionsResponse
        {
            CorrelationId = request.CorrelationId,
            ApiVersion = request.ApiVersion,
            ThrottleTimeMs = 0,
            ErrorCode = ErrorCode.None,
            ClientInstanceId = instanceId,
            // SubscriptionId is opaque to clients — we use a stable hash of
            // the configured metric set so a config change forces re-subscribe.
            SubscriptionId = ComputeSubscriptionId(_config.RequestedMetrics, _config.PushIntervalMs),
            AcceptedCompressionTypes = [0, 1, 2, 3, 4], // none, gzip, snappy, lz4, zstd
            PushIntervalMs = _config.PushIntervalMs,
            TelemetryMaxBytes = _config.MaxBytes,
            DeltaTemporality = false,
            RequestedMetrics = _config.RequestedMetrics.Count == 0
                ? [""]                        // empty string = "all metrics"
                : [.. _config.RequestedMetrics]
        };
    }

    private async Task<PushTelemetryResponse> HandlePushAsync(
        PushTelemetryRequest request,
        RequestContext context,
        CancellationToken cancellationToken)
    {
        if (!_config.Enabled)
        {
            // Mirrors the disabled-path behaviour of GetTelemetry: accept the push,
            // discard the payload, and return success so the client keeps its
            // backoff. If the client missed our long-interval subscription it
            // shouldn't crash.
            return SuccessResponse(request);
        }

        if (request.Metrics.Length > _config.MaxBytes)
        {
            LogPayloadTooLarge(request.ClientInstanceId, request.Metrics.Length, _config.MaxBytes);
            return new PushTelemetryResponse
            {
                CorrelationId = request.CorrelationId,
                ApiVersion = request.ApiVersion,
                ThrottleTimeMs = 0,
                ErrorCode = ErrorCode.MessageTooLarge,
            };
        }

        var ev = new TelemetryPushEvent
        {
            ClientInstanceId = request.ClientInstanceId,
            ClientId = context.ClientId ?? string.Empty,
            SubscriptionId = request.SubscriptionId,
            CompressionType = request.CompressionType,
            MetricsPayload = request.Metrics,
            Terminating = request.Terminating,
        };

        try
        {
            await _ingestor.IngestAsync(ev, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // Per the interface contract ingestors must not throw, but defend in
            // depth — a misbehaving plugin must never bring down the wire.
            LogIngestorError(ex, request.ClientInstanceId);
        }

        return SuccessResponse(request);
    }

    private static PushTelemetryResponse SuccessResponse(PushTelemetryRequest request) => new()
    {
        CorrelationId = request.CorrelationId,
        ApiVersion = request.ApiVersion,
        ThrottleTimeMs = 0,
        ErrorCode = ErrorCode.None,
    };

    private static int ComputeSubscriptionId(IReadOnlyList<string> metrics, int pushIntervalMs)
    {
        // Deterministic hash so the subscription id is stable for a given
        // configuration and changes when the operator edits the metric list
        // or push interval. Clients that see a new id will re-fetch the
        // subscription document — which is the desired behaviour after a
        // config flip.
        unchecked
        {
            var hash = 17 * 31 + pushIntervalMs;
            for (var i = 0; i < metrics.Count; i++) hash = hash * 31 + metrics[i].GetHashCode(StringComparison.Ordinal);
            return hash;
        }
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "GetTelemetrySubscriptions: clientInstanceId={ClientInstanceId} — telemetry disabled, advertising empty subscription")]
    private partial void LogIgnoringClient(Guid clientInstanceId);

    [LoggerMessage(Level = LogLevel.Information, Message = "GetTelemetrySubscriptions: clientInstanceId={ClientInstanceId} subscribed (pushIntervalMs={PushIntervalMs})")]
    private partial void LogSubscribing(Guid clientInstanceId, int pushIntervalMs);

    [LoggerMessage(Level = LogLevel.Warning, Message = "PushTelemetry rejected: clientInstanceId={ClientInstanceId} payload {Bytes} bytes exceeds MaxBytes={MaxBytes}")]
    private partial void LogPayloadTooLarge(Guid clientInstanceId, int bytes, int maxBytes);

    [LoggerMessage(Level = LogLevel.Error, Message = "Telemetry ingestor threw for clientInstanceId={ClientInstanceId}")]
    private partial void LogIngestorError(Exception ex, Guid clientInstanceId);
}
