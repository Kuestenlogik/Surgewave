using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Kuestenlogik.Surgewave.Gateway;

/// <summary>
/// Gateway telemetry using System.Diagnostics.Metrics and ActivitySource (OpenTelemetry compatible).
/// Uses only System.Diagnostics - no OpenTelemetry SDK dependencies.
/// </summary>
public sealed class GatewayMetrics : IDisposable
{
    public const string MeterName = "Kuestenlogik.Surgewave.Gateway";
    public const string ActivitySourceName = "Kuestenlogik.Surgewave.Gateway";

    private readonly Meter _meter;
    private readonly ActivitySource _activitySource;

    // === gRPC Request Metrics ===
    private readonly Counter<long> _grpcRequestsTotal;
    private readonly Histogram<double> _grpcRequestDuration;
    private readonly Counter<long> _grpcErrorsTotal;

    // === Produce Metrics ===
    private readonly Counter<long> _messagesProducedTotal;
    private readonly Counter<long> _bytesProducedTotal;
    private readonly Histogram<double> _produceLatency;

    // === Fetch Metrics ===
    private readonly Counter<long> _messagesFetchedTotal;
    private readonly Counter<long> _bytesFetchedTotal;
    private readonly Histogram<double> _fetchLatency;

    // === WebSocket Metrics ===
    private readonly Counter<long> _wsConnectionsTotal;
    private readonly UpDownCounter<int> _wsConnectionsActive;
    private readonly Counter<long> _wsMessagesReceivedTotal;
    private readonly Counter<long> _wsMessagesSentTotal;
    private readonly Counter<long> _wsBytesReceivedTotal;
    private readonly Counter<long> _wsBytesSentTotal;
    private readonly Histogram<double> _wsMessageLatency;
    private readonly Counter<long> _wsSubscriptionsTotal;
    private readonly UpDownCounter<int> _wsSubscriptionsActive;
    private readonly Counter<long> _wsErrorsTotal;

    // === Broker Connection Metrics ===
    private readonly UpDownCounter<int> _brokerConnectionsActive;
    private readonly Counter<long> _brokerConnectionFailures;
    private readonly Histogram<double> _brokerRequestLatency;

    // === Cluster Registry Metrics ===
    private readonly ObservableGauge<int> _registeredClusters;
    private Func<int>? _getClusterCount;

    public GatewayMetrics()
    {
        _meter = new Meter(MeterName, "1.0.0");
        _activitySource = new ActivitySource(ActivitySourceName, "1.0.0");

        // === gRPC Request Metrics ===
        _grpcRequestsTotal = _meter.CreateCounter<long>(
            "gateway_grpc_requests_total",
            description: "Total gRPC requests handled by gateway");

        _grpcRequestDuration = _meter.CreateHistogram<double>(
            "gateway_grpc_request_duration_ms",
            unit: "ms",
            description: "gRPC request duration in milliseconds");

        _grpcErrorsTotal = _meter.CreateCounter<long>(
            "gateway_grpc_errors_total",
            description: "Total gRPC errors by service and method");

        // === Produce Metrics ===
        _messagesProducedTotal = _meter.CreateCounter<long>(
            "gateway_messages_produced_total",
            description: "Total messages produced through gateway");

        _bytesProducedTotal = _meter.CreateCounter<long>(
            "gateway_bytes_produced_total",
            unit: "By",
            description: "Total bytes produced through gateway");

        _produceLatency = _meter.CreateHistogram<double>(
            "gateway_produce_latency_ms",
            unit: "ms",
            description: "End-to-end produce latency through gateway");

        // === Fetch Metrics ===
        _messagesFetchedTotal = _meter.CreateCounter<long>(
            "gateway_messages_fetched_total",
            description: "Total messages fetched through gateway");

        _bytesFetchedTotal = _meter.CreateCounter<long>(
            "gateway_bytes_fetched_total",
            unit: "By",
            description: "Total bytes fetched through gateway");

        _fetchLatency = _meter.CreateHistogram<double>(
            "gateway_fetch_latency_ms",
            unit: "ms",
            description: "End-to-end fetch latency through gateway");

        // === WebSocket Metrics ===
        _wsConnectionsTotal = _meter.CreateCounter<long>(
            "gateway_ws_connections_total",
            description: "Total WebSocket connections established");

        _wsConnectionsActive = _meter.CreateUpDownCounter<int>(
            "gateway_ws_connections_active",
            description: "Current active WebSocket connections");

        _wsMessagesReceivedTotal = _meter.CreateCounter<long>(
            "gateway_ws_messages_received_total",
            description: "Total WebSocket messages received");

        _wsMessagesSentTotal = _meter.CreateCounter<long>(
            "gateway_ws_messages_sent_total",
            description: "Total WebSocket messages sent");

        _wsBytesReceivedTotal = _meter.CreateCounter<long>(
            "gateway_ws_bytes_received_total",
            unit: "By",
            description: "Total bytes received via WebSocket");

        _wsBytesSentTotal = _meter.CreateCounter<long>(
            "gateway_ws_bytes_sent_total",
            unit: "By",
            description: "Total bytes sent via WebSocket");

        _wsMessageLatency = _meter.CreateHistogram<double>(
            "gateway_ws_message_latency_ms",
            unit: "ms",
            description: "WebSocket message processing latency");

        _wsSubscriptionsTotal = _meter.CreateCounter<long>(
            "gateway_ws_subscriptions_total",
            description: "Total WebSocket subscriptions created");

        _wsSubscriptionsActive = _meter.CreateUpDownCounter<int>(
            "gateway_ws_subscriptions_active",
            description: "Current active WebSocket subscriptions");

        _wsErrorsTotal = _meter.CreateCounter<long>(
            "gateway_ws_errors_total",
            description: "Total WebSocket errors");

        // === Broker Connection Metrics ===
        _brokerConnectionsActive = _meter.CreateUpDownCounter<int>(
            "gateway_broker_connections_active",
            description: "Active connections to upstream brokers");

        _brokerConnectionFailures = _meter.CreateCounter<long>(
            "gateway_broker_connection_failures_total",
            description: "Broker connection failures");

        _brokerRequestLatency = _meter.CreateHistogram<double>(
            "gateway_broker_request_latency_ms",
            unit: "ms",
            description: "Latency of requests to upstream broker");

        // === Cluster Registry ===
        _registeredClusters = _meter.CreateObservableGauge(
            "gateway_clusters_registered",
            () => _getClusterCount?.Invoke() ?? 0,
            description: "Number of registered clusters");
    }

    /// <summary>
    /// Register accessor for cluster count.
    /// </summary>
    public void RegisterClusterCountAccessor(Func<int> getClusterCount)
    {
        _getClusterCount = getClusterCount;
    }

    // === Activity/Tracing Methods ===

    public Activity? StartGrpcRequestActivity(string service, string method)
    {
        var activity = _activitySource.StartActivity($"gateway.grpc.{service}.{method}", ActivityKind.Server);
        activity?.SetTag("rpc.system", "grpc");
        activity?.SetTag("rpc.service", service);
        activity?.SetTag("rpc.method", method);
        return activity;
    }

    public Activity? StartProduceActivity(string topic, int partition, string? clusterId = null)
    {
        var activity = _activitySource.StartActivity("gateway.produce", ActivityKind.Producer);
        activity?.SetTag("messaging.system", "surgewave");
        activity?.SetTag("messaging.destination.name", topic);
        activity?.SetTag("messaging.destination.partition.id", partition);
        activity?.SetTag("messaging.operation", "publish");
        if (clusterId != null)
            activity?.SetTag("surgewave.cluster_id", clusterId);
        return activity;
    }

    public Activity? StartFetchActivity(string topic, int partition, string? clusterId = null)
    {
        var activity = _activitySource.StartActivity("gateway.fetch", ActivityKind.Consumer);
        activity?.SetTag("messaging.system", "surgewave");
        activity?.SetTag("messaging.source.name", topic);
        activity?.SetTag("messaging.source.partition.id", partition);
        activity?.SetTag("messaging.operation", "receive");
        if (clusterId != null)
            activity?.SetTag("surgewave.cluster_id", clusterId);
        return activity;
    }

    public Activity? StartWebSocketSessionActivity(string sessionId, string? clusterId = null)
    {
        var activity = _activitySource.StartActivity("gateway.websocket.session", ActivityKind.Server);
        activity?.SetTag("surgewave.session_id", sessionId);
        activity?.SetTag("surgewave.transport", "websocket");
        if (clusterId != null)
            activity?.SetTag("surgewave.cluster_id", clusterId);
        return activity;
    }

    public Activity? StartWebSocketMessageActivity(string sessionId, string messageType)
    {
        var activity = _activitySource.StartActivity($"gateway.websocket.{messageType}", ActivityKind.Server);
        activity?.SetTag("surgewave.session_id", sessionId);
        activity?.SetTag("surgewave.message_type", messageType);
        return activity;
    }

    public Activity? StartBrokerRequestActivity(string clusterId, string operation)
    {
        var activity = _activitySource.StartActivity("gateway.broker.request", ActivityKind.Client);
        activity?.SetTag("surgewave.cluster_id", clusterId);
        activity?.SetTag("surgewave.operation", operation);
        return activity;
    }

    // === gRPC Recording Methods ===

    public void RecordGrpcRequest(string service, string method, double durationMs, bool success)
    {
        var tags = new TagList { { "service", service }, { "method", method } };
        _grpcRequestsTotal.Add(1, tags);
        _grpcRequestDuration.Record(durationMs, tags);
        if (!success)
        {
            _grpcErrorsTotal.Add(1, tags);
        }
    }

    // === Produce Recording Methods ===

    public void RecordProduce(string topic, int partition, int messageCount, long bytes, double latencyMs)
    {
        var tags = new TagList { { "topic", topic }, { "partition", partition } };
        _messagesProducedTotal.Add(messageCount, tags);
        _bytesProducedTotal.Add(bytes, tags);
        _produceLatency.Record(latencyMs, tags);
    }

    public void RecordProduceError(string topic, int partition, string errorCode)
    {
        _grpcErrorsTotal.Add(1, new TagList
        {
            { "topic", topic },
            { "partition", partition },
            { "error_code", errorCode }
        });
    }

    // === Fetch Recording Methods ===

    public void RecordFetch(string topic, int partition, int messageCount, long bytes, double latencyMs)
    {
        var tags = new TagList { { "topic", topic }, { "partition", partition } };
        _messagesFetchedTotal.Add(messageCount, tags);
        _bytesFetchedTotal.Add(bytes, tags);
        _fetchLatency.Record(latencyMs, tags);
    }

    // === WebSocket Recording Methods ===

    public void RecordWsConnectionOpened(string? clusterId = null)
    {
        var tags = clusterId != null ? new TagList { { "cluster_id", clusterId } } : default;
        _wsConnectionsTotal.Add(1, tags);
        _wsConnectionsActive.Add(1, tags);
    }

    public void RecordWsConnectionClosed(string? clusterId = null)
    {
        var tags = clusterId != null ? new TagList { { "cluster_id", clusterId } } : default;
        _wsConnectionsActive.Add(-1, tags);
    }

    public void RecordWsMessageReceived(string messageType, int bytes)
    {
        var tags = new TagList { { "message_type", messageType } };
        _wsMessagesReceivedTotal.Add(1, tags);
        _wsBytesReceivedTotal.Add(bytes, tags);
    }

    public void RecordWsMessageSent(string messageType, int bytes)
    {
        var tags = new TagList { { "message_type", messageType } };
        _wsMessagesSentTotal.Add(1, tags);
        _wsBytesSentTotal.Add(bytes, tags);
    }

    public void RecordWsMessageLatency(string messageType, double latencyMs)
    {
        _wsMessageLatency.Record(latencyMs, new TagList { { "message_type", messageType } });
    }

    public void RecordWsSubscriptionCreated(string topic)
    {
        var tags = new TagList { { "topic", topic } };
        _wsSubscriptionsTotal.Add(1, tags);
        _wsSubscriptionsActive.Add(1, tags);
    }

    public void RecordWsSubscriptionClosed(string topic)
    {
        _wsSubscriptionsActive.Add(-1, new TagList { { "topic", topic } });
    }

    public void RecordWsError(string errorCode)
    {
        _wsErrorsTotal.Add(1, new TagList { { "error_code", errorCode } });
    }

    // === Broker Connection Recording Methods ===

    public void RecordBrokerConnectionOpened(string clusterId)
    {
        _brokerConnectionsActive.Add(1, new TagList { { "cluster_id", clusterId } });
    }

    public void RecordBrokerConnectionClosed(string clusterId)
    {
        _brokerConnectionsActive.Add(-1, new TagList { { "cluster_id", clusterId } });
    }

    public void RecordBrokerConnectionFailure(string clusterId, string reason)
    {
        _brokerConnectionFailures.Add(1, new TagList
        {
            { "cluster_id", clusterId },
            { "reason", reason }
        });
    }

    public void RecordBrokerRequestLatency(string clusterId, string operation, double latencyMs)
    {
        _brokerRequestLatency.Record(latencyMs, new TagList
        {
            { "cluster_id", clusterId },
            { "operation", operation }
        });
    }

    public void Dispose()
    {
        _meter.Dispose();
        _activitySource.Dispose();
    }
}
