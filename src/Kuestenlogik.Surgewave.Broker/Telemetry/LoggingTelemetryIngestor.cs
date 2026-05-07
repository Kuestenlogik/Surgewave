using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Telemetry;

/// <summary>
/// Default <see cref="ITelemetryIngestor"/>: logs a summary line per push
/// and increments two counters on the broker meter so operators can see
/// in their dashboards how many clients are pushing telemetry and how
/// many bytes they're delivering. The OTLP payload itself is NOT decoded
/// — that's a follow-up that needs the OpenTelemetry.Proto package and a
/// metric-shape mapping. Without decoding the broker still surfaces "is
/// telemetry flowing" answers, which is the operationally interesting
/// question; decoding gives the per-metric values, which most operators
/// already collect through their own client-side OTLP exporter.
/// </summary>
public sealed partial class LoggingTelemetryIngestor : ITelemetryIngestor, IDisposable
{
    public const string MeterName = "Kuestenlogik.Surgewave.Broker.ClientTelemetry";

    private readonly ILogger<LoggingTelemetryIngestor> _logger;
    private readonly Meter _meter;
    private readonly Counter<long> _pushesReceived;
    private readonly Counter<long> _bytesReceived;
    private readonly Counter<long> _terminatingPushes;
    private bool _disposed;

    public LoggingTelemetryIngestor(ILogger<LoggingTelemetryIngestor> logger)
    {
        _logger = logger;
        _meter = new Meter(MeterName);
        // Untagged counters — KIP-714 client-instance-ids are unique per
        // process and would explode metric cardinality if used as tags.
        // Operators who need per-client breakdowns should plug in a custom
        // ingestor that mirrors to the telemetry topic instead.
        _pushesReceived = _meter.CreateCounter<long>(
            "surgewave.broker.client_telemetry.pushes_received",
            description: "Number of KIP-714 PushTelemetry requests received from clients.");
        _bytesReceived = _meter.CreateCounter<long>(
            "surgewave.broker.client_telemetry.bytes_received",
            unit: "By",
            description: "Total bytes of OTLP-encoded telemetry received from clients.");
        _terminatingPushes = _meter.CreateCounter<long>(
            "surgewave.broker.client_telemetry.terminating_pushes",
            description: "Final pushes received before client shutdown — useful to confirm clean disconnects.");
    }

    public ValueTask IngestAsync(TelemetryPushEvent push, CancellationToken cancellationToken)
    {
        _pushesReceived.Add(1);
        _bytesReceived.Add(push.MetricsPayload.Length);
        if (push.Terminating)
        {
            _terminatingPushes.Add(1);
            LogTerminatingPush(push.ClientInstanceId, push.ClientId, push.MetricsPayload.Length);
        }
        else
        {
            LogPushReceived(push.ClientInstanceId, push.ClientId, push.MetricsPayload.Length, push.CompressionType);
        }
        return ValueTask.CompletedTask;
    }

    public void Dispose()
    {
        if (_disposed) return;
        _meter.Dispose();
        _disposed = true;
    }

    [LoggerMessage(Level = LogLevel.Debug,
        Message = "PushTelemetry received: clientInstanceId={ClientInstanceId}, clientId={ClientId}, bytes={Bytes}, compression={CompressionType}")]
    private partial void LogPushReceived(Guid clientInstanceId, string clientId, int bytes, sbyte compressionType);

    [LoggerMessage(Level = LogLevel.Information,
        Message = "PushTelemetry terminating: clientInstanceId={ClientInstanceId}, clientId={ClientId}, finalBytes={Bytes}")]
    private partial void LogTerminatingPush(Guid clientInstanceId, string clientId, int bytes);
}
