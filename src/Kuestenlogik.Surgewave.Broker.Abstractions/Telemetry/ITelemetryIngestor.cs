namespace Kuestenlogik.Surgewave.Broker.Telemetry;

/// <summary>
/// Server-side endpoint for KIP-714 <c>PushTelemetry</c> payloads. The broker
/// hands the raw OTLP-encoded MetricsData blob to an ingestor; the default
/// implementation logs a summary and increments a per-client byte counter.
/// Operators who want to forward the metrics to an OTLP collector or store
/// them in an internal topic plug in their own ingestor through DI.
/// </summary>
public interface ITelemetryIngestor
{
    /// <summary>
    /// Process a single telemetry push. The payload bytes belong to the
    /// caller's pool — implementations that need to retain them must copy.
    /// Failures must NOT throw; the broker logs and continues.
    /// </summary>
    ValueTask IngestAsync(TelemetryPushEvent push, CancellationToken cancellationToken);
}
