namespace Kuestenlogik.Surgewave.Broker.Telemetry;

/// <summary>
/// Telemetry ingestor that delegates to an inner ingestor (typically the
/// logging/meter default) and ALSO mirrors the raw OTLP payload to a Surgewave
/// topic. The split lets operators keep "is telemetry flowing" in their
/// existing dashboards via the inner ingestor's counters while a downstream
/// OTLP collector consumes the topic for the actual metric values.
/// Failures from the topic sink never propagate — the inner ingestor's
/// observation already happened.
/// </summary>
public sealed class TopicForwardingTelemetryIngestor : ITelemetryIngestor
{
    private readonly ITelemetryIngestor _inner;
    private readonly TelemetryTopicSink _sink;

    public TopicForwardingTelemetryIngestor(ITelemetryIngestor inner, TelemetryTopicSink sink)
    {
        _inner = inner;
        _sink = sink;
    }

    public async ValueTask IngestAsync(TelemetryPushEvent push, CancellationToken cancellationToken)
    {
        await _inner.IngestAsync(push, cancellationToken).ConfigureAwait(false);
        await _sink.WriteAsync(push, cancellationToken).ConfigureAwait(false);
    }
}
