using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Kuestenlogik.Surgewave.Core.Dlq;

/// <summary>
/// Metrics for Dead Letter Queue operations.
/// </summary>
public sealed class DlqMetrics : IDisposable
{
    public const string MeterName = "Kuestenlogik.Surgewave.Dlq";

    private readonly Meter _meter;
    private readonly Counter<long> _dlqMessagesTotal;
    private readonly Counter<long> _dlqBytesTotal;
    private readonly Counter<long> _dlqRoutingFailuresTotal;
    private readonly Histogram<double> _dlqRoutingLatency;

    public DlqMetrics()
    {
        _meter = new Meter(MeterName, "1.0.0");

        _dlqMessagesTotal = _meter.CreateCounter<long>(
            "surgewave_dlq_messages_total",
            unit: "{messages}",
            description: "Total messages routed to Dead Letter Queues");

        _dlqBytesTotal = _meter.CreateCounter<long>(
            "surgewave_dlq_bytes_total",
            unit: "By",
            description: "Total bytes routed to Dead Letter Queues");

        _dlqRoutingFailuresTotal = _meter.CreateCounter<long>(
            "surgewave_dlq_routing_failures_total",
            unit: "{failures}",
            description: "Total failed attempts to route messages to DLQ");

        _dlqRoutingLatency = _meter.CreateHistogram<double>(
            "surgewave_dlq_routing_latency_ms",
            unit: "ms",
            description: "Latency of DLQ routing operations");
    }

    /// <summary>
    /// Record a message successfully routed to DLQ.
    /// </summary>
    public void RecordDlqMessage(string originalTopic, string sourceType, long bytes)
    {
        var tags = new TagList
        {
            { "original_topic", originalTopic },
            { "source_type", sourceType }
        };

        _dlqMessagesTotal.Add(1, tags);
        _dlqBytesTotal.Add(bytes, tags);
    }

    /// <summary>
    /// Record a failed DLQ routing attempt.
    /// </summary>
    public void RecordRoutingFailure(string originalTopic, string sourceType, string errorType)
    {
        _dlqRoutingFailuresTotal.Add(1, new TagList
        {
            { "original_topic", originalTopic },
            { "source_type", sourceType },
            { "error_type", errorType }
        });
    }

    /// <summary>
    /// Record the latency of a DLQ routing operation.
    /// </summary>
    public void RecordRoutingLatency(string originalTopic, double latencyMs)
    {
        _dlqRoutingLatency.Record(latencyMs, new TagList
        {
            { "original_topic", originalTopic }
        });
    }

    public void Dispose()
    {
        _meter.Dispose();
    }
}
