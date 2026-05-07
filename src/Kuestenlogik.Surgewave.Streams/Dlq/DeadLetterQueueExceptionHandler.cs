using System.Text.Json;
using Kuestenlogik.Surgewave.Streams.ExceptionHandling;
using Kuestenlogik.Surgewave.Streams.Runtime;

namespace Kuestenlogik.Surgewave.Streams.Dlq;

/// <summary>
/// Processing exception handler that writes failed records to a DLQ and returns Skip.
/// </summary>
public sealed class DeadLetterQueueExceptionHandler : IProcessingExceptionHandler
{
    private readonly DeadLetterQueueWriter _writer;
    private readonly StreamsMetrics _metrics;

    internal DeadLetterQueueExceptionHandler(DeadLetterQueueWriter writer, StreamsMetrics metrics)
    {
        _writer = writer;
        _metrics = metrics;
    }

    public ProcessingHandlerResponse Handle(string topic, int partition, long offset, object? key, object? value, Exception exception)
    {
        // Serialize key/value to bytes for DLQ storage
        var keyBytes = key != null ? JsonSerializer.SerializeToUtf8Bytes(key) : [];
        var valueBytes = value != null ? JsonSerializer.SerializeToUtf8Bytes(value) : [];

        _writer.Write(topic, partition, offset, keyBytes, valueBytes, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), exception);
        _metrics.RecordDlqMessage();
        return ProcessingHandlerResponse.Skip;
    }
}
