using System.Text.Json;
using Kuestenlogik.Surgewave.Streams.ExceptionHandling;
using Kuestenlogik.Surgewave.Streams.Runtime;

namespace Kuestenlogik.Surgewave.Streams.Dlq;

/// <summary>
/// Deserialization exception handler that writes raw bytes to a DLQ and returns Continue.
/// </summary>
public sealed class DeadLetterQueueDeserializationHandler : IDeserializationExceptionHandler
{
    private readonly StreamsProducer _producer;
    private readonly StreamsConfig _config;
    private readonly StreamsMetrics _metrics;

    internal DeadLetterQueueDeserializationHandler(StreamsProducer producer, StreamsConfig config, StreamsMetrics metrics)
    {
        _producer = producer;
        _config = config;
        _metrics = metrics;
    }

    public DeserializationHandlerResponse Handle(string topic, int partition, long offset, byte[]? rawKey, byte[]? rawValue, Exception exception)
    {
        var dlqConfig = _config.DeadLetterQueue;
        var dlqTopic = dlqConfig.GetDlqTopicName(topic);

        var record = new DeadLetterRecord
        {
            OriginalTopic = topic,
            OriginalPartition = partition,
            OriginalOffset = offset,
            Key = rawKey ?? [],
            Value = rawValue ?? [],
            Timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            ApplicationId = _config.ApplicationId,
            ExceptionType = exception.GetType().FullName ?? exception.GetType().Name,
            ExceptionMessage = exception.Message,
            StackTrace = dlqConfig.IncludeStackTrace ? exception.StackTrace : null,
            FailedAt = DateTimeOffset.UtcNow
        };

        var jsonBytes = JsonSerializer.SerializeToUtf8Bytes(record);
        _producer.Produce(new ProducerRecord(dlqTopic, partition, rawKey ?? [], jsonBytes, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()));
        _metrics.RecordDlqMessage();

        return DeserializationHandlerResponse.Continue;
    }
}
