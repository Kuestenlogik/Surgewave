using System.Text.Json;
using Kuestenlogik.Surgewave.Streams.Runtime;

namespace Kuestenlogik.Surgewave.Streams.Dlq;

/// <summary>
/// Writes failed records to a dead letter queue topic as JSON.
/// </summary>
internal sealed class DeadLetterQueueWriter
{
    private readonly StreamsProducer _producer;
    private readonly StreamsConfig _config;

    public DeadLetterQueueWriter(StreamsProducer producer, StreamsConfig config)
    {
        _producer = producer;
        _config = config;
    }

    public void Write(string topic, int partition, long offset, byte[] key, byte[] value, long timestamp, Exception exception)
    {
        var dlqConfig = _config.DeadLetterQueue;
        var dlqTopic = dlqConfig.GetDlqTopicName(topic);

        var record = new DeadLetterRecord
        {
            OriginalTopic = topic,
            OriginalPartition = partition,
            OriginalOffset = offset,
            Key = key,
            Value = value,
            Timestamp = timestamp,
            ApplicationId = _config.ApplicationId,
            ExceptionType = exception.GetType().FullName ?? exception.GetType().Name,
            ExceptionMessage = exception.Message,
            StackTrace = dlqConfig.IncludeStackTrace ? exception.StackTrace : null,
            FailedAt = DateTimeOffset.UtcNow
        };

        var jsonBytes = JsonSerializer.SerializeToUtf8Bytes(record);

        // Preserve original key for partitioning
        _producer.Produce(new ProducerRecord(dlqTopic, partition, key, jsonBytes, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()));
    }
}
