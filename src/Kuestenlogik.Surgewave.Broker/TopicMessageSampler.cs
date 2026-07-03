using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Core.Util;
using Kuestenlogik.Surgewave.Schema.Registry.Inference;

namespace Kuestenlogik.Surgewave.Broker;

/// <summary>
/// Samples raw message values from topics for schema inference.
/// Bridges the Schema Registry inference engine with the broker's LogManager.
/// </summary>
public sealed class TopicMessageSampler : ITopicMessageSampler
{
    private readonly LogManager _logManager;

    public TopicMessageSampler(LogManager logManager)
    {
        _logManager = logManager;
    }

    /// <inheritdoc />
    public IReadOnlyList<string> GetTopics()
    {
        return _logManager.ListTopics().Select(t => t.Name).ToList();
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<ReadOnlyMemory<byte>>> SampleMessagesAsync(
        string topicName,
        int maxMessages,
        CancellationToken cancellationToken = default)
    {
        var messages = new List<ReadOnlyMemory<byte>>();
        var partitions = GetTopicPartitions(topicName);

        if (partitions.Count == 0)
        {
            return messages;
        }

        // Distribute the sample across partitions
        var perPartition = Math.Max(1, maxMessages / partitions.Count);

        foreach (var partition in partitions)
        {
            if (messages.Count >= maxMessages || cancellationToken.IsCancellationRequested)
            {
                break;
            }

            var tp = new TopicPartition { Topic = topicName, Partition = partition };
            var log = _logManager.GetLog(tp);
            if (log is null)
            {
                continue;
            }

            var offset = log.LogStartOffset;
            var highWatermark = log.HighWatermark;
            var batchesRead = 0;

            // If topic has many messages, sample from the tail for freshest data
            if (highWatermark - offset > perPartition * 10)
            {
                offset = Math.Max(offset, highWatermark - perPartition * 10);
            }

            while (offset < highWatermark && batchesRead < 50 && messages.Count < maxMessages)
            {
                List<byte[]> batches;
                try
                {
                    batches = await _logManager.ReadBatchesAsync(tp, offset, maxBytes: 512 * 1024, cancellationToken);
                }
                catch
                {
                    break;
                }

                if (batches.Count == 0)
                {
                    break;
                }

                foreach (var batchBytes in batches)
                {
                    var extracted = ExtractMessageValues(batchBytes);
                    foreach (var value in extracted)
                    {
                        if (messages.Count >= maxMessages)
                        {
                            break;
                        }
                        messages.Add(value);
                        offset++;
                    }
                }

                batchesRead++;
            }
        }

        return messages;
    }

    private List<int> GetTopicPartitions(string topicName)
    {
        var partitions = new List<int>();
        for (var i = 0; i < 256; i++)
        {
            var tp = new TopicPartition { Topic = topicName, Partition = i };
            if (_logManager.GetLog(tp) is not null)
            {
                partitions.Add(i);
            }
            else if (i > 0 && partitions.Count == 0)
            {
                break;
            }
            else if (partitions.Count > 0)
            {
                break;
            }
        }
        return partitions;
    }

    /// <summary>
    /// Extract raw message value bytes from a Kafka RecordBatch (decompressing when needed).
    /// </summary>
    private static List<ReadOnlyMemory<byte>> ExtractMessageValues(byte[] batchBytes)
    {
        var values = new List<ReadOnlyMemory<byte>>();
        foreach (var record in RecordBatchBrowser.Parse(batchBytes).Records)
        {
            if (record.Value is { Length: > 0 })
            {
                values.Add(record.Value);
            }
        }

        return values;
    }
}
