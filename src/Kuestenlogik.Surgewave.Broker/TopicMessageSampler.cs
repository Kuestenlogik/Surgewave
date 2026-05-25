using System.Buffers.Binary;
using System.Text;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
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
    /// Extract raw message value bytes from a Kafka RecordBatch.
    /// </summary>
    private static List<ReadOnlyMemory<byte>> ExtractMessageValues(byte[] batchBytes)
    {
        var values = new List<ReadOnlyMemory<byte>>();

        try
        {
            var span = batchBytes.AsSpan();
            if (span.Length < 61) return values;

            var attributes = BinaryPrimitives.ReadInt16BigEndian(span.Slice(21));
            var recordCount = BinaryPrimitives.ReadInt32BigEndian(span.Slice(57));

            // Skip compressed batches
            var compression = attributes & 0x07;
            if (compression != 0) return values;

            var pos = 61;
            for (var i = 0; i < recordCount && pos < span.Length; i++)
            {
                try
                {
                    var recordLength = ReadVarInt(span, ref pos);
                    if (recordLength <= 0 || pos + recordLength > span.Length) break;

                    ReadVarInt(span, ref pos); // attributes
                    ReadVarLong(span, ref pos); // timestampDelta
                    ReadVarInt(span, ref pos); // offsetDelta

                    // Skip key
                    var keyLength = ReadVarInt(span, ref pos);
                    if (keyLength > 0)
                    {
                        if (pos + keyLength > span.Length) break;
                        pos += keyLength;
                    }

                    // Read value
                    var valueLength = ReadVarInt(span, ref pos);
                    if (valueLength > 0 && pos + valueLength <= span.Length)
                    {
                        var valueBytes = new byte[valueLength];
                        span.Slice(pos, valueLength).CopyTo(valueBytes);
                        values.Add(new ReadOnlyMemory<byte>(valueBytes));
                        pos += valueLength;
                    }
                    else if (valueLength > 0)
                    {
                        break;
                    }

                    // Skip headers
                    var headerCount = ReadVarInt(span, ref pos);
                    for (var h = 0; h < headerCount && pos < span.Length; h++)
                    {
                        var hkLen = ReadVarInt(span, ref pos);
                        if (hkLen > 0)
                        {
                            if (pos + hkLen > span.Length) break;
                            pos += hkLen;
                        }
                        var hvLen = ReadVarInt(span, ref pos);
                        if (hvLen > 0)
                        {
                            if (pos + hvLen > span.Length) break;
                            pos += hvLen;
                        }
                    }
                }
                catch
                {
                    break;
                }
            }
        }
        catch
        {
            // Malformed batch — skip entirely
        }

        return values;
    }

    private static int ReadVarInt(ReadOnlySpan<byte> data, ref int pos)
    {
        int result = 0, shift = 0;
        while (pos < data.Length)
        {
            var b = data[pos++];
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0)
            {
                return (result >> 1) ^ -(result & 1); // ZigZag decode
            }
            shift += 7;
            if (shift > 28) break;
        }
        return 0;
    }

    private static long ReadVarLong(ReadOnlySpan<byte> data, ref int pos)
    {
        long result = 0;
        int shift = 0;
        while (pos < data.Length)
        {
            var b = data[pos++];
            result |= (long)(b & 0x7F) << shift;
            if ((b & 0x80) == 0)
            {
                return (result >> 1) ^ -(result & 1); // ZigZag decode
            }
            shift += 7;
            if (shift > 63) break;
        }
        return 0;
    }
}
