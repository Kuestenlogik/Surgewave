using System.Buffers.Binary;
using System.Text;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;

namespace Kuestenlogik.Surgewave.Streams.Sql.MaterializedViews;

/// <summary>
/// Default <see cref="IRawTopicReader"/> implementation that reads directly
/// from the broker's <see cref="LogManager"/>.
///
/// Discovers partitions by probing IDs 0..31 (matching the existing approach
/// in <c>QueryExecutor</c>), then iteratively reads record batches per
/// partition and yields them in offset order. Used both by the materialized
/// view refresh loop and by ad-hoc PG queries.
/// </summary>
public sealed class LogManagerRawTopicReader : IRawTopicReader
{
    private readonly LogManager _logManager;
    private const int MaxBatchBytes = 1_048_576;
    private const int MaxBatchesPerPartition = 1024;

    public LogManagerRawTopicReader(LogManager logManager)
    {
        _logManager = logManager;
    }

    public IEnumerable<RawTopicMessage> ReadTopic(string topicName)
    {
        var partitions = DiscoverPartitions(topicName);

        foreach (var partition in partitions)
        {
            var tp = new TopicPartition { Topic = topicName, Partition = partition };
            var log = _logManager.GetLog(tp);
            if (log is null) continue;

            var offset = log.LogStartOffset;
            var highWatermark = log.HighWatermark;
            var batchesRead = 0;

            while (offset < highWatermark && batchesRead < MaxBatchesPerPartition)
            {
                List<byte[]> batches;
                try
                {
                    batches = _logManager.ReadBatchesAsync(tp, offset, maxBytes: MaxBatchBytes)
                        .AsTask().GetAwaiter().GetResult();
                }
                catch
                {
                    break;
                }

                if (batches.Count == 0) break;

                foreach (var batchBytes in batches)
                {
                    var messages = ParseRecordBatch(batchBytes, partition);
                    foreach (var msg in messages)
                    {
                        yield return msg;
                        offset = msg.Offset + 1;
                    }
                }

                batchesRead++;
            }
        }
    }

    private List<int> DiscoverPartitions(string topicName)
    {
        var partitions = new List<int>();
        for (var i = 0; i < 32; i++)
        {
            var tp = new TopicPartition { Topic = topicName, Partition = i };
            if (_logManager.GetLog(tp) != null)
                partitions.Add(i);
            else if (i > 0 && partitions.Count == 0)
                break;
            else if (partitions.Count > 0)
                break;
        }
        return partitions;
    }

    private static List<RawTopicMessage> ParseRecordBatch(byte[] batchBytes, int partition)
    {
        var messages = new List<RawTopicMessage>();

        try
        {
            var span = batchBytes.AsSpan();
            if (span.Length < 61) return messages;

            var baseOffset = BinaryPrimitives.ReadInt64BigEndian(span);
            var attributes = BinaryPrimitives.ReadInt16BigEndian(span[21..]);
            var firstTimestamp = BinaryPrimitives.ReadInt64BigEndian(span[27..]);
            var recordCount = BinaryPrimitives.ReadInt32BigEndian(span[57..]);

            var compression = attributes & 0x07;
            if (compression != 0) return messages;

            var pos = 61;
            for (var i = 0; i < recordCount && pos < span.Length; i++)
            {
                try
                {
                    var recordLength = ReadVarInt(span, ref pos);
                    if (recordLength <= 0 || pos + recordLength > span.Length) break;

                    ReadVarInt(span, ref pos); // attributes
                    var timestampDelta = ReadVarLong(span, ref pos);
                    var offsetDelta = ReadVarInt(span, ref pos);

                    var keyLength = ReadVarInt(span, ref pos);
                    string? key = null;
                    if (keyLength > 0 && pos + keyLength <= span.Length)
                    {
                        key = Encoding.UTF8.GetString(span.Slice(pos, keyLength));
                        pos += keyLength;
                    }
                    else if (keyLength > 0) break;

                    var valueLength = ReadVarInt(span, ref pos);
                    string? value = null;
                    if (valueLength > 0 && pos + valueLength <= span.Length)
                    {
                        value = Encoding.UTF8.GetString(span.Slice(pos, valueLength));
                        pos += valueLength;
                    }
                    else if (valueLength > 0) break;

                    var headerCount = ReadVarInt(span, ref pos);
                    var headers = new Dictionary<string, string>();
                    for (var h = 0; h < headerCount && pos < span.Length; h++)
                    {
                        var hkLen = ReadVarInt(span, ref pos);
                        if (hkLen > 0 && pos + hkLen <= span.Length)
                        {
                            var hk = Encoding.UTF8.GetString(span.Slice(pos, hkLen));
                            pos += hkLen;
                            var hvLen = ReadVarInt(span, ref pos);
                            if (hvLen > 0 && pos + hvLen <= span.Length)
                            {
                                headers[hk] = Encoding.UTF8.GetString(span.Slice(pos, hvLen));
                                pos += hvLen;
                            }
                        }
                    }

                    messages.Add(new RawTopicMessage(
                        Offset: baseOffset + offsetDelta,
                        Partition: partition,
                        Timestamp: DateTimeOffset.FromUnixTimeMilliseconds(firstTimestamp + timestampDelta),
                        Key: key,
                        Value: value,
                        Headers: headers.Count > 0 ? headers : null));
                }
                catch
                {
                    break;
                }
            }
        }
        catch
        {
            // Failed to parse batch — return what we have
        }

        return messages;
    }

    private static int ReadVarInt(ReadOnlySpan<byte> span, ref int pos)
    {
        var result = 0;
        var shift = 0;
        while (pos < span.Length)
        {
            var b = span[pos++];
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0)
                return (result >> 1) ^ -(result & 1);
            shift += 7;
            if (shift > 28) break;
        }
        return result;
    }

    private static long ReadVarLong(ReadOnlySpan<byte> span, ref int pos)
    {
        long result = 0;
        var shift = 0;
        while (pos < span.Length)
        {
            var b = span[pos++];
            result |= (long)(b & 0x7F) << shift;
            if ((b & 0x80) == 0)
                return (result >> 1) ^ -(result & 1);
            shift += 7;
            if (shift > 63) break;
        }
        return result;
    }
}
