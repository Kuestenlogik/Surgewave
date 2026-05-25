using System.Buffers.Binary;
using System.Text;
using Kuestenlogik.Surgewave.Api.GraphQL.Types;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Api.GraphQL.Services;

/// <summary>
/// Bridges broker internals (LogManager, RecordBatch parsing) for GraphQL resolvers.
/// </summary>
public sealed class GraphQLBrokerService : IGraphQLBrokerService
{
    private readonly LogManager _logManager;
    private readonly Func<List<Message>, byte[]> _serializeMessages;
    private readonly Func<IReadOnlyList<GroupInfoDto>>? _listGroups;
    private readonly Func<ClusterInfoDto>? _getClusterInfo;
    private readonly ILogger<GraphQLBrokerService> _logger;

    /// <summary>
    /// Creates a new GraphQL broker service.
    /// </summary>
    public GraphQLBrokerService(
        LogManager logManager,
        Func<List<Message>, byte[]> serializeMessages,
        Func<IReadOnlyList<GroupInfoDto>>? listGroups,
        Func<ClusterInfoDto>? getClusterInfo,
        ILogger<GraphQLBrokerService> logger)
    {
        _logManager = logManager;
        _serializeMessages = serializeMessages;
        _listGroups = listGroups;
        _getClusterInfo = getClusterInfo;
        _logger = logger;
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<TopicType>> GetTopicsAsync(CancellationToken cancellationToken = default)
    {
        var topics = _logManager.ListTopics();
        var result = new List<TopicType>();

        foreach (var topic in topics)
        {
            long messageCount = 0;
            for (var p = 0; p < topic.PartitionCount; p++)
            {
                var tp = new TopicPartition { Topic = topic.Name, Partition = p };
                var log = _logManager.GetLog(tp);
                if (log is not null)
                {
                    messageCount += log.HighWatermark - log.LogStartOffset;
                }
            }

            result.Add(new TopicType
            {
                Name = topic.Name,
                PartitionCount = topic.PartitionCount,
                ReplicationFactor = topic.ReplicationFactor,
                MessageCount = messageCount,
                IsMirror = topic.IsMirror,
                IsReadOnly = topic.IsReadOnly,
                CreatedAt = new DateTimeOffset(topic.CreatedAt, TimeSpan.Zero),
            });
        }

        return Task.FromResult<IReadOnlyList<TopicType>>(result);
    }

    /// <inheritdoc />
    public async Task<IReadOnlyList<MessageType>> GetMessagesAsync(
        string topic,
        int? partition = null,
        long? offset = null,
        int limit = 10,
        CancellationToken cancellationToken = default)
    {
        var messages = new List<MessageType>();
        var startPartition = partition ?? 0;
        var endPartition = partition ?? (GetPartitionCount(topic) - 1);

        for (var p = startPartition; p <= endPartition && messages.Count < limit; p++)
        {
            var tp = new TopicPartition { Topic = topic, Partition = p };
            var startOffset = offset ?? 0;

            try
            {
                var batches = await _logManager.ReadBatchesAsync(tp, startOffset, maxBytes: 1024 * 1024, cancellationToken);

                foreach (var batchBytes in batches)
                {
                    var parsed = ParseRecordBatch(batchBytes, topic, p);
                    messages.AddRange(parsed);

                    if (messages.Count >= limit)
                        break;
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to read messages from {Topic}-{Partition} at offset {Offset}",
                    topic, p, startOffset);
            }
        }

        return messages.Take(limit).ToList();
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<ConsumerGroupType>> GetConsumerGroupsAsync(CancellationToken cancellationToken = default)
    {
        if (_listGroups is null)
        {
            return Task.FromResult<IReadOnlyList<ConsumerGroupType>>([]);
        }

        var groups = _listGroups();
        var result = groups.Select(g => new ConsumerGroupType
        {
            GroupId = g.GroupId,
            State = g.State,
            MemberCount = g.MemberCount,
            ProtocolType = g.ProtocolType,
        }).ToList();

        return Task.FromResult<IReadOnlyList<ConsumerGroupType>>(result);
    }

    /// <inheritdoc />
    public Task<ClusterInfoType> GetClusterInfoAsync(CancellationToken cancellationToken = default)
    {
        if (_getClusterInfo is not null)
        {
            var info = _getClusterInfo();
            return Task.FromResult(new ClusterInfoType
            {
                BrokerId = info.BrokerId,
                Host = info.Host,
                Port = info.Port,
                TopicCount = info.TopicCount,
                PartitionCount = info.PartitionCount,
            });
        }

        // Fallback: derive from LogManager
        var topics = _logManager.ListTopics().ToList();
        var totalPartitions = topics.Sum(t => t.PartitionCount);

        return Task.FromResult(new ClusterInfoType
        {
            BrokerId = 0,
            Host = "localhost",
            Port = 9092,
            TopicCount = topics.Count,
            PartitionCount = totalPartitions,
        });
    }

    /// <inheritdoc />
    public async Task<MessageType> ProduceMessageAsync(
        string topic,
        string? key,
        string value,
        int partition = 0,
        CancellationToken cancellationToken = default)
    {
        var tp = new TopicPartition { Topic = topic, Partition = partition };
        var now = DateTimeOffset.UtcNow;
        var timestampMs = now.ToUnixTimeMilliseconds();

        var message = new Message
        {
            Offset = 0, // Will be assigned by the log
            Timestamp = timestampMs,
            Key = key is not null ? Encoding.UTF8.GetBytes(key) : ReadOnlyMemory<byte>.Empty,
            Value = Encoding.UTF8.GetBytes(value),
            Headers = ReadOnlyMemory<byte>.Empty,
        };

        var batchBytes = _serializeMessages([message]);
        var assignedOffset = await _logManager.AppendBatchAsync(tp, batchBytes, cancellationToken);

        return new MessageType
        {
            Topic = topic,
            Partition = partition,
            Offset = assignedOffset,
            Timestamp = now,
            Key = key,
            Value = value,
            Headers = null,
        };
    }

    /// <inheritdoc />
    public async Task<TopicType> CreateTopicAsync(
        string name,
        int partitions = 1,
        int replicationFactor = 1,
        CancellationToken cancellationToken = default)
    {
        var metadata = await _logManager.CreateTopicAsync(
            name, partitions, (short)replicationFactor, cancellationToken: cancellationToken);

        return new TopicType
        {
            Name = metadata.Name,
            PartitionCount = metadata.PartitionCount,
            ReplicationFactor = metadata.ReplicationFactor,
            MessageCount = 0,
            IsMirror = metadata.IsMirror,
            IsReadOnly = metadata.IsReadOnly,
            CreatedAt = new DateTimeOffset(metadata.CreatedAt, TimeSpan.Zero),
        };
    }

    private int GetPartitionCount(string topic)
    {
        var topics = _logManager.ListTopics();
        var metadata = topics.FirstOrDefault(t =>
            string.Equals(t.Name, topic, StringComparison.Ordinal));
        return metadata?.PartitionCount ?? 1;
    }

    // ---- RecordBatch parsing (shared with MessageBrowserRestApi) ----

    private static List<MessageType> ParseRecordBatch(byte[] batchBytes, string topic, int partition)
    {
        var messages = new List<MessageType>();

        try
        {
            var span = batchBytes.AsSpan();
            if (span.Length < 61)
                return messages;

            var baseOffset = BinaryPrimitives.ReadInt64BigEndian(span);
            var attributes = BinaryPrimitives.ReadInt16BigEndian(span.Slice(21));
            var firstTimestamp = BinaryPrimitives.ReadInt64BigEndian(span.Slice(27));
            var recordCount = BinaryPrimitives.ReadInt32BigEndian(span.Slice(57));

            var compression = attributes & 0x07;
            if (compression != 0)
            {
                messages.Add(new MessageType
                {
                    Topic = topic,
                    Partition = partition,
                    Offset = baseOffset,
                    Timestamp = DateTimeOffset.FromUnixTimeMilliseconds(firstTimestamp),
                    Key = null,
                    Value = $"[Compressed batch with {recordCount} records]",
                    Headers = null,
                });
                return messages;
            }

            var pos = 61;
            for (var i = 0; i < recordCount && pos < span.Length; i++)
            {
                try
                {
                    var recordLength = ReadVarInt(span, ref pos);
                    if (recordLength <= 0 || pos + recordLength > span.Length)
                        break;

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
                    else if (keyLength > 0)
                    {
                        break;
                    }

                    var valueLength = ReadVarInt(span, ref pos);
                    string? value = null;
                    if (valueLength > 0 && pos + valueLength <= span.Length)
                    {
                        value = Encoding.UTF8.GetString(span.Slice(pos, valueLength));
                        pos += valueLength;
                    }
                    else if (valueLength > 0)
                    {
                        break;
                    }

                    var headerCount = ReadVarInt(span, ref pos);
                    Dictionary<string, string>? headers = headerCount > 0 ? new() : null;
                    for (var h = 0; h < headerCount && pos < span.Length; h++)
                    {
                        var headerKeyLen = ReadVarInt(span, ref pos);
                        if (headerKeyLen > 0 && pos + headerKeyLen <= span.Length)
                        {
                            var headerKey = Encoding.UTF8.GetString(span.Slice(pos, headerKeyLen));
                            pos += headerKeyLen;

                            var headerValueLen = ReadVarInt(span, ref pos);
                            if (headerValueLen > 0 && pos + headerValueLen <= span.Length)
                            {
                                var headerValue = Encoding.UTF8.GetString(span.Slice(pos, headerValueLen));
                                pos += headerValueLen;
                                headers![headerKey] = headerValue;
                            }
                        }
                    }

                    messages.Add(new MessageType
                    {
                        Topic = topic,
                        Partition = partition,
                        Offset = baseOffset + offsetDelta,
                        Timestamp = DateTimeOffset.FromUnixTimeMilliseconds(firstTimestamp + timestampDelta),
                        Key = key,
                        Value = value,
                        Headers = headers,
                    });
                }
                catch
                {
                    break;
                }
            }
        }
        catch
        {
            // Failed to parse batch
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
            {
                return (result >> 1) ^ -(result & 1);
            }
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
            {
                return (result >> 1) ^ -(result & 1);
            }
            shift += 7;
            if (shift > 63) break;
        }
        return result;
    }
}

/// <summary>
/// DTO for consumer group info passed via delegate from broker.
/// </summary>
public sealed record GroupInfoDto(string GroupId, string State, int MemberCount, string? ProtocolType = null);

/// <summary>
/// DTO for cluster info passed via delegate from broker.
/// </summary>
public sealed record ClusterInfoDto(int BrokerId, string Host, int Port, int TopicCount, int PartitionCount);
