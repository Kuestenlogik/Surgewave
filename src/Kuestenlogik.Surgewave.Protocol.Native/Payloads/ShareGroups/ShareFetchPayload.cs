using System.Text;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.ShareGroups;

/// <summary>
/// Wire format for ShareFetch request.
///
/// Wire layout:
///   groupId                string (int16 length prefix + UTF-8)
///   memberId               string (int16 length prefix + UTF-8)
///   maxWaitMs              int32
///   minBytes               int32
///   maxBytes               int32
///   topicCount             int32
///   for each topic:
///     topicId              16 bytes (big-endian UUID)
///     partitionCount       int32
///     for each partition:
///       partitionIndex     int32
///       partitionMaxBytes  int32
///       ackBatchCount      int32
///       for each ack batch:
///         firstOffset      int64
///         lastOffset       int64
///         ackTypeCount     int32
///         ackTypes         byte[]
///   forgottenTopicCount    int32
///   for each forgotten topic:
///     topicId              16 bytes (big-endian UUID)
///     partitionCount       int32
///     for each partition:
///       partitionIndex     int32
/// </summary>
public readonly record struct ShareFetchRequestPayload
{
    public string GroupId { get; init; }
    public string MemberId { get; init; }
    public int MaxWaitMs { get; init; }
    public int MinBytes { get; init; }
    public int MaxBytes { get; init; }
    public ShareFetchTopic[] Topics { get; init; }
    public ShareFetchForgottenTopic[] ForgottenTopics { get; init; }

    /// <summary>
    /// Read payload from binary data.
    /// </summary>
    public static ShareFetchRequestPayload Read(ref SurgewavePayloadReader reader)
    {
        var groupId = reader.ReadString() ?? "";
        var memberId = reader.ReadString() ?? "";
        var maxWaitMs = reader.ReadInt32();
        var minBytes = reader.ReadInt32();
        var maxBytes = reader.ReadInt32();

        var topicCount = reader.ReadInt32();
        var topics = new ShareFetchTopic[topicCount];
        for (var t = 0; t < topicCount; t++)
        {
            var topicId = GuidHelper.ReadGuid(ref reader);
            var partitionCount = reader.ReadInt32();
            var partitions = new ShareFetchPartition[partitionCount];

            for (var p = 0; p < partitionCount; p++)
            {
                var partitionIndex = reader.ReadInt32();
                var partitionMaxBytes = reader.ReadInt32();

                var ackBatchCount = reader.ReadInt32();
                var ackBatches = new AcknowledgementBatch[ackBatchCount];
                for (var a = 0; a < ackBatchCount; a++)
                {
                    var firstOffset = reader.ReadInt64();
                    var lastOffset = reader.ReadInt64();
                    var ackTypeCount = reader.ReadInt32();
                    var ackTypes = ackTypeCount > 0 ? reader.ReadRaw(ackTypeCount).ToArray() : [];
                    ackBatches[a] = new AcknowledgementBatch(firstOffset, lastOffset, ackTypes);
                }

                partitions[p] = new ShareFetchPartition
                {
                    PartitionIndex = partitionIndex,
                    PartitionMaxBytes = partitionMaxBytes,
                    AcknowledgementBatches = ackBatches
                };
            }

            topics[t] = new ShareFetchTopic(topicId, partitions);
        }

        var forgottenTopicCount = reader.ReadInt32();
        var forgottenTopics = new ShareFetchForgottenTopic[forgottenTopicCount];
        for (var f = 0; f < forgottenTopicCount; f++)
        {
            var topicId = GuidHelper.ReadGuid(ref reader);
            var partitionCount = reader.ReadInt32();
            var partitions = new int[partitionCount];
            for (var p = 0; p < partitionCount; p++)
                partitions[p] = reader.ReadInt32();
            forgottenTopics[f] = new ShareFetchForgottenTopic(topicId, partitions);
        }

        return new ShareFetchRequestPayload
        {
            GroupId = groupId,
            MemberId = memberId,
            MaxWaitMs = maxWaitMs,
            MinBytes = minBytes,
            MaxBytes = maxBytes,
            Topics = topics,
            ForgottenTopics = forgottenTopics
        };
    }

    /// <summary>
    /// Write payload to binary buffer.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(GroupId);
        writer.WriteString(MemberId);
        writer.WriteInt32(MaxWaitMs);
        writer.WriteInt32(MinBytes);
        writer.WriteInt32(MaxBytes);

        writer.WriteInt32(Topics.Length);
        foreach (var topic in Topics)
        {
            GuidHelper.WriteGuid(ref writer, topic.TopicId);
            writer.WriteInt32(topic.Partitions.Length);
            foreach (var partition in topic.Partitions)
            {
                writer.WriteInt32(partition.PartitionIndex);
                writer.WriteInt32(partition.PartitionMaxBytes);
                writer.WriteInt32(partition.AcknowledgementBatches.Length);
                foreach (var ack in partition.AcknowledgementBatches)
                {
                    writer.WriteInt64(ack.FirstOffset);
                    writer.WriteInt64(ack.LastOffset);
                    writer.WriteInt32(ack.AcknowledgeTypes.Length);
                    writer.WriteRaw(ack.AcknowledgeTypes);
                }
            }
        }

        writer.WriteInt32(ForgottenTopics.Length);
        foreach (var ft in ForgottenTopics)
        {
            GuidHelper.WriteGuid(ref writer, ft.TopicId);
            writer.WriteInt32(ft.Partitions.Length);
            foreach (var p in ft.Partitions)
                writer.WriteInt32(p);
        }
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface.
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(GroupId);
        writer.WriteString(MemberId);
        writer.WriteInt32(MaxWaitMs);
        writer.WriteInt32(MinBytes);
        writer.WriteInt32(MaxBytes);

        writer.WriteInt32(Topics.Length);
        foreach (var topic in Topics)
        {
            GuidHelper.WriteGuid(writer, topic.TopicId);
            writer.WriteInt32(topic.Partitions.Length);
            foreach (var partition in topic.Partitions)
            {
                writer.WriteInt32(partition.PartitionIndex);
                writer.WriteInt32(partition.PartitionMaxBytes);
                writer.WriteInt32(partition.AcknowledgementBatches.Length);
                foreach (var ack in partition.AcknowledgementBatches)
                {
                    writer.WriteInt64(ack.FirstOffset);
                    writer.WriteInt64(ack.LastOffset);
                    writer.WriteInt32(ack.AcknowledgeTypes.Length);
                    writer.WriteBytes(ack.AcknowledgeTypes);
                }
            }
        }

        writer.WriteInt32(ForgottenTopics.Length);
        foreach (var ft in ForgottenTopics)
        {
            GuidHelper.WriteGuid(writer, ft.TopicId);
            writer.WriteInt32(ft.Partitions.Length);
            foreach (var p in ft.Partitions)
                writer.WriteInt32(p);
        }
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize()
    {
        var size =
            2 + Encoding.UTF8.GetByteCount(GroupId ?? "") +
            2 + Encoding.UTF8.GetByteCount(MemberId ?? "") +
            4 + // MaxWaitMs
            4 + // MinBytes
            4 + // MaxBytes
            4;  // Topics count

        foreach (var topic in Topics)
        {
            size += 16 + 4; // TopicId(16) + Partitions count(4)
            foreach (var partition in topic.Partitions)
            {
                size += 4 + 4 + 4; // PartitionIndex + PartitionMaxBytes + AckBatch count
                foreach (var ack in partition.AcknowledgementBatches)
                    size += 8 + 8 + 4 + ack.AcknowledgeTypes.Length; // FirstOffset + LastOffset + length + types
            }
        }

        size += 4; // ForgottenTopics count
        foreach (var ft in ForgottenTopics)
            size += 16 + 4 + ft.Partitions.Length * 4; // TopicId(16) + count(4) + partitions

        return size;
    }
}

/// <summary>
/// Wire format for ShareFetch response.
///
/// Wire layout:
///   throttleTimeMs         int32
///   responseCount          int32
///   for each response:
///     topicId              16 bytes (big-endian UUID)
///     partitionCount       int32
///     for each partition:
///       partitionIndex     int32
///       errorCode          int16
///       highWatermark      int64
///       recordsLength      int32 (-1 = null)
///       records            byte[] (if length >= 0)
///       acquiredCount      int32
///       for each acquired:
///         firstOffset      int64
///         lastOffset       int64
///         deliveryCount    int32
/// </summary>
public readonly record struct ShareFetchResponsePayload
{
    public int ThrottleTimeMs { get; init; }
    public ShareFetchTopicResponse[] Responses { get; init; }

    /// <summary>
    /// Read payload from binary data.
    /// </summary>
    public static ShareFetchResponsePayload Read(ref SurgewavePayloadReader reader)
    {
        var throttleTimeMs = reader.ReadInt32();
        var responseCount = reader.ReadInt32();
        var responses = new ShareFetchTopicResponse[responseCount];

        for (var r = 0; r < responseCount; r++)
        {
            var topicId = GuidHelper.ReadGuid(ref reader);
            var partitionCount = reader.ReadInt32();
            var partitions = new ShareFetchPartitionResponse[partitionCount];

            for (var p = 0; p < partitionCount; p++)
            {
                var partitionIndex = reader.ReadInt32();
                var errorCode = reader.ReadInt16();
                var highWatermark = reader.ReadInt64();

                var recordsLength = reader.ReadInt32();
                byte[]? records = null;
                if (recordsLength >= 0)
                    records = reader.ReadRaw(recordsLength).ToArray();

                var acquiredCount = reader.ReadInt32();
                var acquiredRecords = new AcquiredRecord[acquiredCount];
                for (var a = 0; a < acquiredCount; a++)
                {
                    var firstOffset = reader.ReadInt64();
                    var lastOffset = reader.ReadInt64();
                    var deliveryCount = reader.ReadInt32();
                    acquiredRecords[a] = new AcquiredRecord(firstOffset, lastOffset, deliveryCount);
                }

                partitions[p] = new ShareFetchPartitionResponse
                {
                    PartitionIndex = partitionIndex,
                    ErrorCode = errorCode,
                    HighWatermark = highWatermark,
                    Records = records,
                    AcquiredRecords = acquiredRecords
                };
            }

            responses[r] = new ShareFetchTopicResponse(topicId, partitions);
        }

        return new ShareFetchResponsePayload
        {
            ThrottleTimeMs = throttleTimeMs,
            Responses = responses
        };
    }

    /// <summary>
    /// Write payload to binary buffer.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteInt32(ThrottleTimeMs);
        writer.WriteInt32(Responses.Length);

        foreach (var response in Responses)
        {
            GuidHelper.WriteGuid(ref writer, response.TopicId);
            writer.WriteInt32(response.Partitions.Length);

            foreach (var partition in response.Partitions)
            {
                writer.WriteInt32(partition.PartitionIndex);
                writer.WriteInt16(partition.ErrorCode);
                writer.WriteInt64(partition.HighWatermark);

                if (partition.Records is null)
                {
                    writer.WriteInt32(-1);
                }
                else
                {
                    writer.WriteInt32(partition.Records.Length);
                    writer.WriteRaw(partition.Records);
                }

                writer.WriteInt32(partition.AcquiredRecords.Length);
                foreach (var acquired in partition.AcquiredRecords)
                {
                    writer.WriteInt64(acquired.FirstOffset);
                    writer.WriteInt64(acquired.LastOffset);
                    writer.WriteInt32(acquired.DeliveryCount);
                }
            }
        }
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface.
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteInt32(ThrottleTimeMs);
        writer.WriteInt32(Responses.Length);

        foreach (var response in Responses)
        {
            GuidHelper.WriteGuid(writer, response.TopicId);
            writer.WriteInt32(response.Partitions.Length);

            foreach (var partition in response.Partitions)
            {
                writer.WriteInt32(partition.PartitionIndex);
                writer.WriteInt16(partition.ErrorCode);
                writer.WriteInt64(partition.HighWatermark);

                if (partition.Records is null)
                {
                    writer.WriteInt32(-1);
                }
                else
                {
                    writer.WriteInt32(partition.Records.Length);
                    writer.WriteBytes(partition.Records);
                }

                writer.WriteInt32(partition.AcquiredRecords.Length);
                foreach (var acquired in partition.AcquiredRecords)
                {
                    writer.WriteInt64(acquired.FirstOffset);
                    writer.WriteInt64(acquired.LastOffset);
                    writer.WriteInt32(acquired.DeliveryCount);
                }
            }
        }
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize()
    {
        var size = 4 + 4; // ThrottleTimeMs + Responses count

        foreach (var response in Responses)
        {
            size += 16 + 4; // TopicId(16) + Partitions count(4)
            foreach (var partition in response.Partitions)
            {
                size += 4 + 2 + 8; // PartitionIndex + ErrorCode + HighWatermark
                size += 4 + (partition.Records?.Length ?? 0); // Records length prefix + data
                size += 4; // AcquiredRecords count
                size += partition.AcquiredRecords.Length * (8 + 8 + 4); // Per acquired: FirstOffset + LastOffset + DeliveryCount
            }
        }

        return size;
    }
}

/// <summary>
/// A topic within a ShareFetch request containing partitions to fetch.
/// </summary>
public readonly record struct ShareFetchTopic(Guid TopicId, ShareFetchPartition[] Partitions);

/// <summary>
/// A partition within a ShareFetch request.
/// </summary>
public readonly record struct ShareFetchPartition
{
    public int PartitionIndex { get; init; }
    public int PartitionMaxBytes { get; init; }
    public AcknowledgementBatch[] AcknowledgementBatches { get; init; }
}

/// <summary>
/// An acknowledgement batch containing a range of offsets and their acknowledge types.
/// </summary>
public readonly record struct AcknowledgementBatch(long FirstOffset, long LastOffset, byte[] AcknowledgeTypes);

/// <summary>
/// A forgotten topic within a ShareFetch request (partitions to remove from the session).
/// </summary>
public readonly record struct ShareFetchForgottenTopic(Guid TopicId, int[] Partitions);

/// <summary>
/// A topic response within a ShareFetch response.
/// </summary>
public readonly record struct ShareFetchTopicResponse(Guid TopicId, ShareFetchPartitionResponse[] Partitions);

/// <summary>
/// A partition response within a ShareFetch response.
/// </summary>
public readonly record struct ShareFetchPartitionResponse
{
    public int PartitionIndex { get; init; }
    public short ErrorCode { get; init; }
    public long HighWatermark { get; init; }
    public byte[]? Records { get; init; }
    public AcquiredRecord[] AcquiredRecords { get; init; }
}

/// <summary>
/// An acquired record range with delivery count tracking.
/// </summary>
public readonly record struct AcquiredRecord(long FirstOffset, long LastOffset, int DeliveryCount);
