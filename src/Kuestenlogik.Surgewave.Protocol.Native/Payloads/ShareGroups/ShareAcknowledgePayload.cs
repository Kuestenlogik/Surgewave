using System.Text;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.ShareGroups;

/// <summary>
/// Wire format for ShareAcknowledge request.
///
/// Wire layout:
///   groupId                string (int16 length prefix + UTF-8)
///   memberId               string (int16 length prefix + UTF-8)
///   topicCount             int32
///   for each topic:
///     topicId              16 bytes (big-endian UUID)
///     partitionCount       int32
///     for each partition:
///       partitionIndex     int32
///       ackBatchCount      int32
///       for each ack batch:
///         firstOffset      int64
///         lastOffset       int64
///         ackTypeCount     int32
///         ackTypes         byte[]
/// </summary>
public readonly record struct ShareAcknowledgeRequestPayload
{
    public string GroupId { get; init; }
    public string MemberId { get; init; }
    public ShareAcknowledgeTopic[] Topics { get; init; }

    /// <summary>
    /// Read payload from binary data.
    /// </summary>
    public static ShareAcknowledgeRequestPayload Read(ref SurgewavePayloadReader reader)
    {
        var groupId = reader.ReadString() ?? "";
        var memberId = reader.ReadString() ?? "";

        var topicCount = reader.ReadInt32();
        var topics = new ShareAcknowledgeTopic[topicCount];

        for (var t = 0; t < topicCount; t++)
        {
            var topicId = GuidHelper.ReadGuid(ref reader);
            var partitionCount = reader.ReadInt32();
            var partitions = new ShareAcknowledgePartition[partitionCount];

            for (var p = 0; p < partitionCount; p++)
            {
                var partitionIndex = reader.ReadInt32();
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

                partitions[p] = new ShareAcknowledgePartition(partitionIndex, ackBatches);
            }

            topics[t] = new ShareAcknowledgeTopic(topicId, partitions);
        }

        return new ShareAcknowledgeRequestPayload
        {
            GroupId = groupId,
            MemberId = memberId,
            Topics = topics
        };
    }

    /// <summary>
    /// Write payload to binary buffer.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(GroupId);
        writer.WriteString(MemberId);
        writer.WriteInt32(Topics.Length);

        foreach (var topic in Topics)
        {
            GuidHelper.WriteGuid(ref writer, topic.TopicId);
            writer.WriteInt32(topic.Partitions.Length);

            foreach (var partition in topic.Partitions)
            {
                writer.WriteInt32(partition.PartitionIndex);
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
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface.
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(GroupId);
        writer.WriteString(MemberId);
        writer.WriteInt32(Topics.Length);

        foreach (var topic in Topics)
        {
            GuidHelper.WriteGuid(writer, topic.TopicId);
            writer.WriteInt32(topic.Partitions.Length);

            foreach (var partition in topic.Partitions)
            {
                writer.WriteInt32(partition.PartitionIndex);
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
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize()
    {
        var size =
            2 + Encoding.UTF8.GetByteCount(GroupId ?? "") +
            2 + Encoding.UTF8.GetByteCount(MemberId ?? "") +
            4; // Topics count

        foreach (var topic in Topics)
        {
            size += 16 + 4; // TopicId(16) + Partitions count(4)
            foreach (var partition in topic.Partitions)
            {
                size += 4 + 4; // PartitionIndex + AckBatch count
                foreach (var ack in partition.AcknowledgementBatches)
                    size += 8 + 8 + 4 + ack.AcknowledgeTypes.Length;
            }
        }

        return size;
    }
}

/// <summary>
/// Wire format for ShareAcknowledge response.
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
/// </summary>
public readonly record struct ShareAcknowledgeResponsePayload
{
    public int ThrottleTimeMs { get; init; }
    public ShareAcknowledgeTopicResponse[] Responses { get; init; }

    /// <summary>
    /// Read payload from binary data.
    /// </summary>
    public static ShareAcknowledgeResponsePayload Read(ref SurgewavePayloadReader reader)
    {
        var throttleTimeMs = reader.ReadInt32();
        var responseCount = reader.ReadInt32();
        var responses = new ShareAcknowledgeTopicResponse[responseCount];

        for (var r = 0; r < responseCount; r++)
        {
            var topicId = GuidHelper.ReadGuid(ref reader);
            var partitionCount = reader.ReadInt32();
            var partitions = new ShareAcknowledgePartitionResponse[partitionCount];

            for (var p = 0; p < partitionCount; p++)
            {
                var partitionIndex = reader.ReadInt32();
                var errorCode = reader.ReadInt16();
                partitions[p] = new ShareAcknowledgePartitionResponse(partitionIndex, errorCode);
            }

            responses[r] = new ShareAcknowledgeTopicResponse(topicId, partitions);
        }

        return new ShareAcknowledgeResponsePayload
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
            size += response.Partitions.Length * (4 + 2); // Per partition: PartitionIndex + ErrorCode
        }

        return size;
    }
}

/// <summary>
/// A topic within a ShareAcknowledge request.
/// </summary>
public readonly record struct ShareAcknowledgeTopic(Guid TopicId, ShareAcknowledgePartition[] Partitions);

/// <summary>
/// A partition within a ShareAcknowledge request.
/// </summary>
public readonly record struct ShareAcknowledgePartition(int PartitionIndex, AcknowledgementBatch[] AcknowledgementBatches);

/// <summary>
/// A topic response within a ShareAcknowledge response.
/// </summary>
public readonly record struct ShareAcknowledgeTopicResponse(Guid TopicId, ShareAcknowledgePartitionResponse[] Partitions);

/// <summary>
/// A partition response within a ShareAcknowledge response.
/// </summary>
public readonly record struct ShareAcknowledgePartitionResponse(int PartitionIndex, short ErrorCode);
