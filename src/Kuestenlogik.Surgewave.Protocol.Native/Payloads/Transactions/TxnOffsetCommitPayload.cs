using Kuestenlogik.Surgewave.Protocol.Native.Payloads;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Transactions;

/// <summary>
/// Partition data for TxnOffsetCommit request.
/// </summary>
public readonly record struct TxnOffsetCommitPartition
{
    public int Partition { get; init; }
    public long CommittedOffset { get; init; }
    public string? Metadata { get; init; }

    public static TxnOffsetCommitPartition Read(ref SurgewavePayloadReader reader)
    {
        return new TxnOffsetCommitPartition
        {
            Partition = reader.ReadInt32(),
            CommittedOffset = reader.ReadInt64(),
            Metadata = reader.ReadString()
        };
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteInt32(Partition);
        writer.WriteInt64(CommittedOffset);
        writer.WriteString(Metadata);
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteInt32(Partition);
        writer.WriteInt64(CommittedOffset);
        writer.WriteString(Metadata);
    }

    public int EstimateSize() =>
        4 +                                           // Partition
        8 +                                           // CommittedOffset
        2 + (Metadata != null ? System.Text.Encoding.UTF8.GetByteCount(Metadata) : 0); // Metadata
}

/// <summary>
/// Wire format for TxnOffsetCommit request.
/// Shared between broker (read) and client (write) to ensure consistency.
/// </summary>
public readonly record struct TxnOffsetCommitRequestPayload
{
    public string TransactionalId { get; init; }
    public string GroupId { get; init; }
    public long ProducerId { get; init; }
    public short ProducerEpoch { get; init; }
    public Dictionary<string, List<TxnOffsetCommitPartition>> Topics { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static TxnOffsetCommitRequestPayload Read(ref SurgewavePayloadReader reader)
    {
        var transactionalId = reader.ReadString() ?? string.Empty;
        var groupId = reader.ReadString() ?? string.Empty;
        var producerId = reader.ReadInt64();
        var producerEpoch = reader.ReadInt16();

        var topicCount = reader.ReadInt32();
        var topics = new Dictionary<string, List<TxnOffsetCommitPartition>>(topicCount);

        for (int i = 0; i < topicCount; i++)
        {
            var topic = reader.ReadString() ?? string.Empty;
            var partitionCount = reader.ReadInt32();
            var partitions = new List<TxnOffsetCommitPartition>(partitionCount);

            for (int j = 0; j < partitionCount; j++)
            {
                partitions.Add(TxnOffsetCommitPartition.Read(ref reader));
            }

            topics[topic] = partitions;
        }

        return new TxnOffsetCommitRequestPayload
        {
            TransactionalId = transactionalId,
            GroupId = groupId,
            ProducerId = producerId,
            ProducerEpoch = producerEpoch,
            Topics = topics
        };
    }

    /// <summary>
    /// Write payload to binary buffer. Use with pre-sized or pooled buffers.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(TransactionalId);
        writer.WriteString(GroupId);
        writer.WriteInt64(ProducerId);
        writer.WriteInt16(ProducerEpoch);
        writer.WriteInt32(Topics?.Count ?? 0);

        if (Topics != null)
        {
            foreach (var (topic, partitions) in Topics)
            {
                writer.WriteString(topic);
                writer.WriteInt32(partitions.Count);
                foreach (var partition in partitions)
                {
                    partition.Write(ref writer);
                }
            }
        }
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface (for BigEndianWriter and other implementations).
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(TransactionalId);
        writer.WriteString(GroupId);
        writer.WriteInt64(ProducerId);
        writer.WriteInt16(ProducerEpoch);
        writer.WriteInt32(Topics?.Count ?? 0);

        if (Topics != null)
        {
            foreach (var (topic, partitions) in Topics)
            {
                writer.WriteString(topic);
                writer.WriteInt32(partitions.Count);
                foreach (var partition in partitions)
                {
                    partition.WriteTo(writer);
                }
            }
        }
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize()
    {
        var size = 2 + System.Text.Encoding.UTF8.GetByteCount(TransactionalId ?? "") + // TransactionalId
                   2 + System.Text.Encoding.UTF8.GetByteCount(GroupId ?? "") +         // GroupId
                   8 +                                           // ProducerId
                   2 +                                           // ProducerEpoch
                   4;                                            // Topic count

        if (Topics != null)
        {
            foreach (var (topic, partitions) in Topics)
            {
                size += 2 + System.Text.Encoding.UTF8.GetByteCount(topic); // Topic name
                size += 4;                                      // Partition count
                foreach (var partition in partitions)
                {
                    size += partition.EstimateSize();
                }
            }
        }

        return size;
    }
}

/// <summary>
/// Wire format for TxnOffsetCommit response.
/// Shared between broker (write) and client (read) to ensure consistency.
/// </summary>
public readonly record struct TxnOffsetCommitResponsePayload
{
    public Dictionary<string, List<PartitionResult>> Topics { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static TxnOffsetCommitResponsePayload Read(ref SurgewavePayloadReader reader)
    {
        var topicCount = reader.ReadInt32();
        var topics = new Dictionary<string, List<PartitionResult>>(topicCount);

        for (int i = 0; i < topicCount; i++)
        {
            var topic = reader.ReadString() ?? string.Empty;
            var partitionCount = reader.ReadInt32();
            var partitionResults = new List<PartitionResult>(partitionCount);

            for (int j = 0; j < partitionCount; j++)
            {
                partitionResults.Add(PartitionResult.Read(ref reader));
            }

            topics[topic] = partitionResults;
        }

        return new TxnOffsetCommitResponsePayload { Topics = topics };
    }

    /// <summary>
    /// Write payload to binary buffer. Use with pre-sized or pooled buffers.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteInt32(Topics?.Count ?? 0);

        if (Topics != null)
        {
            foreach (var (topic, partitionResults) in Topics)
            {
                writer.WriteString(topic);
                writer.WriteInt32(partitionResults.Count);

                foreach (var partitionResult in partitionResults)
                {
                    partitionResult.Write(ref writer);
                }
            }
        }
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface (for BigEndianWriter and other implementations).
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteInt32(Topics?.Count ?? 0);

        if (Topics != null)
        {
            foreach (var (topic, partitionResults) in Topics)
            {
                writer.WriteString(topic);
                writer.WriteInt32(partitionResults.Count);

                foreach (var partitionResult in partitionResults)
                {
                    partitionResult.WriteTo(writer);
                }
            }
        }
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize()
    {
        var size = 4; // Topic count

        if (Topics != null)
        {
            foreach (var (topic, partitionResults) in Topics)
            {
                size += 2 + System.Text.Encoding.UTF8.GetByteCount(topic); // Topic name
                size += 4;                                      // Partition count
                size += partitionResults.Count * (4 + 2);       // Partition + ErrorCode
            }
        }

        return size;
    }
}
