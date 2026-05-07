using Kuestenlogik.Surgewave.Protocol.Native.Payloads;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Transactions;

/// <summary>
/// Wire format for AddPartitionsToTxn request.
/// Shared between broker (read) and client (write) to ensure consistency.
/// </summary>
public readonly record struct AddPartitionsToTxnRequestPayload
{
    public string TransactionalId { get; init; }
    public long ProducerId { get; init; }
    public short ProducerEpoch { get; init; }
    public Dictionary<string, List<int>> Topics { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static AddPartitionsToTxnRequestPayload Read(ref SurgewavePayloadReader reader)
    {
        var transactionalId = reader.ReadString() ?? string.Empty;
        var producerId = reader.ReadInt64();
        var producerEpoch = reader.ReadInt16();

        var topicCount = reader.ReadInt32();
        var topics = new Dictionary<string, List<int>>(topicCount);

        for (int i = 0; i < topicCount; i++)
        {
            var topic = reader.ReadString() ?? string.Empty;
            var partitionCount = reader.ReadInt32();
            var partitions = new List<int>(partitionCount);

            for (int j = 0; j < partitionCount; j++)
            {
                partitions.Add(reader.ReadInt32());
            }

            topics[topic] = partitions;
        }

        return new AddPartitionsToTxnRequestPayload
        {
            TransactionalId = transactionalId,
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
                    writer.WriteInt32(partition);
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
                    writer.WriteInt32(partition);
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
                   8 +                                           // ProducerId
                   2 +                                           // ProducerEpoch
                   4;                                            // Topic count

        if (Topics != null)
        {
            foreach (var (topic, partitions) in Topics)
            {
                size += 2 + System.Text.Encoding.UTF8.GetByteCount(topic); // Topic name
                size += 4;                                      // Partition count
                size += partitions.Count * 4;                   // Partition IDs
            }
        }

        return size;
    }
}

/// <summary>
/// Partition result for AddPartitionsToTxn response.
/// </summary>
public readonly record struct PartitionResult
{
    public int Partition { get; init; }
    public ushort ErrorCode { get; init; }

    public static PartitionResult Read(ref SurgewavePayloadReader reader)
    {
        return new PartitionResult
        {
            Partition = reader.ReadInt32(),
            ErrorCode = reader.ReadUInt16()
        };
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteInt32(Partition);
        writer.WriteUInt16(ErrorCode);
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteInt32(Partition);
        writer.WriteUInt16(ErrorCode);
    }
}

/// <summary>
/// Wire format for AddPartitionsToTxn response.
/// Shared between broker (write) and client (read) to ensure consistency.
/// </summary>
public readonly record struct AddPartitionsToTxnResponsePayload
{
    public Dictionary<string, List<PartitionResult>> Results { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static AddPartitionsToTxnResponsePayload Read(ref SurgewavePayloadReader reader)
    {
        var topicCount = reader.ReadInt32();
        var results = new Dictionary<string, List<PartitionResult>>(topicCount);

        for (int i = 0; i < topicCount; i++)
        {
            var topic = reader.ReadString() ?? string.Empty;
            var partitionCount = reader.ReadInt32();
            var partitionResults = new List<PartitionResult>(partitionCount);

            for (int j = 0; j < partitionCount; j++)
            {
                partitionResults.Add(PartitionResult.Read(ref reader));
            }

            results[topic] = partitionResults;
        }

        return new AddPartitionsToTxnResponsePayload { Results = results };
    }

    /// <summary>
    /// Write payload to binary buffer. Use with pre-sized or pooled buffers.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteInt32(Results?.Count ?? 0);

        if (Results != null)
        {
            foreach (var (topic, partitionResults) in Results)
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
        writer.WriteInt32(Results?.Count ?? 0);

        if (Results != null)
        {
            foreach (var (topic, partitionResults) in Results)
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

        if (Results != null)
        {
            foreach (var (topic, partitionResults) in Results)
            {
                size += 2 + System.Text.Encoding.UTF8.GetByteCount(topic); // Topic name
                size += 4;                                      // Partition count
                size += partitionResults.Count * (4 + 2);       // Partition + ErrorCode
            }
        }

        return size;
    }
}
