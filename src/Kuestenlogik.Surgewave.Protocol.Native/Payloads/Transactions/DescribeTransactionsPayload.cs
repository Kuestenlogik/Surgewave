using Kuestenlogik.Surgewave.Protocol.Native.Payloads;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Transactions;

/// <summary>
/// Wire format for DescribeTransactions request.
/// Shared between broker (read) and client (write) to ensure consistency.
/// </summary>
public readonly record struct DescribeTransactionsRequestPayload
{
    public List<string> TransactionalIds { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static DescribeTransactionsRequestPayload Read(ref SurgewavePayloadReader reader)
    {
        var txnCount = reader.ReadInt32();
        var transactionalIds = new List<string>(txnCount);

        for (int i = 0; i < txnCount; i++)
        {
            transactionalIds.Add(reader.ReadString() ?? string.Empty);
        }

        return new DescribeTransactionsRequestPayload { TransactionalIds = transactionalIds };
    }

    /// <summary>
    /// Write payload to binary buffer. Use with pre-sized or pooled buffers.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteInt32(TransactionalIds?.Count ?? 0);

        if (TransactionalIds != null)
        {
            foreach (var txnId in TransactionalIds)
            {
                writer.WriteString(txnId);
            }
        }
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface (for BigEndianWriter and other implementations).
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteInt32(TransactionalIds?.Count ?? 0);

        if (TransactionalIds != null)
        {
            foreach (var txnId in TransactionalIds)
            {
                writer.WriteString(txnId);
            }
        }
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize()
    {
        var size = 4; // Count

        if (TransactionalIds != null)
        {
            foreach (var txnId in TransactionalIds)
            {
                size += 2 + System.Text.Encoding.UTF8.GetByteCount(txnId ?? "");
            }
        }

        return size;
    }
}

/// <summary>
/// Partition info for transaction description.
/// </summary>
public readonly record struct TransactionPartition
{
    public string Topic { get; init; }
    public int Partition { get; init; }

    public static TransactionPartition Read(ref SurgewavePayloadReader reader)
    {
        return new TransactionPartition
        {
            Topic = reader.ReadString() ?? string.Empty,
            Partition = reader.ReadInt32()
        };
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(Topic);
        writer.WriteInt32(Partition);
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(Topic);
        writer.WriteInt32(Partition);
    }

    public int EstimateSize() =>
        2 + System.Text.Encoding.UTF8.GetByteCount(Topic ?? "") + // Topic
        4;                                            // Partition
}

/// <summary>
/// Transaction description data.
/// </summary>
public readonly record struct TransactionDescription
{
    public string TransactionalId { get; init; }
    public ushort ErrorCode { get; init; }
    public string State { get; init; }
    public long ProducerId { get; init; }
    public short ProducerEpoch { get; init; }
    public List<TransactionPartition> Partitions { get; init; }

    public static TransactionDescription Read(ref SurgewavePayloadReader reader)
    {
        var txnId = reader.ReadString() ?? string.Empty;
        var errorCode = reader.ReadUInt16();
        var state = reader.ReadString() ?? string.Empty;
        var producerId = reader.ReadInt64();
        var producerEpoch = reader.ReadInt16();
        var partitionCount = reader.ReadInt32();

        var partitions = new List<TransactionPartition>(partitionCount);
        for (int i = 0; i < partitionCount; i++)
        {
            partitions.Add(TransactionPartition.Read(ref reader));
        }

        return new TransactionDescription
        {
            TransactionalId = txnId,
            ErrorCode = errorCode,
            State = state,
            ProducerId = producerId,
            ProducerEpoch = producerEpoch,
            Partitions = partitions
        };
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(TransactionalId);
        writer.WriteUInt16(ErrorCode);
        writer.WriteString(State);
        writer.WriteInt64(ProducerId);
        writer.WriteInt16(ProducerEpoch);
        writer.WriteInt32(Partitions?.Count ?? 0);

        if (Partitions != null)
        {
            foreach (var partition in Partitions)
            {
                partition.Write(ref writer);
            }
        }
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(TransactionalId);
        writer.WriteUInt16(ErrorCode);
        writer.WriteString(State);
        writer.WriteInt64(ProducerId);
        writer.WriteInt16(ProducerEpoch);
        writer.WriteInt32(Partitions?.Count ?? 0);

        if (Partitions != null)
        {
            foreach (var partition in Partitions)
            {
                partition.WriteTo(writer);
            }
        }
    }

    public int EstimateSize()
    {
        var size = 2 + System.Text.Encoding.UTF8.GetByteCount(TransactionalId ?? "") + // TransactionalId
                   2 +                                           // ErrorCode
                   2 + System.Text.Encoding.UTF8.GetByteCount(State ?? "") +           // State
                   8 +                                           // ProducerId
                   2 +                                           // ProducerEpoch
                   4;                                            // Partition count

        if (Partitions != null)
        {
            foreach (var partition in Partitions)
            {
                size += partition.EstimateSize();
            }
        }

        return size;
    }
}

/// <summary>
/// Wire format for DescribeTransactions response.
/// Shared between broker (write) and client (read) to ensure consistency.
/// </summary>
public readonly record struct DescribeTransactionsResponsePayload
{
    public ushort ErrorCode { get; init; }
    public List<TransactionDescription> Transactions { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static DescribeTransactionsResponsePayload Read(ref SurgewavePayloadReader reader)
    {
        var errorCode = reader.ReadUInt16();
        var txnCount = reader.ReadInt32();
        var transactions = new List<TransactionDescription>(txnCount);

        for (int i = 0; i < txnCount; i++)
        {
            transactions.Add(TransactionDescription.Read(ref reader));
        }

        return new DescribeTransactionsResponsePayload
        {
            ErrorCode = errorCode,
            Transactions = transactions
        };
    }

    /// <summary>
    /// Write payload to binary buffer. Use with pre-sized or pooled buffers.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteUInt16(ErrorCode);
        writer.WriteInt32(Transactions?.Count ?? 0);

        if (Transactions != null)
        {
            foreach (var txn in Transactions)
            {
                txn.Write(ref writer);
            }
        }
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface (for BigEndianWriter and other implementations).
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteUInt16(ErrorCode);
        writer.WriteInt32(Transactions?.Count ?? 0);

        if (Transactions != null)
        {
            foreach (var txn in Transactions)
            {
                txn.WriteTo(writer);
            }
        }
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize()
    {
        var size = 2 +  // ErrorCode
                   4;   // Transaction count

        if (Transactions != null)
        {
            foreach (var txn in Transactions)
            {
                size += txn.EstimateSize();
            }
        }

        return size;
    }
}
