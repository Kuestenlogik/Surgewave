using System.Buffers.Binary;
using System.Text;

namespace Kuestenlogik.Surgewave.Broker.Transactions;

/// <summary>
/// Record format for entries in the __transaction_state topic.
/// Key: transactionalId (string)
/// Value: TransactionLogEntry (binary)
/// </summary>
internal sealed class TransactionLogEntry
{
    /// <summary>Version of the log entry format.</summary>
    public const short CurrentVersion = 1;

    /// <summary>The transactional ID (also the key).</summary>
    public required string TransactionalId { get; init; }

    /// <summary>The producer ID assigned to this transaction.</summary>
    public long ProducerId { get; init; }

    /// <summary>The current producer epoch.</summary>
    public short ProducerEpoch { get; init; }

    /// <summary>The transaction state.</summary>
    public TransactionLogState State { get; init; }

    /// <summary>Transaction timeout in milliseconds.</summary>
    public int TransactionTimeoutMs { get; init; }

    /// <summary>Timestamp when this entry was created.</summary>
    public long TimestampMs { get; init; }

    /// <summary>Coordinator epoch when this entry was written.</summary>
    public int CoordinatorEpoch { get; init; }

    /// <summary>Partitions participating in the transaction.</summary>
    public List<TransactionLogPartition> Partitions { get; init; } = [];

    /// <summary>
    /// Serializes the entry to binary format.
    /// </summary>
    public byte[] Serialize()
    {
        // Calculate size
        var txnIdBytes = Encoding.UTF8.GetBytes(TransactionalId);
        var partitionsSize = Partitions.Sum(p => 2 + Encoding.UTF8.GetByteCount(p.Topic) + 4);
        var totalSize = 2 + // version
                        2 + txnIdBytes.Length + // transactionalId
                        8 + // producerId
                        2 + // producerEpoch
                        1 + // state
                        4 + // transactionTimeoutMs
                        8 + // timestampMs
                        4 + // coordinatorEpoch
                        4 + partitionsSize; // partitions

        var buffer = new byte[totalSize];
        var span = buffer.AsSpan();
        var offset = 0;

        // Version
        BinaryPrimitives.WriteInt16BigEndian(span[offset..], CurrentVersion);
        offset += 2;

        // TransactionalId
        BinaryPrimitives.WriteInt16BigEndian(span[offset..], (short)txnIdBytes.Length);
        offset += 2;
        txnIdBytes.CopyTo(span[offset..]);
        offset += txnIdBytes.Length;

        // ProducerId
        BinaryPrimitives.WriteInt64BigEndian(span[offset..], ProducerId);
        offset += 8;

        // ProducerEpoch
        BinaryPrimitives.WriteInt16BigEndian(span[offset..], ProducerEpoch);
        offset += 2;

        // State
        buffer[offset++] = (byte)State;

        // TransactionTimeoutMs
        BinaryPrimitives.WriteInt32BigEndian(span[offset..], TransactionTimeoutMs);
        offset += 4;

        // TimestampMs
        BinaryPrimitives.WriteInt64BigEndian(span[offset..], TimestampMs);
        offset += 8;

        // CoordinatorEpoch
        BinaryPrimitives.WriteInt32BigEndian(span[offset..], CoordinatorEpoch);
        offset += 4;

        // Partitions count
        BinaryPrimitives.WriteInt32BigEndian(span[offset..], Partitions.Count);
        offset += 4;

        // Partitions
        foreach (var partition in Partitions)
        {
            var topicBytes = Encoding.UTF8.GetBytes(partition.Topic);
            BinaryPrimitives.WriteInt16BigEndian(span[offset..], (short)topicBytes.Length);
            offset += 2;
            topicBytes.CopyTo(span[offset..]);
            offset += topicBytes.Length;
            BinaryPrimitives.WriteInt32BigEndian(span[offset..], partition.Partition);
            offset += 4;
        }

        return buffer;
    }

    /// <summary>
    /// Deserializes an entry from binary format.
    /// </summary>
    public static TransactionLogEntry Deserialize(ReadOnlySpan<byte> data)
    {
        var offset = 0;

        // Version
        var version = BinaryPrimitives.ReadInt16BigEndian(data[offset..]);
        offset += 2;

        if (version != CurrentVersion)
        {
            throw new InvalidOperationException($"Unsupported transaction log entry version: {version}");
        }

        // TransactionalId
        var txnIdLength = BinaryPrimitives.ReadInt16BigEndian(data[offset..]);
        offset += 2;
        var transactionalId = Encoding.UTF8.GetString(data.Slice(offset, txnIdLength));
        offset += txnIdLength;

        // ProducerId
        var producerId = BinaryPrimitives.ReadInt64BigEndian(data[offset..]);
        offset += 8;

        // ProducerEpoch
        var producerEpoch = BinaryPrimitives.ReadInt16BigEndian(data[offset..]);
        offset += 2;

        // State
        var state = (TransactionLogState)data[offset++];

        // TransactionTimeoutMs
        var timeoutMs = BinaryPrimitives.ReadInt32BigEndian(data[offset..]);
        offset += 4;

        // TimestampMs
        var timestampMs = BinaryPrimitives.ReadInt64BigEndian(data[offset..]);
        offset += 8;

        // CoordinatorEpoch
        var coordinatorEpoch = BinaryPrimitives.ReadInt32BigEndian(data[offset..]);
        offset += 4;

        // Partitions
        var partitionCount = BinaryPrimitives.ReadInt32BigEndian(data[offset..]);
        offset += 4;

        var partitions = new List<TransactionLogPartition>(partitionCount);
        for (int i = 0; i < partitionCount; i++)
        {
            var topicLength = BinaryPrimitives.ReadInt16BigEndian(data[offset..]);
            offset += 2;
            var topic = Encoding.UTF8.GetString(data.Slice(offset, topicLength));
            offset += topicLength;
            var partitionIndex = BinaryPrimitives.ReadInt32BigEndian(data[offset..]);
            offset += 4;
            partitions.Add(new TransactionLogPartition(topic, partitionIndex));
        }

        return new TransactionLogEntry
        {
            TransactionalId = transactionalId,
            ProducerId = producerId,
            ProducerEpoch = producerEpoch,
            State = state,
            TransactionTimeoutMs = timeoutMs,
            TimestampMs = timestampMs,
            CoordinatorEpoch = coordinatorEpoch,
            Partitions = partitions
        };
    }

    /// <summary>
    /// Creates a key for the __transaction_state topic.
    /// </summary>
    public static byte[] CreateKey(string transactionalId)
    {
        return Encoding.UTF8.GetBytes(transactionalId);
    }
}

/// <summary>
/// States in the transaction log (maps to KafkaConstants.TransactionState).
/// </summary>
internal enum TransactionLogState : byte
{
    Empty = 0,
    Ongoing = 1,
    PrepareCommit = 2,
    PrepareAbort = 3,
    CompleteCommit = 4,
    CompleteAbort = 5,
    Dead = 6
}

/// <summary>
/// A partition entry in the transaction log.
/// </summary>
internal readonly record struct TransactionLogPartition(string Topic, int Partition);
