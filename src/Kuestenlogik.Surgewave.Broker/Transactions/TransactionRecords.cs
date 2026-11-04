using System.Buffers.Binary;
using System.Text;
using Kuestenlogik.Surgewave.Coordination.Transactions;
using Kuestenlogik.Surgewave.Core.Models;
using TransactionState = Kuestenlogik.Surgewave.Core.KafkaConstants.TransactionState;

namespace Kuestenlogik.Surgewave.Broker.Transactions;

/// <summary>
/// The records of <c>__transaction_state</c> (#197): one per transactional id, keyed as Kafka's TransactionLogKey v0,
/// its value led by the fields of Kafka's TransactionLogValue v0 — producer id and epoch, timeout, status, partitions,
/// timestamps — so Kafka tooling that dumps the topic reads the transaction. Behind them, in a section of its own
/// with its own version, what this coordinator needs to take a transaction over: the producer ids around an epoch
/// rotation, the client's protocol and the groups. The offsets the transaction commits are the group coordinator's,
/// in <c>__consumer_offsets</c>. A record without a value is a tombstone: the transactional id is gone.
/// </summary>
internal static class TransactionRecords
{
    private const short KeyVersion = 0;
    private const short ValueVersion = 0;
    private const short ExtensionVersion = 2;

    public static byte[] Key(string transactionalId)
    {
        var writer = new Writer();
        writer.Int16(KeyVersion);
        writer.String(transactionalId);
        return writer.ToArray();
    }

    public static byte[] Value(TransactionMetadata txn)
    {
        var writer = new Writer();
        writer.Int16(ValueVersion);
        writer.Int64(txn.ProducerId);
        writer.Int16(txn.ProducerEpoch);
        writer.Int32(txn.TransactionTimeoutMs);
        writer.Int8((sbyte)txn.State);

        var byTopic = txn.Partitions.GroupBy(p => p.Topic, StringComparer.Ordinal).ToList();
        writer.Int32(byTopic.Count);
        foreach (var topic in byTopic)
        {
            writer.String(topic.Key);
            var partitions = topic.Select(p => p.Partition).Order().ToList();
            writer.Int32(partitions.Count);
            foreach (var p in partitions)
                writer.Int32(p);
        }

        writer.Int64(txn.LastActivityTime.ToUnixTimeMilliseconds());
        writer.Int64(txn.TxnStartTime.ToUnixTimeMilliseconds());

        // Surgewave's section.
        writer.Int16(ExtensionVersion);
        writer.Int16(txn.LastProducerEpoch);
        writer.Int64(txn.PreviousProducerId);
        writer.Int64(txn.NextProducerId);
        writer.Int16((short)txn.ClientTransactionVersion);
        writer.Int32(txn.ConsumerGroups.Count);
        foreach (var group in txn.ConsumerGroups)
            writer.String(group);
        return writer.ToArray();
    }

    /// <summary>The transactional id of a key; null for a key version this broker does not write.</summary>
    public static string? ReadKey(ReadOnlySpan<byte> key)
    {
        if (key.Length < 2 || BinaryPrimitives.ReadInt16BigEndian(key) != KeyVersion)
            return null;
        var at = 2;
        return ReadString(key, ref at);
    }

    /// <summary>The transaction a value describes, as the coordinator takes it over.</summary>
    public static TransactionMetadata ReadValue(string transactionalId, ReadOnlySpan<byte> value)
    {
        var at = 2; // value version
        var txn = new TransactionMetadata
        {
            TransactionalId = transactionalId,
            ProducerId = Int64(value, ref at),
            ProducerEpoch = Int16(value, ref at),
            TransactionTimeoutMs = Int32(value, ref at),
            State = (TransactionState)(sbyte)value[at++],
        };

        var topics = Int32(value, ref at);
        for (var t = 0; t < topics; t++)
        {
            var topic = ReadString(value, ref at)!;
            var partitions = Int32(value, ref at);
            for (var p = 0; p < partitions; p++)
                txn.Partitions.Add(new TopicPartition { Topic = topic, Partition = Int32(value, ref at) });
        }

        txn.LastActivityTime = DateTimeOffset.FromUnixTimeMilliseconds(Int64(value, ref at));
        txn.TxnStartTime = DateTimeOffset.FromUnixTimeMilliseconds(Int64(value, ref at));

        // A value written by Kafka has no section of ours; one of a newer version carries at least this one's fields.
        if (at + 2 <= value.Length && Int16(value, ref at) >= ExtensionVersion)
        {
            txn.LastProducerEpoch = Int16(value, ref at);
            txn.PreviousProducerId = Int64(value, ref at);
            txn.NextProducerId = Int64(value, ref at);
            txn.ClientTransactionVersion = (TransactionVersion)Int16(value, ref at);
            var groups = Int32(value, ref at);
            for (var g = 0; g < groups; g++)
                txn.ConsumerGroups.Add(ReadString(value, ref at)!);
        }
        return txn;
    }

    private static short Int16(ReadOnlySpan<byte> span, ref int at)
    {
        var v = BinaryPrimitives.ReadInt16BigEndian(span[at..]);
        at += 2;
        return v;
    }

    private static int Int32(ReadOnlySpan<byte> span, ref int at)
    {
        var v = BinaryPrimitives.ReadInt32BigEndian(span[at..]);
        at += 4;
        return v;
    }

    private static long Int64(ReadOnlySpan<byte> span, ref int at)
    {
        var v = BinaryPrimitives.ReadInt64BigEndian(span[at..]);
        at += 8;
        return v;
    }

    // Kafka's string: int16 length, -1 for null, UTF-8.
    private static string? ReadString(ReadOnlySpan<byte> span, ref int at)
    {
        var length = Int16(span, ref at);
        if (length < 0)
            return null;
        var s = Encoding.UTF8.GetString(span.Slice(at, length));
        at += length;
        return s;
    }

    private sealed class Writer
    {
        private readonly MemoryStream _stream = new();
        private readonly byte[] _scratch = new byte[8];

        public void Int8(sbyte v) => _stream.WriteByte((byte)v);

        public void Int16(short v)
        {
            BinaryPrimitives.WriteInt16BigEndian(_scratch, v);
            _stream.Write(_scratch, 0, 2);
        }

        public void Int32(int v)
        {
            BinaryPrimitives.WriteInt32BigEndian(_scratch, v);
            _stream.Write(_scratch, 0, 4);
        }

        public void Int64(long v)
        {
            BinaryPrimitives.WriteInt64BigEndian(_scratch, v);
            _stream.Write(_scratch, 0, 8);
        }

        public void String(string? v)
        {
            if (v is null)
            {
                Int16(-1);
                return;
            }
            var bytes = Encoding.UTF8.GetBytes(v);
            Int16((short)bytes.Length);
            _stream.Write(bytes);
        }

        public byte[] ToArray() => _stream.ToArray();
    }
}
