using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Core.Storage;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Microsoft.Extensions.Logging;
using TransactionState = Kuestenlogik.Surgewave.Core.KafkaConstants.TransactionState;

namespace Kuestenlogik.Surgewave.Broker.Transactions;

/// <summary>
/// Manages the __transaction_state internal topic for durable transaction state storage.
/// This is a compacted topic that stores the latest state for each transactional ID.
/// </summary>
internal sealed partial class TransactionStateLog : IAsyncDisposable
{
    public const string TopicName = "__transaction_state";
    public const int DefaultPartitionCount = 50;
    public const short DefaultReplicationFactor = 3;

    private readonly LogManager _logManager;
    private readonly ILogger<TransactionStateLog> _logger;
    private readonly ConcurrentDictionary<string, TransactionLogEntry> _stateCache = new();
    private readonly int _partitionCount;
    private readonly TransactionStateLogOptions _options;
    private bool _initialized;

    public TransactionStateLog(
        LogManager logManager,
        ILogger<TransactionStateLog> logger,
        TransactionStateLogOptions? options = null)
    {
        _logManager = logManager;
        _logger = logger;
        _options = options ?? new TransactionStateLogOptions();
        _partitionCount = _options.PartitionCount;
    }

    /// <summary>
    /// Initializes the transaction state log by loading existing entries.
    /// </summary>
    public async Task InitializeAsync(CancellationToken cancellationToken)
    {
        if (_initialized) return;

        LogInitializing();

        // Load state from each partition
        for (int partition = 0; partition < _partitionCount; partition++)
        {
            await LoadPartitionAsync(partition, cancellationToken);
        }

        _initialized = true;
        LogInitialized(_stateCache.Count);
    }

    /// <summary>
    /// Writes a transaction state update to the log.
    /// </summary>
    public async Task<long> WriteStateAsync(
        TransactionMetadata metadata,
        int coordinatorEpoch,
        CancellationToken cancellationToken)
    {
        var entry = CreateEntry(metadata, coordinatorEpoch);
        var partition = GetPartitionForTransactionalId(metadata.TransactionalId);
        var tp = new TopicPartition { Topic = TopicName, Partition = partition };

        // Create the record batch
        var key = TransactionLogEntry.CreateKey(metadata.TransactionalId);
        var value = entry.Serialize();
        var batch = CreateRecordBatch(key, value);

        // Write to log
        var offset = await _logManager.AppendBatchAsync(tp, batch, cancellationToken);

        // Update cache
        _stateCache[metadata.TransactionalId] = entry;

        LogStateWritten(metadata.TransactionalId, metadata.State.ToString(), partition, offset);
        return offset;
    }

    /// <summary>
    /// Reads the current state for a transactional ID from cache.
    /// </summary>
    public TransactionLogEntry? GetState(string transactionalId)
    {
        return _stateCache.GetValueOrDefault(transactionalId);
    }

    /// <summary>
    /// Gets all cached transaction states.
    /// </summary>
    public IEnumerable<TransactionLogEntry> GetAllStates()
    {
        return _stateCache.Values;
    }

    /// <summary>
    /// Removes a transaction state (writes a tombstone).
    /// </summary>
    public async Task RemoveStateAsync(string transactionalId, CancellationToken cancellationToken)
    {
        var partition = GetPartitionForTransactionalId(transactionalId);
        var tp = new TopicPartition { Topic = TopicName, Partition = partition };

        // Write tombstone (null value)
        var key = TransactionLogEntry.CreateKey(transactionalId);
        var batch = CreateRecordBatch(key, null);

        await _logManager.AppendBatchAsync(tp, batch, cancellationToken);
        _stateCache.TryRemove(transactionalId, out _);

        LogStateRemoved(transactionalId, partition);
    }

    /// <summary>
    /// Gets the partition for a transactional ID using consistent hashing.
    /// </summary>
    public int GetPartitionForTransactionalId(string transactionalId)
    {
        var hash = GetMurmur2Hash(transactionalId);
        return Math.Abs(hash) % _partitionCount;
    }

    /// <summary>
    /// Loads state from a specific partition.
    /// </summary>
    private async Task LoadPartitionAsync(int partition, CancellationToken cancellationToken)
    {
        var tp = new TopicPartition { Topic = TopicName, Partition = partition };

        try
        {
            var startOffset = 0L;
            var entriesLoaded = 0;

            while (true)
            {
                var records = await ReadRecordsAsync(tp, startOffset, _options.BatchSize, cancellationToken);
                if (records.Count == 0)
                    break;

                foreach (var record in records)
                {
                    ProcessRecord(record);
                    entriesLoaded++;
                }

                startOffset = records[^1].Offset + 1;
            }

            if (entriesLoaded > 0)
            {
                LogPartitionLoaded(partition, entriesLoaded);
            }
        }
        catch (Exception ex)
        {
            LogPartitionLoadError(partition, ex);
        }
    }

    private void ProcessRecord(LogRecord record)
    {
        if (record.Key == null || record.Key.Length == 0)
            return;

        var transactionalId = System.Text.Encoding.UTF8.GetString(record.Key);

        // Tombstone - remove from cache
        if (record.Value == null || record.Value.Length == 0)
        {
            _stateCache.TryRemove(transactionalId, out _);
            return;
        }

        // Parse and cache entry
        try
        {
            var entry = TransactionLogEntry.Deserialize(record.Value);
            _stateCache[transactionalId] = entry;
        }
        catch (Exception ex)
        {
            LogRecordParseError(transactionalId, ex);
        }
    }

    private async Task<List<LogRecord>> ReadRecordsAsync(
        TopicPartition tp,
        long startOffset,
        int maxRecords,
        CancellationToken cancellationToken)
    {
        // Use the LogManager to read records
        var partitionLog = _logManager.GetOrCreateLog(tp);
        var records = new List<LogRecord>();

        // Read raw batches
        var batches = await partitionLog.ReadBatchesAsync(startOffset, maxRecords * 1024, cancellationToken);

        long currentOffset = startOffset;
        foreach (var batchBytes in batches)
        {
            // Parse records from batch
            var batchRecords = ParseRecordsFromBatch(batchBytes, ref currentOffset);
            records.AddRange(batchRecords);

            if (records.Count >= maxRecords)
                break;
        }

        return records;
    }

    private static List<LogRecord> ParseRecordsFromBatch(byte[] batchBytes, ref long baseOffset)
    {
        var records = new List<LogRecord>();

        if (batchBytes.Length < KafkaConstants.RecordBatch.HeaderSize)
            return records;

        try
        {
            var span = batchBytes.AsSpan();

            // Read base offset from batch header
            baseOffset = System.Buffers.Binary.BinaryPrimitives.ReadInt64BigEndian(span);
            var batchLength = System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(span[8..]);

            // Skip header to get to records (after CRC and batch header)
            var recordsStart = KafkaConstants.RecordBatch.HeaderSize;
            if (recordsStart >= batchBytes.Length)
                return records;

            var recordsSpan = span[recordsStart..];
            var offset = 0;

            while (offset < recordsSpan.Length && records.Count < 100) // Safety limit
            {
                // Read record length (varint zigzag)
                var recordLength = ReadVarIntZigzag(recordsSpan[offset..], out var varIntBytes);
                offset += varIntBytes;

                if (recordLength <= 0 || offset + recordLength > recordsSpan.Length)
                    break;

                var recordSpan = recordsSpan.Slice(offset, recordLength);
                offset += recordLength;

                // Parse individual record
                var record = ParseRecord(recordSpan, baseOffset + records.Count);
                if (record.HasValue)
                {
                    records.Add(record.Value);
                }
            }
        }
        catch
        {
            // Ignore parse errors for malformed batches
        }

        return records;
    }

    private static LogRecord? ParseRecord(ReadOnlySpan<byte> recordSpan, long recordOffset)
    {
        if (recordSpan.Length < 5)
            return null;

        var pos = 0;

        // Skip attributes (1 byte)
        pos++;

        // Skip timestamp delta (varlong zigzag)
        ReadVarLongZigzag(recordSpan[pos..], out var varBytes);
        pos += varBytes;

        // Skip offset delta (varint zigzag)
        ReadVarIntZigzag(recordSpan[pos..], out varBytes);
        pos += varBytes;

        // Read key length (varint zigzag)
        var keyLength = ReadVarIntZigzag(recordSpan[pos..], out varBytes);
        pos += varBytes;

        byte[]? key = null;
        if (keyLength >= 0 && pos + keyLength <= recordSpan.Length)
        {
            key = recordSpan.Slice(pos, keyLength).ToArray();
            pos += keyLength;
        }
        else if (keyLength >= 0)
        {
            return null;
        }

        // Read value length (varint zigzag)
        var valueLength = ReadVarIntZigzag(recordSpan[pos..], out varBytes);
        pos += varBytes;

        byte[]? value = null;
        if (valueLength >= 0 && pos + valueLength <= recordSpan.Length)
        {
            value = recordSpan.Slice(pos, valueLength).ToArray();
        }

        return new LogRecord(recordOffset, key, value);
    }

    private static int ReadVarIntZigzag(ReadOnlySpan<byte> span, out int bytesRead)
    {
        var value = ReadVarInt(span, out bytesRead);
        return (int)KafkaProtocolPrimitives.ZigzagDecode((uint)value);
    }

    private static long ReadVarLongZigzag(ReadOnlySpan<byte> span, out int bytesRead)
    {
        var value = ReadVarLong(span, out bytesRead);
        return KafkaProtocolPrimitives.ZigzagDecode((ulong)value);
    }

    private static int ReadVarInt(ReadOnlySpan<byte> span, out int bytesRead)
    {
        int result = 0;
        int shift = 0;
        bytesRead = 0;

        while (bytesRead < span.Length && bytesRead < 5)
        {
            byte b = span[bytesRead++];
            result |= (b & 0x7F) << shift;
            if ((b & 0x80) == 0)
                return result;
            shift += 7;
        }

        return result;
    }

    private static long ReadVarLong(ReadOnlySpan<byte> span, out int bytesRead)
    {
        long result = 0;
        int shift = 0;
        bytesRead = 0;

        while (bytesRead < span.Length && bytesRead < 10)
        {
            byte b = span[bytesRead++];
            result |= (long)(b & 0x7F) << shift;
            if ((b & 0x80) == 0)
                return result;
            shift += 7;
        }

        return result;
    }

    private static TransactionLogEntry CreateEntry(TransactionMetadata metadata, int coordinatorEpoch)
    {
        return new TransactionLogEntry
        {
            TransactionalId = metadata.TransactionalId,
            ProducerId = metadata.ProducerId,
            ProducerEpoch = metadata.ProducerEpoch,
            State = MapState(metadata.State),
            TransactionTimeoutMs = metadata.TransactionTimeoutMs,
            TimestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            CoordinatorEpoch = coordinatorEpoch,
            Partitions = metadata.Partitions
                .Select(p => new TransactionLogPartition(p.Topic, p.Partition))
                .ToList()
        };
    }

    private static TransactionLogState MapState(TransactionState state)
    {
        return state switch
        {
            TransactionState.Empty => TransactionLogState.Empty,
            TransactionState.Ongoing => TransactionLogState.Ongoing,
            TransactionState.PrepareCommit => TransactionLogState.PrepareCommit,
            TransactionState.PrepareAbort => TransactionLogState.PrepareAbort,
            TransactionState.CompleteCommit => TransactionLogState.CompleteCommit,
            TransactionState.CompleteAbort => TransactionLogState.CompleteAbort,
            TransactionState.Dead => TransactionLogState.Dead,
            _ => TransactionLogState.Empty
        };
    }

    /// <summary>
    /// Creates a record batch for a key-value pair.
    /// Uses the Kafka v2 record batch format.
    /// </summary>
    private static byte[] CreateRecordBatch(byte[] key, byte[]? value)
    {
        var baseTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        // Build the record
        using var recordStream = new MemoryStream();

        // attributes (int8) = 0
        recordStream.WriteByte(0);

        // timestampDelta (varlong zigzag) = 0
        recordStream.WriteByte(0);

        // offsetDelta (varint zigzag) = 0
        recordStream.WriteByte(0);

        // keyLength (varint zigzag)
        WriteVarInt(recordStream, (int)Kuestenlogik.Surgewave.Protocol.Kafka.KafkaProtocolPrimitives.ZigzagEncode(key.Length));
        recordStream.Write(key, 0, key.Length);

        // valueLength (varint zigzag)
        if (value == null || value.Length == 0)
        {
            // Null/tombstone value - write -1
            WriteVarInt(recordStream, (int)Kuestenlogik.Surgewave.Protocol.Kafka.KafkaProtocolPrimitives.ZigzagEncode(-1));
        }
        else
        {
            WriteVarInt(recordStream, (int)Kuestenlogik.Surgewave.Protocol.Kafka.KafkaProtocolPrimitives.ZigzagEncode(value.Length));
            recordStream.Write(value, 0, value.Length);
        }

        // headers count (varint) = 0
        recordStream.WriteByte(0);

        var recordBytes = recordStream.ToArray();

        // Build the full batch
        using var batchStream = new MemoryStream();
        using var writer = new BinaryWriter(batchStream);

        // Attributes: compression=0, timestamp=CreateTime
        short attributes = 0;

        // Calculate batch length (everything after length field)
        var recordsLength = GetVarIntSize(Kuestenlogik.Surgewave.Protocol.Kafka.KafkaProtocolPrimitives.ZigzagEncode(recordBytes.Length)) + recordBytes.Length;
        var batchLength = KafkaConstants.RecordBatch.HeaderSize -
                         KafkaConstants.RecordBatch.BaseOffsetSize -
                         KafkaConstants.RecordBatch.LengthSize +
                         recordsLength;

        // Write batch header
        WriteBigEndianInt64(writer, 0L); // Base Offset
        WriteBigEndianInt32(writer, batchLength);
        WriteBigEndianInt32(writer, 0); // Partition Leader Epoch
        writer.Write(KafkaConstants.Magic.V2);

        // Prepare CRC data
        using var crcStream = new MemoryStream();
        using var crcWriter = new BinaryWriter(crcStream);

        WriteBigEndianInt16(crcWriter, attributes);
        WriteBigEndianInt32(crcWriter, 0); // Last Offset Delta
        WriteBigEndianInt64(crcWriter, baseTimestamp);
        WriteBigEndianInt64(crcWriter, baseTimestamp); // Max Timestamp
        WriteBigEndianInt64(crcWriter, -1L); // Producer ID (-1 for non-transactional)
        WriteBigEndianInt16(crcWriter, (short)-1); // Producer Epoch
        WriteBigEndianInt32(crcWriter, -1); // Base Sequence
        WriteBigEndianInt32(crcWriter, 1); // Record Count

        // Record length (zigzag varint)
        WriteVarInt(crcStream, (int)Kuestenlogik.Surgewave.Protocol.Kafka.KafkaProtocolPrimitives.ZigzagEncode(recordBytes.Length));
        crcStream.Write(recordBytes, 0, recordBytes.Length);

        var crcData = crcStream.ToArray();
        var crc = Kuestenlogik.Surgewave.Core.Util.Crc32C.Compute(crcData);

        WriteBigEndianUInt32(writer, crc);
        writer.Write(crcData);

        return batchStream.ToArray();
    }

    private static void WriteVarInt(Stream stream, int value)
    {
        while ((value & ~0x7F) != 0)
        {
            stream.WriteByte((byte)((value & 0x7F) | 0x80));
            value = (int)((uint)value >> 7);
        }
        stream.WriteByte((byte)value);
    }

    private static int GetVarIntSize(long value)
    {
        int size = 0;
        while ((value & ~0x7FL) != 0)
        {
            size++;
            value = (long)((ulong)value >> 7);
        }
        return size + 1;
    }

    private static void WriteBigEndianInt16(BinaryWriter writer, short value)
    {
        Span<byte> buffer = stackalloc byte[2];
        System.Buffers.Binary.BinaryPrimitives.WriteInt16BigEndian(buffer, value);
        writer.Write(buffer);
    }

    private static void WriteBigEndianInt32(BinaryWriter writer, int value)
    {
        Span<byte> buffer = stackalloc byte[4];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(buffer, value);
        writer.Write(buffer);
    }

    private static void WriteBigEndianInt64(BinaryWriter writer, long value)
    {
        Span<byte> buffer = stackalloc byte[8];
        System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(buffer, value);
        writer.Write(buffer);
    }

    private static void WriteBigEndianUInt32(BinaryWriter writer, uint value)
    {
        Span<byte> buffer = stackalloc byte[4];
        System.Buffers.Binary.BinaryPrimitives.WriteUInt32BigEndian(buffer, value);
        writer.Write(buffer);
    }

    /// <summary>
    /// Murmur2 hash compatible with Kafka's partitioning.
    /// </summary>
    private static int GetMurmur2Hash(string key)
    {
        var data = System.Text.Encoding.UTF8.GetBytes(key);
        const uint seed = 0x9747b28c;
        const uint m = 0x5bd1e995;
        const int r = 24;

        uint h = seed ^ (uint)data.Length;

        int length4 = data.Length / 4;
        for (int i = 0; i < length4; i++)
        {
            int i4 = i * 4;
            uint k = (uint)(data[i4] & 0xff) |
                     ((uint)(data[i4 + 1] & 0xff) << 8) |
                     ((uint)(data[i4 + 2] & 0xff) << 16) |
                     ((uint)(data[i4 + 3] & 0xff) << 24);

            k *= m;
            k ^= k >> r;
            k *= m;

            h *= m;
            h ^= k;
        }

        int remaining = data.Length % 4;
        int offset = length4 * 4;
        switch (remaining)
        {
            case 3:
                h ^= (uint)(data[offset + 2] & 0xff) << 16;
                goto case 2;
            case 2:
                h ^= (uint)(data[offset + 1] & 0xff) << 8;
                goto case 1;
            case 1:
                h ^= (uint)(data[offset] & 0xff);
                h *= m;
                break;
        }

        h ^= h >> 13;
        h *= m;
        h ^= h >> 15;

        return (int)h;
    }

    public ValueTask DisposeAsync()
    {
        // Clear cache
        _stateCache.Clear();
        return ValueTask.CompletedTask;
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Initializing transaction state log")]
    private partial void LogInitializing();

    [LoggerMessage(Level = LogLevel.Information, Message = "Transaction state log initialized with {Count} entries")]
    private partial void LogInitialized(int count);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Transaction state written: {TransactionalId} -> {State} (partition={Partition}, offset={Offset})")]
    private partial void LogStateWritten(string transactionalId, string state, int partition, long offset);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Transaction state removed: {TransactionalId} (partition={Partition})")]
    private partial void LogStateRemoved(string transactionalId, int partition);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Loaded {Count} entries from partition {Partition}")]
    private partial void LogPartitionLoaded(int partition, int count);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Error loading partition {Partition}")]
    private partial void LogPartitionLoadError(int partition, Exception ex);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Error parsing record for {TransactionalId}")]
    private partial void LogRecordParseError(string transactionalId, Exception ex);
}

/// <summary>
/// Configuration options for the transaction state log.
/// </summary>
internal sealed class TransactionStateLogOptions
{
    /// <summary>
    /// Number of partitions for the __transaction_state topic.
    /// Default: 50
    /// </summary>
    public int PartitionCount { get; init; } = TransactionStateLog.DefaultPartitionCount;

    /// <summary>
    /// Replication factor for the __transaction_state topic.
    /// Default: 3
    /// </summary>
    public short ReplicationFactor { get; init; } = TransactionStateLog.DefaultReplicationFactor;

    /// <summary>
    /// Batch size for reading records during initialization.
    /// Default: 1000
    /// </summary>
    public int BatchSize { get; init; } = 1000;
}

/// <summary>
/// A record read from the log.
/// </summary>
internal readonly record struct LogRecord(long Offset, byte[]? Key, byte[]? Value);
