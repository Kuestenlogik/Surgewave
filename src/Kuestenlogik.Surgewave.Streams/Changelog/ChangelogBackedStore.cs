using Kuestenlogik.Surgewave.Streams.Windows;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kuestenlogik.Surgewave.Streams.Changelog;

/// <summary>
/// A key-value store wrapper that writes all changes to a changelog topic.
/// Provides durability and recovery capabilities for state stores.
/// </summary>
public sealed class ChangelogBackedStore<TKey, TValue> : IKeyValueStore<TKey, TValue>, ICheckpointable, IChangelogBacked
    where TKey : notnull
{
    private readonly IKeyValueStore<TKey, TValue> _innerStore;
    private readonly ISerde<TKey> _keySerde;
    private readonly ISerde<TValue> _valueSerde;
    private readonly string _applicationId;
    private readonly ILogger _logger;
    private ChangelogWriter<TKey, TValue>? _changelogWriter;
    private ProcessorContext? _context;
    private int _partition;
    private long _currentChangelogOffset;

    public string Name => _innerStore.Name;
    public bool Persistent => true;
    public long ApproximateNumEntries => _innerStore.ApproximateNumEntries;

    public ChangelogBackedStore(
        IKeyValueStore<TKey, TValue> innerStore,
        ISerde<TKey> keySerde,
        ISerde<TValue> valueSerde,
        string applicationId,
        ILogger? logger = null)
    {
        _innerStore = innerStore;
        _keySerde = keySerde;
        _valueSerde = valueSerde;
        _applicationId = applicationId;
        _logger = logger ?? NullLogger.Instance;
    }

    public void Init(ProcessorContext context)
    {
        _context = context;
        _partition = context.Partition;

        _innerStore.Init(context);

        _changelogWriter = new ChangelogWriter<TKey, TValue>(
            _applicationId,
            _innerStore.Name,
            _partition,
            _keySerde,
            _valueSerde,
            null,
            _logger);

        _logger.LogDebug("Initialized changelog-backed store {StoreName} for partition {Partition}",
            Name, _partition);
    }

    public TValue? Get(TKey key)
    {
        return _innerStore.Get(key);
    }

    public void Put(TKey key, TValue value)
    {
        var timestamp = _context?.Timestamp ?? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        _changelogWriter?.Write(key, value, timestamp);
        _innerStore.Put(key, value);
        Interlocked.Increment(ref _currentChangelogOffset);
    }

    public TValue? PutIfAbsent(TKey key, TValue value)
    {
        var existing = _innerStore.Get(key);
        if (existing != null)
            return existing;

        Put(key, value);
        return default;
    }

    public void PutAll(IEnumerable<KeyValue<TKey, TValue>> entries)
    {
        foreach (var entry in entries)
        {
            Put(entry.Key, entry.Value);
        }
    }

    public TValue? Delete(TKey key)
    {
        var existing = _innerStore.Get(key);
        if (existing == null)
            return default;

        var timestamp = _context?.Timestamp ?? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        _changelogWriter?.Delete(key, timestamp);
        _innerStore.Delete(key);
        Interlocked.Increment(ref _currentChangelogOffset);

        return existing;
    }

    public IEnumerable<KeyValue<TKey, TValue>> All()
    {
        return _innerStore.All();
    }

    public IEnumerable<KeyValue<TKey, TValue>> Range(TKey from, TKey to)
    {
        return _innerStore.Range(from, to);
    }

    public void Flush()
    {
        _changelogWriter?.Flush();
        _innerStore.Flush();
    }

    public StoreCheckpoint CreateCheckpoint()
    {
        var entries = _innerStore.All().ToList();
        using var ms = new System.IO.MemoryStream();
        using var writer = new System.IO.BinaryWriter(ms);

        writer.Write(entries.Count);
        foreach (var entry in entries)
        {
            var keyBytes = _keySerde.Serialize(entry.Key);
            var valueBytes = _valueSerde.Serialize(entry.Value);
            writer.Write(keyBytes.Length);
            writer.Write(keyBytes);
            writer.Write(valueBytes.Length);
            writer.Write(valueBytes);
        }

        return new StoreCheckpoint
        {
            StoreName = Name,
            ChangelogOffset = Interlocked.Read(ref _currentChangelogOffset),
            TimestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            SnapshotData = ms.ToArray(),
            EntryCount = entries.Count
        };
    }

    public void RestoreFromCheckpoint(StoreCheckpoint checkpoint)
    {
        if (checkpoint.SnapshotData == null || checkpoint.SnapshotData.Length == 0)
            return;

        using var ms = new System.IO.MemoryStream(checkpoint.SnapshotData);
        using var reader = new System.IO.BinaryReader(ms);

        var count = reader.ReadInt32();
        for (int i = 0; i < count; i++)
        {
            var keyLen = reader.ReadInt32();
            var keyBytes = reader.ReadBytes(keyLen);
            var valueLen = reader.ReadInt32();
            var valueBytes = reader.ReadBytes(valueLen);

            var key = _keySerde.Deserialize(keyBytes);
            var value = _valueSerde.Deserialize(valueBytes);
            _innerStore.Put(key, value);
        }

        Interlocked.Exchange(ref _currentChangelogOffset, checkpoint.ChangelogOffset);
        _logger.LogInformation("Restored {Count} entries from checkpoint for store {StoreName}",
            count, Name);
    }

    // IChangelogBacked implementation
    public string ChangelogTopicName => _changelogWriter?.TopicName ?? $"{_applicationId}-{Name}-changelog";
    public int ChangelogPartition => _partition;

    public void RestoreRecord(byte[] key, byte[] value)
    {
        var typedKey = _keySerde.Deserialize(key);
        if (value.Length == 0)
        {
            _innerStore.Delete(typedKey);
        }
        else
        {
            var typedValue = _valueSerde.Deserialize(value);
            _innerStore.Put(typedKey, typedValue);
        }
    }

    public void Close()
    {
        _changelogWriter?.Dispose();
        _innerStore.Close();
    }

    public void Dispose()
    {
        Close();
    }
}

/// <summary>
/// A window store wrapper that writes all changes to a changelog topic.
/// </summary>
public sealed class ChangelogBackedWindowStore<TKey, TValue> : IWindowStore<TKey, TValue>
    where TKey : notnull
{
    private readonly IWindowStore<TKey, TValue> _innerStore;
    private readonly ISerde<TKey> _keySerde;
    private readonly ISerde<TValue> _valueSerde;
    private readonly string _applicationId;
    private readonly ILogger _logger;
    private ChangelogWriter<WindowedKey<TKey>, TValue>? _changelogWriter;
    private ProcessorContext? _context;
    private int _partition;

    public string Name => _innerStore.Name;
    public bool Persistent => true;

    public ChangelogBackedWindowStore(
        IWindowStore<TKey, TValue> innerStore,
        ISerde<TKey> keySerde,
        ISerde<TValue> valueSerde,
        string applicationId,
        ILogger? logger = null)
    {
        _innerStore = innerStore;
        _keySerde = keySerde;
        _valueSerde = valueSerde;
        _applicationId = applicationId;
        _logger = logger ?? NullLogger.Instance;
    }

    public void Init(ProcessorContext context)
    {
        _context = context;
        _partition = context.Partition;

        _innerStore.Init(context);

        var windowedKeySerde = new WindowedKeySerde<TKey>(_keySerde);
        _changelogWriter = new ChangelogWriter<WindowedKey<TKey>, TValue>(
            _applicationId,
            _innerStore.Name,
            _partition,
            windowedKeySerde,
            _valueSerde,
            null,
            _logger);

        _logger.LogDebug("Initialized changelog-backed window store {StoreName} for partition {Partition}",
            Name, _partition);
    }

    public void Put(TKey key, TValue value, long windowStartMs)
    {
        var timestamp = _context?.Timestamp ?? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var windowedKey = new WindowedKey<TKey>(key, windowStartMs);

        _changelogWriter?.Write(windowedKey, value, timestamp);
        _innerStore.Put(key, value, windowStartMs);
    }

    public TValue? Fetch(TKey key, long windowStartMs)
    {
        return _innerStore.Fetch(key, windowStartMs);
    }

    public IEnumerable<KeyValue<Windowed<TKey>, TValue>> Fetch(TKey key, long timeFrom, long timeTo)
    {
        return _innerStore.Fetch(key, timeFrom, timeTo);
    }

    public IEnumerable<KeyValue<Windowed<TKey>, TValue>> FetchAll(long timeFrom, long timeTo)
    {
        return _innerStore.FetchAll(timeFrom, timeTo);
    }

    public void Flush()
    {
        _changelogWriter?.Flush();
        _innerStore.Flush();
    }

    public void Close()
    {
        _changelogWriter?.Dispose();
        _innerStore.Close();
    }

    public void Dispose()
    {
        Close();
    }
}

/// <summary>
/// Serde for windowed keys used in changelog.
/// </summary>
internal sealed class WindowedKeySerde<TKey> : ISerde<WindowedKey<TKey>>
    where TKey : notnull
{
    private readonly ISerde<TKey> _innerSerde;

    public WindowedKeySerde(ISerde<TKey> innerSerde)
    {
        _innerSerde = innerSerde;
    }

    public byte[] Serialize(WindowedKey<TKey> value)
    {
        var keyBytes = _innerSerde.Serialize(value.Key);
        var windowBytes = BitConverter.GetBytes(value.WindowStart);

        var result = new byte[keyBytes.Length + 8];
        Buffer.BlockCopy(keyBytes, 0, result, 0, keyBytes.Length);
        Buffer.BlockCopy(windowBytes, 0, result, keyBytes.Length, 8);

        return result;
    }

    public WindowedKey<TKey> Deserialize(byte[] data)
    {
        var keyBytes = new byte[data.Length - 8];
        Buffer.BlockCopy(data, 0, keyBytes, 0, keyBytes.Length);

        var key = _innerSerde.Deserialize(keyBytes);
        var windowStart = BitConverter.ToInt64(data, keyBytes.Length);

        return new WindowedKey<TKey>(key, windowStart);
    }
}

/// <summary>
/// Represents a key with its window start timestamp for changelog serialization.
/// </summary>
public readonly record struct WindowedKey<TKey>(TKey Key, long WindowStart) where TKey : notnull;
