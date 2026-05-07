using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// Persistent key-value store implementation backed by a file.
/// Uses a write-ahead log for durability and periodic compaction.
/// </summary>
public sealed class PersistentKeyValueStore<TKey, TValue> : IKeyValueStore<TKey, TValue>
    where TKey : notnull
{
    private readonly ConcurrentDictionary<TKey, TValue> _cache = new();
    private readonly IComparer<TKey>? _comparer;
    private readonly ISerde<TKey> _keySerde;
    private readonly ISerde<TValue> _valueSerde;
    private string _stateDir = "";
    private string _logPath = "";
    private string _snapshotPath = "";
    private StreamWriter? _logWriter;
    private ProcessorContext? _context;
    private int _unflushedWrites;
    private const int FlushThreshold = 100;
    private readonly object _lock = new();

    public string Name { get; }
    public bool Persistent => true;
    public long ApproximateNumEntries => _cache.Count;

    public PersistentKeyValueStore(string name, ISerde<TKey> keySerde, ISerde<TValue> valueSerde, IComparer<TKey>? comparer = null)
    {
        Name = name;
        _keySerde = keySerde;
        _valueSerde = valueSerde;
        _comparer = comparer;
    }

    public void Init(ProcessorContext context)
    {
        _context = context;

        // Set up state directory
        _stateDir = Path.Combine(context.Config.StateDir, context.ApplicationId, context.TaskId ?? "default", Name);
        Directory.CreateDirectory(_stateDir);

        _logPath = Path.Combine(_stateDir, "log");
        _snapshotPath = Path.Combine(_stateDir, "snapshot");

        // Restore from snapshot first
        RestoreFromSnapshot();

        // Then replay log for any writes since last snapshot
        ReplayLog();

        // Open log for new writes
        _logWriter = new StreamWriter(new FileStream(_logPath, FileMode.Append, FileAccess.Write, FileShare.Read));
    }

    public TValue? Get(TKey key)
    {
        _cache.TryGetValue(key, out var value);
        return value;
    }

    public void Put(TKey key, TValue value)
    {
        _cache[key] = value;
        WriteToLog(key, value, isDelete: false);
    }

    public TValue? PutIfAbsent(TKey key, TValue value)
    {
        if (_cache.TryAdd(key, value))
        {
            WriteToLog(key, value, isDelete: false);
            return default;
        }
        return _cache[key];
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
        if (_cache.TryRemove(key, out var value))
        {
            WriteToLog(key, default!, isDelete: true);
            return value;
        }
        return default;
    }

    public IEnumerable<KeyValue<TKey, TValue>> Range(TKey from, TKey to)
    {
        if (_comparer == null)
            throw new InvalidOperationException("Range queries require a comparer");

        return _cache
            .Where(kv => _comparer.Compare(kv.Key, from) >= 0 && _comparer.Compare(kv.Key, to) <= 0)
            .OrderBy(kv => kv.Key, _comparer)
            .Select(kv => new KeyValue<TKey, TValue>(kv.Key, kv.Value));
    }

    public IEnumerable<KeyValue<TKey, TValue>> All()
    {
        return _cache.Select(kv => new KeyValue<TKey, TValue>(kv.Key, kv.Value));
    }

    public void Flush()
    {
        lock (_lock)
        {
            _logWriter?.Flush();
            _unflushedWrites = 0;

            // Periodically create snapshot for faster recovery
            if (_cache.Count > 1000)
            {
                CreateSnapshot();
            }
        }
    }

    public void Close()
    {
        lock (_lock)
        {
            CreateSnapshot();
            _logWriter?.Dispose();
            _logWriter = null;

            // Clean up log file after successful snapshot
            if (File.Exists(_logPath))
            {
                try { File.Delete(_logPath); } catch { }
            }
        }
    }

    public void Dispose() => Close();

    private void WriteToLog(TKey key, TValue value, bool isDelete)
    {
        lock (_lock)
        {
            if (_logWriter == null) return;

            var keyBytes = Convert.ToBase64String(_keySerde.Serialize(key));
            var valueBytes = isDelete ? "" : Convert.ToBase64String(_valueSerde.Serialize(value));
            var entry = isDelete ? $"D|{keyBytes}" : $"P|{keyBytes}|{valueBytes}";

            _logWriter.WriteLine(entry);
            _unflushedWrites++;

            if (_unflushedWrites >= FlushThreshold)
            {
                _logWriter.Flush();
                _unflushedWrites = 0;
            }
        }
    }

    private void RestoreFromSnapshot()
    {
        if (!File.Exists(_snapshotPath)) return;

        try
        {
            using var reader = new StreamReader(_snapshotPath);
            while (!reader.EndOfStream)
            {
                var line = reader.ReadLine();
                if (string.IsNullOrEmpty(line)) continue;

                var parts = line.Split('|', 2);
                if (parts.Length != 2) continue;

                var keyBytes = Convert.FromBase64String(parts[0]);
                var valueBytes = Convert.FromBase64String(parts[1]);

                var key = _keySerde.Deserialize(keyBytes);
                var value = _valueSerde.Deserialize(valueBytes);

                _cache[key] = value;
            }

            _context?.Logger.LogDebug("Restored {Count} entries from snapshot for store {Name}",
                _cache.Count, Name);
        }
        catch (Exception ex)
        {
            _context?.Logger.LogWarning(ex, "Failed to restore from snapshot for store {Name}", Name);
        }
    }

    private void ReplayLog()
    {
        if (!File.Exists(_logPath)) return;

        try
        {
            using var reader = new StreamReader(_logPath);
            var replayedCount = 0;

            while (!reader.EndOfStream)
            {
                var line = reader.ReadLine();
                if (string.IsNullOrEmpty(line)) continue;

                var parts = line.Split('|');
                if (parts.Length < 2) continue;

                var op = parts[0];
                var keyBytes = Convert.FromBase64String(parts[1]);
                var key = _keySerde.Deserialize(keyBytes);

                if (op == "D")
                {
                    _cache.TryRemove(key, out _);
                }
                else if (op == "P" && parts.Length >= 3)
                {
                    var valueBytes = Convert.FromBase64String(parts[2]);
                    var value = _valueSerde.Deserialize(valueBytes);
                    _cache[key] = value;
                }

                replayedCount++;
            }

            _context?.Logger.LogDebug("Replayed {Count} log entries for store {Name}",
                replayedCount, Name);
        }
        catch (Exception ex)
        {
            _context?.Logger.LogWarning(ex, "Failed to replay log for store {Name}", Name);
        }
    }

    private void CreateSnapshot()
    {
        var tempPath = _snapshotPath + ".tmp";

        try
        {
            using (var writer = new StreamWriter(tempPath))
            {
                foreach (var kv in _cache)
                {
                    var keyBytes = Convert.ToBase64String(_keySerde.Serialize(kv.Key));
                    var valueBytes = Convert.ToBase64String(_valueSerde.Serialize(kv.Value));
                    writer.WriteLine($"{keyBytes}|{valueBytes}");
                }
            }

            // Atomic replace
            if (File.Exists(_snapshotPath))
            {
                File.Delete(_snapshotPath);
            }
            File.Move(tempPath, _snapshotPath);

            // Clear log after successful snapshot
            _logWriter?.Dispose();
            if (File.Exists(_logPath))
            {
                File.Delete(_logPath);
            }
            _logWriter = new StreamWriter(new FileStream(_logPath, FileMode.Create, FileAccess.Write, FileShare.Read));

            _context?.Logger.LogDebug("Created snapshot for store {Name} with {Count} entries",
                Name, _cache.Count);
        }
        catch (Exception ex)
        {
            _context?.Logger.LogWarning(ex, "Failed to create snapshot for store {Name}", Name);
            if (File.Exists(tempPath))
            {
                try { File.Delete(tempPath); } catch { }
            }
        }
    }
}
