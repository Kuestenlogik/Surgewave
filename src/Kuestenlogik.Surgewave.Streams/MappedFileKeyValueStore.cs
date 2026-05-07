using System.IO.MemoryMappedFiles;
using Kuestenlogik.Surgewave.Streams.Monitoring;

namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// Memory-mapped file key-value store using a simplified LSM-tree architecture.
/// Zero external dependencies — uses only .NET MemoryMappedFile, SortedDictionary, and binary I/O.
///
/// Architecture:
///   MemTable (in-memory SortedDictionary) + WAL → flush → immutable Segments (mmapped files)
///   Read path: MemTable → newest segment → ... → oldest segment
///   Compaction merges segments to reclaim space and remove tombstones.
/// </summary>
public sealed class MappedFileKeyValueStore<TKey, TValue> : IKeyValueStore<TKey, TValue>
    where TKey : notnull
{
    private readonly ISerde<TKey> _keySerde;
    private readonly ISerde<TValue> _valueSerde;
    private readonly MappedFileStoreConfig _config;

    private string _storeDir = "";
    private ProcessorContext? _context;
    private StateStoreMetrics? _metrics;

    // Active write state
    private SortedDictionary<byte[], byte[]?> _memTable = new(ByteArrayComparer.Instance);
    private FileStream? _walStream;
    private BinaryWriter? _walWriter;

    // Immutable segments, newest first
    private readonly List<Segment> _segments = [];

    private long _approximateEntries;
    private long _nextSegmentSeq;
    private bool _disposed;

    public string Name { get; }
    public bool Persistent => true;
    public long ApproximateNumEntries => Interlocked.Read(ref _approximateEntries);

    public MappedFileKeyValueStore(
        string name,
        ISerde<TKey> keySerde,
        ISerde<TValue> valueSerde,
        MappedFileStoreConfig? config = null)
    {
        Name = name;
        _keySerde = keySerde;
        _valueSerde = valueSerde;
        _config = config ?? new MappedFileStoreConfig();
    }

    public void Init(ProcessorContext context)
    {
        _context = context;

        var stateDir = context.Config.StateDir ?? Path.Combine(Path.GetTempPath(), "surgewave-streams");
        _storeDir = Path.Combine(stateDir, context.ApplicationId, context.TaskId ?? "default", Name);
        Directory.CreateDirectory(_storeDir);

        // Discover and open existing segments
        LoadExistingSegments();

        // Replay WAL into MemTable
        ReplayWal();

        // Open WAL for new writes
        var walPath = Path.Combine(_storeDir, "wal.bin");
        _walStream = new FileStream(walPath, FileMode.Append, FileAccess.Write, FileShare.Read);
        _walWriter = new BinaryWriter(_walStream);

        UpdateApproximateEntries();
        _metrics = context.Metrics.GetOrCreateStoreMetrics(Name, () => ApproximateNumEntries);
    }

    public TValue? Get(TKey key)
    {
        var keyBytes = _keySerde.Serialize(key);

        // 1. Check MemTable
        if (_memTable.TryGetValue(keyBytes, out var memValue))
        {
            _metrics?.RecordGet();
            return memValue == null ? default : _valueSerde.Deserialize(memValue);
        }

        // 2. Check segments newest-to-oldest
        for (var i = 0; i < _segments.Count; i++)
        {
            var (value, found, tombstone) = _segments[i].Find(keyBytes);
            if (found)
            {
                _metrics?.RecordGet();
                return tombstone ? default : _valueSerde.Deserialize(value!);
            }
        }

        _metrics?.RecordGet();
        return default;
    }

    public void Put(TKey key, TValue value)
    {
        var keyBytes = _keySerde.Serialize(key);
        var valueBytes = _valueSerde.Serialize(value);

        _memTable[keyBytes] = valueBytes;
        WriteWal(WalOp.Put, keyBytes, valueBytes);
        Interlocked.Increment(ref _approximateEntries);
        _metrics?.RecordPut();

        if (_memTable.Count >= _config.MaxMemTableEntries)
            FlushMemTable();
    }

    public TValue? PutIfAbsent(TKey key, TValue value)
    {
        var keyBytes = _keySerde.Serialize(key);

        // Check MemTable
        if (_memTable.TryGetValue(keyBytes, out var existing) && existing != null)
            return _valueSerde.Deserialize(existing);

        // Check segments
        for (var i = 0; i < _segments.Count; i++)
        {
            var (val, found, tombstone) = _segments[i].Find(keyBytes);
            if (found && !tombstone)
                return _valueSerde.Deserialize(val!);
        }

        // Not found — insert
        var valueBytes = _valueSerde.Serialize(value);
        _memTable[keyBytes] = valueBytes;
        WriteWal(WalOp.Put, keyBytes, valueBytes);
        Interlocked.Increment(ref _approximateEntries);
        return default;
    }

    public void PutAll(IEnumerable<KeyValue<TKey, TValue>> entries)
    {
        var count = 0;
        foreach (var entry in entries)
        {
            var keyBytes = _keySerde.Serialize(entry.Key);
            var valueBytes = _valueSerde.Serialize(entry.Value);
            _memTable[keyBytes] = valueBytes;
            WriteWal(WalOp.Put, keyBytes, valueBytes);
            count++;
        }

        Interlocked.Add(ref _approximateEntries, count);
        _metrics?.RecordPut(count);

        if (_memTable.Count >= _config.MaxMemTableEntries)
            FlushMemTable();
    }

    public TValue? Delete(TKey key)
    {
        var keyBytes = _keySerde.Serialize(key);
        var existing = Get(key);

        // Write tombstone
        _memTable[keyBytes] = null;
        WriteWal(WalOp.Delete, keyBytes, null);

        if (existing != null || !EqualityComparer<TValue>.Default.Equals(existing, default))
        {
            Interlocked.Decrement(ref _approximateEntries);
            _metrics?.RecordDelete();
        }

        return existing;
    }

    public IEnumerable<KeyValue<TKey, TValue>> Range(TKey from, TKey to)
    {
        var fromBytes = _keySerde.Serialize(from);
        var toBytes = _keySerde.Serialize(to);

        // Merge all sources: oldest segments first, then MemTable (newest wins)
        var merged = new SortedDictionary<byte[], byte[]?>(ByteArrayComparer.Instance);

        for (var i = _segments.Count - 1; i >= 0; i--)
        {
            foreach (var (k, v) in _segments[i].Scan(fromBytes, toBytes))
                merged[k] = v;
        }

        foreach (var kv in _memTable)
        {
            if (ByteArrayComparer.Instance.Compare(kv.Key, fromBytes) >= 0 &&
                ByteArrayComparer.Instance.Compare(kv.Key, toBytes) <= 0)
            {
                merged[kv.Key] = kv.Value;
            }
        }

        foreach (var kv in merged)
        {
            if (kv.Value != null)
            {
                yield return new KeyValue<TKey, TValue>(
                    _keySerde.Deserialize(kv.Key),
                    _valueSerde.Deserialize(kv.Value));
            }
        }
    }

    public IEnumerable<KeyValue<TKey, TValue>> All()
    {
        var merged = new SortedDictionary<byte[], byte[]?>(ByteArrayComparer.Instance);

        for (var i = _segments.Count - 1; i >= 0; i--)
        {
            foreach (var (k, v) in _segments[i].ScanAll())
                merged[k] = v;
        }

        foreach (var kv in _memTable)
            merged[kv.Key] = kv.Value;

        foreach (var kv in merged)
        {
            if (kv.Value != null)
            {
                yield return new KeyValue<TKey, TValue>(
                    _keySerde.Deserialize(kv.Key),
                    _valueSerde.Deserialize(kv.Value));
            }
        }
    }

    public void Flush()
    {
        if (_memTable.Count > 0)
            FlushMemTable();

        if (_segments.Count > _config.MaxSegmentsBeforeCompaction)
            Compact();

        UpdateApproximateEntries();
    }

    public void Close()
    {
        if (_disposed) return;

        // Flush remaining MemTable to segment
        if (_memTable.Count > 0)
            FlushMemTable();

        _walWriter?.Dispose();
        _walStream?.Dispose();
        _walWriter = null;
        _walStream = null;

        foreach (var segment in _segments)
            segment.Dispose();
        _segments.Clear();

        // Delete WAL after clean shutdown
        var walPath = Path.Combine(_storeDir, "wal.bin");
        try { if (File.Exists(walPath)) File.Delete(walPath); } catch { }

        _disposed = true;
    }

    public void Dispose() => Close();

    // --- Internal operations ---

    [System.Diagnostics.CodeAnalysis.SuppressMessage("Reliability", "CA2000", Justification = "Segment is tracked in _segments list and disposed in Close/Compact")]
    private void FlushMemTable()
    {
        if (_memTable.Count == 0) return;

        var seq = _nextSegmentSeq++;
        var segPath = Path.Combine(_storeDir, $"segment_{seq:D8}.dat");

        Segment.Write(segPath, _memTable);

        var segment = Segment.Open(segPath, seq);
        _segments.Insert(0, segment); // newest first

        _memTable = new SortedDictionary<byte[], byte[]?>(ByteArrayComparer.Instance);

        // Reset WAL
        _walWriter?.Dispose();
        _walStream?.Dispose();

        var walPath = Path.Combine(_storeDir, "wal.bin");
        _walStream = new FileStream(walPath, FileMode.Create, FileAccess.Write, FileShare.Read);
        _walWriter = new BinaryWriter(_walStream);
    }

    private void Compact()
    {
        if (_segments.Count <= 1) return;

        // Merge all segments into one: iterate oldest-to-newest, newest wins
        var merged = new SortedDictionary<byte[], byte[]?>(ByteArrayComparer.Instance);
        for (var i = _segments.Count - 1; i >= 0; i--)
        {
            foreach (var (k, v) in _segments[i].ScanAll())
                merged[k] = v;
        }

        // Remove tombstones during compaction
        var keysToRemove = new List<byte[]>();
        foreach (var kv in merged)
        {
            if (kv.Value == null)
                keysToRemove.Add(kv.Key);
        }
        foreach (var k in keysToRemove)
            merged.Remove(k);

        var seq = _nextSegmentSeq++;
        var newPath = Path.Combine(_storeDir, $"segment_{seq:D8}.dat");
        Segment.Write(newPath, merged);

        // Close old segments
        var oldSegments = new List<Segment>(_segments);
        _segments.Clear();

        foreach (var old in oldSegments)
        {
            var path = old.FilePath;
            old.Dispose();
            try { File.Delete(path); } catch { }
        }

        // Open new compacted segment
        _segments.Add(Segment.Open(newPath, seq));
    }

    private void WriteWal(WalOp op, byte[] keyBytes, byte[]? valueBytes)
    {
        if (_walWriter == null) return;

        _walWriter.Write((byte)op);
        _walWriter.Write(keyBytes.Length);
        _walWriter.Write(keyBytes);

        if (op == WalOp.Put && valueBytes != null)
        {
            _walWriter.Write(valueBytes.Length);
            _walWriter.Write(valueBytes);
        }

        _walWriter.Flush();
    }

    private void ReplayWal()
    {
        var walPath = Path.Combine(_storeDir, "wal.bin");
        if (!File.Exists(walPath)) return;

        try
        {
            using var fs = new FileStream(walPath, FileMode.Open, FileAccess.Read, FileShare.Read);
            using var reader = new BinaryReader(fs);

            while (fs.Position < fs.Length)
            {
                var op = (WalOp)reader.ReadByte();
                var keyLen = reader.ReadInt32();
                var keyBytes = reader.ReadBytes(keyLen);

                if (op == WalOp.Put)
                {
                    var valLen = reader.ReadInt32();
                    var valBytes = reader.ReadBytes(valLen);
                    _memTable[keyBytes] = valBytes;
                }
                else
                {
                    _memTable[keyBytes] = null; // tombstone
                }
            }
        }
        catch
        {
            // Partial WAL — recover what we can
        }
    }

    private void LoadExistingSegments()
    {
        var segFiles = Directory.GetFiles(_storeDir, "segment_*.dat");
        Array.Sort(segFiles, StringComparer.Ordinal);

        foreach (var path in segFiles)
        {
            var fileName = Path.GetFileNameWithoutExtension(path);
            var seqStr = fileName.Replace("segment_", "");
            if (long.TryParse(seqStr, out var seq))
            {
                _segments.Add(Segment.Open(path, seq));
                if (seq >= _nextSegmentSeq)
                    _nextSegmentSeq = seq + 1;
            }
        }

        // Newest first
        _segments.Reverse();
    }

    private void UpdateApproximateEntries()
    {
        long count = _memTable.Count(kv => kv.Value != null);
        foreach (var seg in _segments)
            count += seg.EntryCount;
        Interlocked.Exchange(ref _approximateEntries, Math.Max(0, count));
    }

    private enum WalOp : byte
    {
        Put = 1,
        Delete = 2
    }

    /// <summary>
    /// Immutable sorted segment backed by a memory-mapped file.
    /// Format: [4:count][entries sorted by key...]
    /// Entry: [4:keyLen][keyBytes][4:valLen][valBytes] — valLen=-1 for tombstone
    /// </summary>
    private sealed class Segment : IDisposable
    {
        private readonly MemoryMappedFile _mmf;
        private readonly MemoryMappedViewAccessor _accessor;
        private readonly long[] _entryOffsets;
        private readonly long _fileSize;

        public string FilePath { get; }
        public long SequenceNumber { get; }
        public int EntryCount => _entryOffsets.Length;

        private Segment(
            string filePath,
            long sequenceNumber,
            MemoryMappedFile mmf,
            MemoryMappedViewAccessor accessor,
            long[] entryOffsets,
            long fileSize)
        {
            FilePath = filePath;
            SequenceNumber = sequenceNumber;
            _mmf = mmf;
            _accessor = accessor;
            _entryOffsets = entryOffsets;
            _fileSize = fileSize;
        }

        public static Segment Open(string path, long sequenceNumber)
        {
            var fileSize = new FileInfo(path).Length;
            var mmf = MemoryMappedFile.CreateFromFile(path, FileMode.Open, null, 0, MemoryMappedFileAccess.Read);
            var accessor = mmf.CreateViewAccessor(0, fileSize, MemoryMappedFileAccess.Read);

            var count = accessor.ReadInt32(0);
            var offsets = new long[count];
            long pos = 4;

            for (var i = 0; i < count; i++)
            {
                offsets[i] = pos;
                var keyLen = accessor.ReadInt32(pos);
                pos += 4 + keyLen;
                var valLen = accessor.ReadInt32(pos);
                pos += 4 + (valLen >= 0 ? valLen : 0);
            }

            return new Segment(path, sequenceNumber, mmf, accessor, offsets, fileSize);
        }

        public static void Write(string path, SortedDictionary<byte[], byte[]?> entries)
        {
            using var fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None);
            using var writer = new BinaryWriter(fs);

            writer.Write(entries.Count);

            foreach (var kv in entries)
            {
                writer.Write(kv.Key.Length);
                writer.Write(kv.Key);

                if (kv.Value != null)
                {
                    writer.Write(kv.Value.Length);
                    writer.Write(kv.Value);
                }
                else
                {
                    writer.Write(-1); // tombstone
                }
            }

            writer.Flush();
        }

        /// <summary>
        /// Binary search for a key. Returns (value, found, isTombstone).
        /// </summary>
        public (byte[]? value, bool found, bool tombstone) Find(byte[] keyBytes)
        {
            var lo = 0;
            var hi = _entryOffsets.Length - 1;

            while (lo <= hi)
            {
                var mid = lo + (hi - lo) / 2;
                var (midKey, _) = ReadEntry(mid);
                var cmp = ByteArrayComparer.Instance.Compare(midKey, keyBytes);

                if (cmp == 0)
                {
                    var (_, val) = ReadEntry(mid);
                    return val == null ? (null, true, true) : (val, true, false);
                }
                else if (cmp < 0)
                {
                    lo = mid + 1;
                }
                else
                {
                    hi = mid - 1;
                }
            }

            return (null, false, false);
        }

        /// <summary>
        /// Scan entries within [fromKey, toKey] range.
        /// </summary>
        public IEnumerable<(byte[] key, byte[]? value)> Scan(byte[] fromKey, byte[] toKey)
        {
            // Binary search for start position
            var startIdx = LowerBound(fromKey);

            for (var i = startIdx; i < _entryOffsets.Length; i++)
            {
                var (key, value) = ReadEntry(i);
                if (ByteArrayComparer.Instance.Compare(key, toKey) > 0)
                    yield break;
                yield return (key, value);
            }
        }

        /// <summary>
        /// Scan all entries.
        /// </summary>
        public IEnumerable<(byte[] key, byte[]? value)> ScanAll()
        {
            for (var i = 0; i < _entryOffsets.Length; i++)
            {
                yield return ReadEntry(i);
            }
        }

        private int LowerBound(byte[] keyBytes)
        {
            var lo = 0;
            var hi = _entryOffsets.Length;

            while (lo < hi)
            {
                var mid = lo + (hi - lo) / 2;
                var (midKey, _) = ReadEntry(mid);
                if (ByteArrayComparer.Instance.Compare(midKey, keyBytes) < 0)
                    lo = mid + 1;
                else
                    hi = mid;
            }

            return lo;
        }

        private (byte[] key, byte[]? value) ReadEntry(int index)
        {
            var pos = _entryOffsets[index];

            var keyLen = _accessor.ReadInt32(pos);
            pos += 4;
            var key = new byte[keyLen];
            _accessor.ReadArray(pos, key, 0, keyLen);
            pos += keyLen;

            var valLen = _accessor.ReadInt32(pos);
            pos += 4;

            if (valLen < 0)
                return (key, null); // tombstone

            var value = new byte[valLen];
            _accessor.ReadArray(pos, value, 0, valLen);
            return (key, value);
        }

        public void Dispose()
        {
            _accessor.Dispose();
            _mmf.Dispose();
        }
    }

    /// <summary>
    /// Lexicographic byte array comparer.
    /// </summary>
    internal sealed class ByteArrayComparer : IComparer<byte[]>
    {
        public static readonly ByteArrayComparer Instance = new();

        public int Compare(byte[]? x, byte[]? y)
        {
            if (ReferenceEquals(x, y)) return 0;
            if (x == null) return -1;
            if (y == null) return 1;

            var len = Math.Min(x.Length, y.Length);
            for (var i = 0; i < len; i++)
            {
                if (x[i] != y[i])
                    return x[i].CompareTo(y[i]);
            }

            return x.Length.CompareTo(y.Length);
        }
    }
}

/// <summary>
/// Configuration for the memory-mapped file state store.
/// </summary>
public sealed class MappedFileStoreConfig
{
    /// <summary>
    /// Max entries in MemTable before flushing to a segment (default: 10,000).
    /// </summary>
    public int MaxMemTableEntries { get; init; } = 10_000;

    /// <summary>
    /// Max number of segments before triggering compaction (default: 8).
    /// </summary>
    public int MaxSegmentsBeforeCompaction { get; init; } = 8;
}
