using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Kuestenlogik.Surgewave.Streams;
using Kuestenlogik.Surgewave.Streams.Runtime;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kuestenlogik.Surgewave.Benchmarks.Transport;

/// <summary>
/// Benchmarks for <see cref="MappedFileKeyValueStore{TKey,TValue}"/> (LSM-tree backed by MemoryMappedFile)
/// compared with <see cref="InMemoryKeyValueStore{TKey,TValue}"/> as a baseline.
///
/// Covers:
///   Put    — single key-value write
///   Get    — warm-cache point lookup
///   PutGet — put immediately followed by get (round-trip)
///   BulkPut — 1 000 sequential puts in one iteration
///   Range  — range query over a populated store
/// </summary>
[SimpleJob(RuntimeMoniker.HostProcess)]
[MemoryDiagnoser]
[RankColumn]
[BenchmarkCategory("Transport", "SharedMemory", "MappedFileStore")]
public class MappedFileKeyValueStoreBenchmarks : IDisposable
{
    private IKeyValueStore<string, string> _mappedStore = null!;
    private IKeyValueStore<string, string> _inMemoryStore = null!;

    private string _tempDirectory = null!;
    private ProcessorContext _context = null!;
    private StreamsMetrics _metrics = null!;

    private string[] _keys = null!;
    private string[] _values = null!;

    private int _putIndex;

    /// <summary>
    /// Number of entries pre-loaded into the store before benchmarks run.
    /// </summary>
    [Params(500, 5_000)]
    public int PreloadCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _tempDirectory = Path.Combine(Path.GetTempPath(), "surgewave-shm-benchmarks", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDirectory);

        _metrics = new StreamsMetrics();
        var config = new StreamsConfig
        {
            ApplicationId = "shm-bench",
            BootstrapServers = "dummy:9092",
            StateDir = _tempDirectory
        };
        _context = new ProcessorContext(config, _metrics, NullLogger.Instance);

        // Mapped file store
        _mappedStore = new MappedFileKeyValueStore<string, string>(
            "mapped-bench",
            Serdes.String(),
            Serdes.String(),
            new MappedFileStoreConfig { MaxMemTableEntries = 100_000 });
        _mappedStore.Init(_context);

        // In-memory baseline
        _inMemoryStore = new InMemoryKeyValueStore<string, string>("inmemory-bench");
        _inMemoryStore.Init(_context);

        // Pre-generate keys and values (extra 1 000 slots for write benchmarks)
        var total = PreloadCount + 1000;
        _keys = new string[total];
        _values = new string[total];
        for (var i = 0; i < total; i++)
        {
            _keys[i] = $"key-{i:D8}";
            _values[i] = $"value-{i}-{new string('x', 80)}";
        }

        // Pre-load
        for (var i = 0; i < PreloadCount; i++)
        {
            _mappedStore.Put(_keys[i], _values[i]);
            _inMemoryStore.Put(_keys[i], _values[i]);
        }
    }

    [GlobalCleanup]
    public void Cleanup() => Dispose();

    public void Dispose()
    {
        _mappedStore?.Dispose();
        _inMemoryStore?.Dispose();
        _metrics?.Dispose();

        try
        {
            if (Directory.Exists(_tempDirectory))
                Directory.Delete(_tempDirectory, recursive: true);
        }
        catch
        {
            // Ignore cleanup errors on Windows (file locks, etc.)
        }

        GC.SuppressFinalize(this);
    }

    // =========================================================================
    // PUT benchmarks
    // =========================================================================

    /// <summary>
    /// Single put into the memory-mapped LSM store (baseline for MappedFile).
    /// </summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Put")]
    public void MappedFileStore_Put()
    {
        var idx = PreloadCount + (Interlocked.Increment(ref _putIndex) % 1000);
        _mappedStore.Put(_keys[idx], _values[idx]);
    }

    /// <summary>
    /// Single put into an in-memory store (comparison baseline).
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Put")]
    public void InMemoryStore_Put()
    {
        var idx = PreloadCount + (Interlocked.Increment(ref _putIndex) % 1000);
        _inMemoryStore.Put(_keys[idx], _values[idx]);
    }

    // =========================================================================
    // GET benchmarks
    // =========================================================================

    private int _getIndex;

    /// <summary>
    /// Point lookup from the memory-mapped LSM store (warm cache, key exists).
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Get")]
    public string? MappedFileStore_Get()
    {
        var idx = Interlocked.Increment(ref _getIndex) % PreloadCount;
        return _mappedStore.Get(_keys[idx]);
    }

    /// <summary>
    /// Point lookup from the in-memory store (comparison baseline).
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Get")]
    public string? InMemoryStore_Get()
    {
        var idx = Interlocked.Increment(ref _getIndex) % PreloadCount;
        return _inMemoryStore.Get(_keys[idx]);
    }

    // =========================================================================
    // Round-trip (Put → Get)
    // =========================================================================

    private int _rtIndex;

    /// <summary>
    /// Write a key-value pair then immediately read it back from the mapped store.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Roundtrip")]
    public string? MappedFileStore_PutGet_Roundtrip()
    {
        var idx = PreloadCount + (Interlocked.Increment(ref _rtIndex) % 1000);
        _mappedStore.Put(_keys[idx], _values[idx]);
        return _mappedStore.Get(_keys[idx]);
    }

    /// <summary>
    /// Write a key-value pair then immediately read it back from the in-memory store.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Roundtrip")]
    public string? InMemoryStore_PutGet_Roundtrip()
    {
        var idx = PreloadCount + (Interlocked.Increment(ref _rtIndex) % 1000);
        _inMemoryStore.Put(_keys[idx], _values[idx]);
        return _inMemoryStore.Get(_keys[idx]);
    }

    // =========================================================================
    // Bulk PUT (1 000 entries per iteration)
    // =========================================================================

    private int _bulkIndex;

    /// <summary>
    /// 1 000 sequential puts into the memory-mapped LSM store.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("BulkPut")]
    public void MappedFileStore_BulkPut()
    {
        var start = PreloadCount + (Interlocked.Add(ref _bulkIndex, 1000) % 500);
        var entries = new KeyValue<string, string>[1000];
        for (var i = 0; i < 1000; i++)
        {
            var idx = (start + i) % _keys.Length;
            entries[i] = new KeyValue<string, string>(_keys[idx], _values[idx]);
        }
        _mappedStore.PutAll(entries);
    }

    /// <summary>
    /// 1 000 sequential puts into the in-memory store (comparison baseline).
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("BulkPut")]
    public void InMemoryStore_BulkPut()
    {
        var start = PreloadCount + (Interlocked.Add(ref _bulkIndex, 1000) % 500);
        var entries = new KeyValue<string, string>[1000];
        for (var i = 0; i < 1000; i++)
        {
            var idx = (start + i) % _keys.Length;
            entries[i] = new KeyValue<string, string>(_keys[idx], _values[idx]);
        }
        _inMemoryStore.PutAll(entries);
    }

    // =========================================================================
    // Range query
    // =========================================================================

    /// <summary>
    /// Range scan over the middle tenth of the pre-loaded key space in the mapped store.
    /// Returns the count of results to prevent dead-code elimination.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Range")]
    public int MappedFileStore_Range()
    {
        var fromIdx = PreloadCount / 10;
        var toIdx = (PreloadCount / 10) * 2;
        var count = 0;
        foreach (var _ in _mappedStore.Range(_keys[fromIdx], _keys[toIdx]))
            count++;
        return count;
    }

    /// <summary>
    /// Range scan over the middle tenth of the pre-loaded key space in the in-memory store.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Range")]
    public int InMemoryStore_Range()
    {
        var fromIdx = PreloadCount / 10;
        var toIdx = (PreloadCount / 10) * 2;
        var count = 0;
        foreach (var _ in _inMemoryStore.Range(_keys[fromIdx], _keys[toIdx]))
            count++;
        return count;
    }
}
