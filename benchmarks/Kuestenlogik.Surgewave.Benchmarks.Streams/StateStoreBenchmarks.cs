using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Kuestenlogik.Surgewave.Streams;
using Kuestenlogik.Surgewave.Streams.Runtime;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kuestenlogik.Surgewave.Benchmarks.Streams;

/// <summary>
/// Benchmarks for state store operations (Put/Get/Delete/Range) comparing
/// InMemory vs RocksDB vs SQLite vs MappedFile vs Caching store backends.
/// </summary>
[SimpleJob(RuntimeMoniker.HostProcess)]
[MemoryDiagnoser]
[RankColumn]
[BenchmarkCategory("Streams", "StateStore")]
public class StateStoreBenchmarks : IDisposable
{
    private IKeyValueStore<string, string> _store = null!;
    private string _tempDirectory = null!;
    private ProcessorContext _context = null!;
    private StreamsMetrics _metrics = null!;
    private string[] _keys = null!;
    private string[] _values = null!;

    [Params("InMemory", "RocksDb", "Sqlite", "MappedFile", "Caching")]
    public string StoreType { get; set; } = "InMemory";

    [Params(1000, 10_000)]
    public int PreloadCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _tempDirectory = Path.Combine(Path.GetTempPath(), "surgewave-bench-stores", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDirectory);

        _metrics = new StreamsMetrics();
        var config = new StreamsConfig
        {
            ApplicationId = "bench",
            BootstrapServers = "dummy:9092",
            StateDir = _tempDirectory
        };
        _context = new ProcessorContext(config, _metrics, NullLogger.Instance);

        _store = CreateStore();
        _store.Init(_context);

        // Pre-generate keys and values
        _keys = new string[PreloadCount + 1000];
        _values = new string[_keys.Length];
        for (int i = 0; i < _keys.Length; i++)
        {
            _keys[i] = $"key-{i:D8}";
            _values[i] = $"value-{i}-{new string('x', 100)}";
        }

        // Preload data
        for (int i = 0; i < PreloadCount; i++)
        {
            _store.Put(_keys[i], _values[i]);
        }
    }

    private IKeyValueStore<string, string> CreateStore()
    {
        var keySerde = Serdes.String();
        var valueSerde = Serdes.String();

#pragma warning disable CA2000 // Ownership transferred to CachingKeyValueStore
        return StoreType switch
        {
            "RocksDb" => new RocksDbKeyValueStore<string, string>("bench-store", keySerde, valueSerde),
            "Sqlite" => new SqliteKeyValueStore<string, string>("bench-store", keySerde, valueSerde),
            "MappedFile" => new MappedFileKeyValueStore<string, string>("bench-store", keySerde, valueSerde),
            "Caching" => new CachingKeyValueStore<string, string>(
                new InMemoryKeyValueStore<string, string>("bench-inner"),
                maxCacheSize: 50_000),
            _ => new InMemoryKeyValueStore<string, string>("bench-store")
        };
#pragma warning restore CA2000
    }

    [GlobalCleanup]
    public void Cleanup() => Dispose();

    public void Dispose()
    {
        _store?.Dispose();
        _metrics?.Dispose();
        try
        {
            if (Directory.Exists(_tempDirectory))
                Directory.Delete(_tempDirectory, recursive: true);
        }
        catch { /* ignore cleanup errors */ }
        GC.SuppressFinalize(this);
    }

    // === PUT BENCHMARKS ===

    private int _putIndex;

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Put")]
    public void Put_Single()
    {
        var idx = PreloadCount + (Interlocked.Increment(ref _putIndex) % 1000);
        _store.Put(_keys[idx], _values[idx]);
    }

    [Benchmark]
    [BenchmarkCategory("Put")]
    public void Put_Batch_100()
    {
        var entries = new KeyValue<string, string>[100];
        for (int i = 0; i < 100; i++)
        {
            var idx = PreloadCount + ((i + _putIndex) % 1000);
            entries[i] = new KeyValue<string, string>(_keys[idx], _values[idx]);
        }
        _store.PutAll(entries);
        Interlocked.Add(ref _putIndex, 100);
    }

    // === GET BENCHMARKS ===

    private int _getIndex;

    [Benchmark]
    [BenchmarkCategory("Get")]
    public string? Get_Existing()
    {
        var idx = Interlocked.Increment(ref _getIndex) % PreloadCount;
        return _store.Get(_keys[idx]);
    }

    [Benchmark]
    [BenchmarkCategory("Get")]
    public string? Get_Missing()
    {
        return _store.Get("nonexistent-key-12345");
    }

    // === DELETE BENCHMARKS ===

    [Benchmark]
    [BenchmarkCategory("Delete")]
    public string? Delete_Single()
    {
        // Delete and re-insert to keep store populated
        var idx = Interlocked.Increment(ref _getIndex) % PreloadCount;
        var result = _store.Delete(_keys[idx]);
        _store.Put(_keys[idx], _values[idx]);
        return result;
    }

    // === ENUMERATE BENCHMARKS ===

    [Benchmark]
    [BenchmarkCategory("Enumerate")]
    public int All_Count()
    {
        int count = 0;
        foreach (var _ in _store.All())
            count++;
        return count;
    }
}
