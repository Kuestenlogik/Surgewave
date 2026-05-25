using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Kuestenlogik.Surgewave.Streams;

namespace Kuestenlogik.Surgewave.Benchmarks.Streams;

/// <summary>
/// Benchmarks for window store operations: Put, Fetch by key, Fetch by time range, FetchAll.
/// Tests InMemory and Persistent window stores.
/// </summary>
[SimpleJob(RuntimeMoniker.HostProcess)]
[MemoryDiagnoser]
[RankColumn]
[BenchmarkCategory("Streams", "Window")]
public class WindowStoreBenchmarks : IDisposable
{
    private IWindowStore<string, int> _store = null!;
    private string _tempDirectory = null!;
    private string[] _keys = null!;
    private long _baseTimestamp;

    [Params("InMemory", "Persistent")]
    public string StoreType { get; set; } = "InMemory";

    [Params(1000, 10_000)]
    public int WindowCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _tempDirectory = Path.Combine(Path.GetTempPath(), "surgewave-bench-window", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDirectory);

        var windowSize = TimeSpan.FromMinutes(1);
        var retention = TimeSpan.FromHours(24);

        _store = StoreType switch
        {
            "Persistent" => new PersistentWindowStore<string, int>(
                "bench-window", windowSize, retention,
                Serdes.String(), Serdes.Int32()),
            _ => new InMemoryWindowStore<string, int>("bench-window", windowSize, retention)
        };

        _baseTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        // Generate keys
        _keys = new string[100];
        for (int i = 0; i < _keys.Length; i++)
            _keys[i] = $"key-{i:D4}";

        // Preload windows
        for (int i = 0; i < WindowCount; i++)
        {
            var key = _keys[i % _keys.Length];
            var windowStart = _baseTimestamp + (i * 60_000L); // 1 minute apart
            _store.Put(key, i, windowStart);
        }
    }

    [GlobalCleanup]
    public void Cleanup() => Dispose();

    public void Dispose()
    {
        _store?.Dispose();
        try
        {
            if (Directory.Exists(_tempDirectory))
                Directory.Delete(_tempDirectory, recursive: true);
        }
        catch { /* ignore */ }
        GC.SuppressFinalize(this);
    }

    private int _putIndex;

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Put")]
    public void Put_Window()
    {
        var idx = Interlocked.Increment(ref _putIndex);
        var key = _keys[idx % _keys.Length];
        var windowStart = _baseTimestamp + ((WindowCount + idx) * 60_000L);
        _store.Put(key, idx, windowStart);
    }

    [Benchmark]
    [BenchmarkCategory("Fetch")]
    public int? Fetch_SingleWindow()
    {
        var key = _keys[0];
        return _store.Fetch(key, _baseTimestamp);
    }

    [Benchmark]
    [BenchmarkCategory("Fetch")]
    public int Fetch_TimeRange_Small()
    {
        var key = _keys[0];
        int count = 0;
        foreach (var _ in _store.Fetch(key, _baseTimestamp, _baseTimestamp + 600_000)) // 10 minutes
            count++;
        return count;
    }

    [Benchmark]
    [BenchmarkCategory("Fetch")]
    public int Fetch_TimeRange_Large()
    {
        var key = _keys[0];
        int count = 0;
        foreach (var _ in _store.Fetch(key, _baseTimestamp, _baseTimestamp + 3_600_000)) // 1 hour
            count++;
        return count;
    }

    [Benchmark]
    [BenchmarkCategory("FetchAll")]
    public int FetchAll_SmallRange()
    {
        int count = 0;
        foreach (var _ in _store.FetchAll(_baseTimestamp, _baseTimestamp + 600_000))
            count++;
        return count;
    }
}
