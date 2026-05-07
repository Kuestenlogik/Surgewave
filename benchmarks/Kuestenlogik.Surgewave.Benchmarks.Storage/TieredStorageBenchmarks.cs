using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Kuestenlogik.Surgewave.Storage.Tiering;

namespace Kuestenlogik.Surgewave.Benchmarks.Storage;

/// <summary>
/// Benchmarks for tiered storage operations using LocalFileSystemStorageProvider.
/// Measures upload/download throughput and compares local read vs tiered read latency.
/// </summary>
[SimpleJob(RuntimeMoniker.HostProcess)]
[MemoryDiagnoser]
[RankColumn]
[BenchmarkCategory("TieredStorage")]
public class TieredStorageBenchmarks : IDisposable
{
    private string _localDir = null!;
    private string _tieredDir = null!;
    private LocalFileSystemStorageProvider _provider = null!;

    private byte[] _smallSegment = null!;   //  64 KB
    private byte[] _mediumSegment = null!;  //   1 MB
    private byte[] _largeSegment = null!;   //  16 MB

    private const string Topic = "benchmark-topic";
    private const int Partition = 0;

    [GlobalSetup]
    public void Setup()
    {
        var root = Path.Combine(Path.GetTempPath(), "surgewave-tiered-bench", Guid.NewGuid().ToString("N"));
        _localDir = Path.Combine(root, "local");
        _tieredDir = Path.Combine(root, "tiered");
        Directory.CreateDirectory(_localDir);
        Directory.CreateDirectory(_tieredDir);

        _provider = new LocalFileSystemStorageProvider(_tieredDir);

        _smallSegment = CreatePayload(64 * 1024);
        _mediumSegment = CreatePayload(1 * 1024 * 1024);
        _largeSegment = CreatePayload(16 * 1024 * 1024);

        // Pre-upload segments that download benchmarks will retrieve
        _provider.UploadSegmentAsync(Topic, Partition, baseOffset: 0,
            logData: _smallSegment, indexData: ReadOnlyMemory<byte>.Empty, timeIndexData: ReadOnlyMemory<byte>.Empty).GetAwaiter().GetResult();

        _provider.UploadSegmentAsync(Topic, Partition, baseOffset: 1000,
            logData: _mediumSegment, indexData: ReadOnlyMemory<byte>.Empty, timeIndexData: ReadOnlyMemory<byte>.Empty).GetAwaiter().GetResult();

        _provider.UploadSegmentAsync(Topic, Partition, baseOffset: 2000,
            logData: _largeSegment, indexData: ReadOnlyMemory<byte>.Empty, timeIndexData: ReadOnlyMemory<byte>.Empty).GetAwaiter().GetResult();
    }

    [GlobalCleanup]
    public void Cleanup() => Dispose();

    public void Dispose()
    {
        _provider.DisposeAsync().AsTask().Wait();
        try
        {
            var root = Directory.GetParent(_localDir)!.FullName;
            if (Directory.Exists(root))
                Directory.Delete(root, recursive: true);
        }
        catch { /* Ignore cleanup errors */ }
        GC.SuppressFinalize(this);
    }

    // ── Upload benchmarks ──────────────────────────────────────────────────

    /// <summary>Upload a 64 KB log segment to tiered storage.</summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Upload")]
    public Task Upload_Small_64KB() =>
        _provider.UploadSegmentAsync(Topic, Partition, baseOffset: 10,
            logData: _smallSegment, indexData: ReadOnlyMemory<byte>.Empty, timeIndexData: ReadOnlyMemory<byte>.Empty);

    /// <summary>Upload a 1 MB log segment to tiered storage.</summary>
    [Benchmark]
    [BenchmarkCategory("Upload")]
    public Task Upload_Medium_1MB() =>
        _provider.UploadSegmentAsync(Topic, Partition, baseOffset: 20,
            logData: _mediumSegment, indexData: ReadOnlyMemory<byte>.Empty, timeIndexData: ReadOnlyMemory<byte>.Empty);

    /// <summary>Upload a 16 MB log segment to tiered storage.</summary>
    [Benchmark]
    [BenchmarkCategory("Upload")]
    public Task Upload_Large_16MB() =>
        _provider.UploadSegmentAsync(Topic, Partition, baseOffset: 30,
            logData: _largeSegment, indexData: ReadOnlyMemory<byte>.Empty, timeIndexData: ReadOnlyMemory<byte>.Empty);

    // ── Download benchmarks ────────────────────────────────────────────────

    /// <summary>Download a 64 KB segment from tiered storage (pre-uploaded in setup).</summary>
    [Benchmark]
    [BenchmarkCategory("Download")]
    public Task<(byte[], byte[], byte[])> Download_Small_64KB() =>
        _provider.DownloadSegmentAsync(Topic, Partition, baseOffset: 0);

    /// <summary>Download a 1 MB segment from tiered storage (pre-uploaded in setup).</summary>
    [Benchmark]
    [BenchmarkCategory("Download")]
    public Task<(byte[], byte[], byte[])> Download_Medium_1MB() =>
        _provider.DownloadSegmentAsync(Topic, Partition, baseOffset: 1000);

    /// <summary>Download a 16 MB segment from tiered storage (pre-uploaded in setup).</summary>
    [Benchmark]
    [BenchmarkCategory("Download")]
    public Task<(byte[], byte[], byte[])> Download_Large_16MB() =>
        _provider.DownloadSegmentAsync(Topic, Partition, baseOffset: 2000);

    // ── Local vs tiered read latency comparison ────────────────────────────

    /// <summary>Read a 64 KB segment directly from the local filesystem (baseline for latency comparison).</summary>
    [Benchmark]
    [BenchmarkCategory("LocalVsTiered")]
    public byte[] LocalRead_Small_64KB()
    {
        var path = Path.Combine(_localDir, "local-small.bin");
        if (!File.Exists(path))
            File.WriteAllBytes(path, _smallSegment);
        return File.ReadAllBytes(path);
    }

    /// <summary>Stream-read a 64 KB segment from tiered storage (range fetch, no full download).</summary>
    [Benchmark]
    [BenchmarkCategory("LocalVsTiered")]
    public async Task<int> TieredStreamRead_Small_64KB()
    {
        using var stream = await _provider.FetchLogSegmentAsync(
            Topic, Partition, baseOffset: 0, startPosition: 0, endPosition: null);
        return (int)stream.Length;
    }

    /// <summary>List all segments for a topic partition in tiered storage.</summary>
    [Benchmark]
    [BenchmarkCategory("Metadata")]
    public Task<IReadOnlyList<RemoteSegmentInfo>> ListSegments() =>
        _provider.ListSegmentsAsync(Topic, Partition);

    /// <summary>Check whether a specific segment exists in tiered storage.</summary>
    [Benchmark]
    [BenchmarkCategory("Metadata")]
    public Task<bool> SegmentExists() =>
        _provider.SegmentExistsAsync(Topic, Partition, baseOffset: 0);

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static byte[] CreatePayload(int size)
    {
        var data = new byte[size];
        Random.Shared.NextBytes(data);
        return data;
    }
}
