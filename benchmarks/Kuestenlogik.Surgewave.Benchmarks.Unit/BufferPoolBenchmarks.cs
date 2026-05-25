using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Kuestenlogik.Surgewave.Core.Util;

namespace Kuestenlogik.Surgewave.Benchmarks.Unit;

/// <summary>
/// Benchmarks comparing BufferPool allocations vs regular heap allocations.
/// Measures GC pressure reduction from buffer pooling.
/// </summary>
[SimpleJob(RuntimeMoniker.HostProcess)]
[MemoryDiagnoser]
[RankColumn]
[BenchmarkCategory("Unit", "BufferPool")]
public class BufferPoolBenchmarks
{
    private BufferPool _pool = null!;

    [Params(4096, 65536, 1048576)] // 4KB, 64KB, 1MB
    public int BufferSize { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _pool = new BufferPool();
    }

    /// <summary>
    /// Standard heap allocation (new byte[])
    /// </summary>
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Allocation")]
    public byte[] HeapAllocation()
    {
        return new byte[BufferSize];
    }

    /// <summary>
    /// BufferPool rent (pooled allocation)
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Allocation")]
    public byte[] PooledAllocation()
    {
        var buffer = _pool.Rent(BufferSize);
        _pool.Return(buffer);
        return buffer;
    }

    /// <summary>
    /// Simulate typical usage pattern: rent, fill, return
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Usage")]
    public int TypicalHeapUsage()
    {
        var buffer = new byte[BufferSize];
        Array.Fill(buffer, (byte)0x42);
        return buffer.Length;
    }

    /// <summary>
    /// Simulate typical pooled usage pattern: rent, fill, return
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Usage")]
    public int TypicalPooledUsage()
    {
        var buffer = _pool.Rent(BufferSize);
        Array.Fill(buffer, (byte)0x42);
        _pool.Return(buffer);
        return buffer.Length;
    }

    /// <summary>
    /// Test disposable wrapper overhead
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Allocation")]
    public int RentDisposable()
    {
        using var rented = _pool.RentDisposable(BufferSize);
        return rented.Length;
    }
}
