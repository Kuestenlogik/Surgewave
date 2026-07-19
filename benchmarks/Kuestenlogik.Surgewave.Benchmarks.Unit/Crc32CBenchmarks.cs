using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using Kuestenlogik.Surgewave.Core.Util;

namespace Kuestenlogik.Surgewave.Benchmarks.Unit;

/// <summary>
/// #85 S2: the serial single-chain CRC32C vs the 3-way interleaved kernel across batch sizes. Below the
/// interleave threshold (3072 B) the interleaved path degrades to the serial tail (no regression); above
/// it, three independent <c>crc32</c> chains hide the ~3-cycle instruction latency for a large-buffer
/// speedup. Per-size category baselines (serial) so the <c>ByCategory</c> grouping validates — without it
/// BenchmarkDotNet sees many baselines in one group, refuses to validate, and runs the whole assembly
/// empty (see ByteArrayComparerBenchmarks).
/// </summary>
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
[SimpleJob(RuntimeMoniker.HostProcess)]
[MemoryDiagnoser]
[RankColumn]
[BenchmarkCategory("Unit", "Simd")]
public class Crc32CBenchmarks
{
    private byte[] _b120 = null!;
    private byte[] _b1k = null!;
    private byte[] _b4k = null!;
    private byte[] _b16k = null!;
    private byte[] _b64k = null!;

    [GlobalSetup]
    public void Setup()
    {
        var rng = new Random(42);
        _b120 = NewBuffer(rng, 120);
        _b1k = NewBuffer(rng, 1024);
        _b4k = NewBuffer(rng, 4096);
        _b16k = NewBuffer(rng, 16384);
        _b64k = NewBuffer(rng, 65536);
        Console.WriteLine($"CRC32C Implementation: {Crc32C.Implementation}");
    }

    private static byte[] NewBuffer(Random rng, int size)
    {
        var b = new byte[size];
        rng.NextBytes(b);
        return b;
    }

    // Below the 3072 B threshold — interleaved == serial tail, must NOT regress.
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("120B")]
    public uint Serial_120B() => Crc32C.ComputeSse42X64Serial(_b120);

    [Benchmark]
    [BenchmarkCategory("120B")]
    public uint Interleaved_120B() => Crc32C.ComputeSse42X64Interleaved(_b120);

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("1KB")]
    public uint Serial_1KB() => Crc32C.ComputeSse42X64Serial(_b1k);

    [Benchmark]
    [BenchmarkCategory("1KB")]
    public uint Interleaved_1KB() => Crc32C.ComputeSse42X64Interleaved(_b1k);

    // Above the threshold — the 3-way interleave should win.
    [Benchmark(Baseline = true)]
    [BenchmarkCategory("4KB")]
    public uint Serial_4KB() => Crc32C.ComputeSse42X64Serial(_b4k);

    [Benchmark]
    [BenchmarkCategory("4KB")]
    public uint Interleaved_4KB() => Crc32C.ComputeSse42X64Interleaved(_b4k);

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("16KB")]
    public uint Serial_16KB() => Crc32C.ComputeSse42X64Serial(_b16k);

    [Benchmark]
    [BenchmarkCategory("16KB")]
    public uint Interleaved_16KB() => Crc32C.ComputeSse42X64Interleaved(_b16k);

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("64KB")]
    public uint Serial_64KB() => Crc32C.ComputeSse42X64Serial(_b64k);

    [Benchmark]
    [BenchmarkCategory("64KB")]
    public uint Interleaved_64KB() => Crc32C.ComputeSse42X64Interleaved(_b64k);
}
