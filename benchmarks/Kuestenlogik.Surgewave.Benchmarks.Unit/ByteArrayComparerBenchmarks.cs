using System.Diagnostics;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Jobs;
using Kuestenlogik.Surgewave.Core.Util;

namespace Kuestenlogik.Surgewave.Benchmarks.Unit;

/// <summary>
/// Benchmarks comparing SIMD byte array comparison vs naive implementations.
/// Tests both equality checks and hash code generation across different array sizes.
/// Simulates the key comparison workload that happens during log compaction.
/// </summary>
// Each size/operation category has its own naive baseline. Without grouping by category
// BenchmarkDotNet sees six baselines in one job group, refuses to validate, and then runs NOTHING
// in the whole assembly — which is why the regression gate never produced a report.
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
[CategoriesColumn]
[SimpleJob(RuntimeMoniker.HostProcess)]
[MemoryDiagnoser]
[RankColumn]
[BenchmarkCategory("Unit", "Simd")]
public class ByteArrayComparerBenchmarks
{
    private byte[] _small1 = null!;
    private byte[] _small2 = null!;
    private byte[] _medium1 = null!;
    private byte[] _medium2 = null!;
    private byte[] _large1 = null!;
    private byte[] _large2 = null!;

    // For compaction simulation
    private byte[][] _compactionKeys = null!;
    private const int CompactionKeyCount = 10000;
    private const int CompactionKeySize = 32;

    private readonly SimdByteArrayComparer _simdComparer = SimdByteArrayComparer.Instance;
    private readonly NaiveByteArrayComparer _naiveComparer = new();

    [GlobalSetup]
    public void Setup()
    {
        var random = new Random(42);

        // Small arrays (16 bytes - typical Kafka key)
        _small1 = new byte[16];
        _small2 = new byte[16];
        random.NextBytes(_small1);
        Array.Copy(_small1, _small2, 16);

        // Medium arrays (64 bytes)
        _medium1 = new byte[64];
        _medium2 = new byte[64];
        random.NextBytes(_medium1);
        Array.Copy(_medium1, _medium2, 64);

        // Large arrays (256 bytes)
        _large1 = new byte[256];
        _large2 = new byte[256];
        random.NextBytes(_large1);
        Array.Copy(_large1, _large2, 256);

        // Generate keys for compaction simulation (like log compaction key mapping)
        _compactionKeys = new byte[CompactionKeyCount][];
        for (int i = 0; i < CompactionKeyCount; i++)
        {
            _compactionKeys[i] = new byte[CompactionKeySize];
            random.NextBytes(_compactionKeys[i]);
        }

        Console.WriteLine($"SIMD Implementation: {SimdByteArrayComparer.Implementation}");
        Console.WriteLine($"Hardware Accelerated: {SimdByteArrayComparer.IsHardwareAccelerated}");
    }

    // === Equality Benchmarks ===

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Equals", "Small")]
    public bool Equals_Naive_16B() => _naiveComparer.Equals(_small1, _small2);

    [Benchmark]
    [BenchmarkCategory("Equals", "Small")]
    public bool Equals_SIMD_16B() => _simdComparer.Equals(_small1, _small2);

    [Benchmark]
    [BenchmarkCategory("Equals", "Small")]
    public bool Equals_SequenceEqual_16B() => _small1.AsSpan().SequenceEqual(_small2);

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Equals", "Medium")]
    public bool Equals_Naive_64B() => _naiveComparer.Equals(_medium1, _medium2);

    [Benchmark]
    [BenchmarkCategory("Equals", "Medium")]
    public bool Equals_SIMD_64B() => _simdComparer.Equals(_medium1, _medium2);

    [Benchmark]
    [BenchmarkCategory("Equals", "Medium")]
    public bool Equals_SequenceEqual_64B() => _medium1.AsSpan().SequenceEqual(_medium2);

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Equals", "Large")]
    public bool Equals_Naive_256B() => _naiveComparer.Equals(_large1, _large2);

    [Benchmark]
    [BenchmarkCategory("Equals", "Large")]
    public bool Equals_SIMD_256B() => _simdComparer.Equals(_large1, _large2);

    [Benchmark]
    [BenchmarkCategory("Equals", "Large")]
    public bool Equals_SequenceEqual_256B() => _large1.AsSpan().SequenceEqual(_large2);

    // === Hash Code Benchmarks ===

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Hash", "Small")]
    public int Hash_Naive_16B() => _naiveComparer.GetHashCode(_small1);

    [Benchmark]
    [BenchmarkCategory("Hash", "Small")]
    public int Hash_SIMD_16B() => _simdComparer.GetHashCode(_small1);

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Hash", "Medium")]
    public int Hash_Naive_64B() => _naiveComparer.GetHashCode(_medium1);

    [Benchmark]
    [BenchmarkCategory("Hash", "Medium")]
    public int Hash_SIMD_64B() => _simdComparer.GetHashCode(_medium1);

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Hash", "Large")]
    public int Hash_Naive_256B() => _naiveComparer.GetHashCode(_large1);

    [Benchmark]
    [BenchmarkCategory("Hash", "Large")]
    public int Hash_SIMD_256B() => _simdComparer.GetHashCode(_large1);

    // === Dictionary Lookup Benchmarks ===

    private Dictionary<byte[], int> _simdDict = null!;
    private Dictionary<byte[], int> _naiveDict = null!;
    private byte[][] _keys = null!;

    [IterationSetup(Target = nameof(DictLookup_SIMD))]
    public void SetupSimdDict()
    {
        _simdDict = new Dictionary<byte[], int>(SimdByteArrayComparer.Instance);
        _keys = new byte[1000][];
        var random = new Random(42);
        for (int i = 0; i < 1000; i++)
        {
            _keys[i] = new byte[32];
            random.NextBytes(_keys[i]);
            _simdDict[_keys[i]] = i;
        }
    }

    [IterationSetup(Target = nameof(DictLookup_Naive))]
    public void SetupNaiveDict()
    {
        _naiveDict = new Dictionary<byte[], int>(new NaiveByteArrayComparer());
        _keys = new byte[1000][];
        var random = new Random(42);
        for (int i = 0; i < 1000; i++)
        {
            _keys[i] = new byte[32];
            random.NextBytes(_keys[i]);
            _naiveDict[_keys[i]] = i;
        }
    }

    [Benchmark]
    [BenchmarkCategory("Dictionary")]
    public int DictLookup_Naive()
    {
        int sum = 0;
        for (int i = 0; i < 100; i++)
        {
            sum += _naiveDict[_keys[i]];
        }
        return sum;
    }

    [Benchmark]
    [BenchmarkCategory("Dictionary")]
    public int DictLookup_SIMD()
    {
        int sum = 0;
        for (int i = 0; i < 100; i++)
        {
            sum += _simdDict[_keys[i]];
        }
        return sum;
    }

    // === Compaction Simulation Benchmarks ===
    // These simulate the key mapping operations that happen during log compaction:
    // - BuildKeyOffsetMap: For each record, check if key exists, if not add it
    // - CompactSegment: For each record, lookup if this is the latest offset

    /// <summary>
    /// Simulates building the key->offset map during compaction (SIMD comparer).
    /// This is the hot path in BuildKeyOffsetMapAsync.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Compaction")]
    public Dictionary<byte[], long> BuildKeyOffsetMap_SIMD()
    {
        var keyOffsets = new Dictionary<byte[], long>(SimdByteArrayComparer.Instance);
        for (int i = 0; i < _compactionKeys.Length; i++)
        {
            var key = _compactionKeys[i];
            if (!keyOffsets.ContainsKey(key))
            {
                keyOffsets[key] = i;
            }
        }
        return keyOffsets;
    }

    /// <summary>
    /// Simulates building the key->offset map during compaction (Naive comparer).
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Compaction")]
    public Dictionary<byte[], long> BuildKeyOffsetMap_Naive()
    {
        var keyOffsets = new Dictionary<byte[], long>(new NaiveByteArrayComparer());
        for (int i = 0; i < _compactionKeys.Length; i++)
        {
            var key = _compactionKeys[i];
            if (!keyOffsets.ContainsKey(key))
            {
                keyOffsets[key] = i;
            }
        }
        return keyOffsets;
    }

    /// <summary>
    /// Simulates checking if records should be kept during segment compaction (SIMD).
    /// This is the hot path in CompactSegmentAsync.
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Compaction")]
    public int CompactSegmentLookup_SIMD()
    {
        var keyOffsets = new Dictionary<byte[], long>(SimdByteArrayComparer.Instance);
        // First pass: build the map
        for (int i = 0; i < _compactionKeys.Length; i++)
        {
            keyOffsets[_compactionKeys[i]] = i;
        }

        // Second pass: check each key (simulates compaction decision)
        int keptCount = 0;
        for (int i = 0; i < _compactionKeys.Length; i++)
        {
            if (keyOffsets.TryGetValue(_compactionKeys[i], out var latestOffset))
            {
                if (i >= latestOffset)
                {
                    keptCount++;
                }
            }
        }
        return keptCount;
    }

    /// <summary>
    /// Simulates checking if records should be kept during segment compaction (Naive).
    /// </summary>
    [Benchmark]
    [BenchmarkCategory("Compaction")]
    public int CompactSegmentLookup_Naive()
    {
        var keyOffsets = new Dictionary<byte[], long>(new NaiveByteArrayComparer());
        // First pass: build the map
        for (int i = 0; i < _compactionKeys.Length; i++)
        {
            keyOffsets[_compactionKeys[i]] = i;
        }

        // Second pass: check each key (simulates compaction decision)
        int keptCount = 0;
        for (int i = 0; i < _compactionKeys.Length; i++)
        {
            if (keyOffsets.TryGetValue(_compactionKeys[i], out var latestOffset))
            {
                if (i >= latestOffset)
                {
                    keptCount++;
                }
            }
        }
        return keptCount;
    }
}

/// <summary>
/// Naive byte-by-byte comparer for baseline comparison.
/// This is similar to what the original LogCompactor used.
/// </summary>
public sealed class NaiveByteArrayComparer : IEqualityComparer<byte[]>
{
    public bool Equals(byte[]? x, byte[]? y)
    {
        if (ReferenceEquals(x, y)) return true;
        if (x is null || y is null) return false;
        if (x.Length != y.Length) return false;

        for (int i = 0; i < x.Length; i++)
        {
            if (x[i] != y[i]) return false;
        }
        return true;
    }

    public int GetHashCode(byte[] obj)
    {
        if (obj is null || obj.Length == 0) return 0;

        // Simple hash combining bytes
        int hash = 17;
        for (int i = 0; i < obj.Length; i++)
        {
            hash = hash * 31 + obj[i];
        }
        return hash;
    }
}
