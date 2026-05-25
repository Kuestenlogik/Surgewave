using System.Buffers.Binary;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Kuestenlogik.Surgewave.Core.Util;

namespace Kuestenlogik.Surgewave.Benchmarks.Unit;

/// <summary>
/// Benchmarks for SIMD-optimized big-endian batch write/read operations.
/// Compares SIMD batch methods vs scalar BinaryPrimitives for different array sizes.
/// </summary>
[SimpleJob(RuntimeMoniker.HostProcess)]
[MemoryDiagnoser]
[RankColumn]
[BenchmarkCategory("Unit", "Simd")]
public class SimdBigEndianBenchmarks
{
    private long[] _int64Values = null!;
    private int[] _int32Values = null!;
    private short[] _int16Values = null!;
    private byte[] _writeBuffer = null!;
    private byte[] _readBuffer = null!;

    [Params(2, 4, 8, 16, 32, 64, 128)]
    public int Count { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        Console.WriteLine($"SIMD BigEndian Implementation: {SimdBigEndian.Implementation}");
        Console.WriteLine($"Hardware Accelerated: {SimdBigEndian.IsHardwareAccelerated}");

        _int64Values = new long[Count];
        _int32Values = new int[Count];
        _int16Values = new short[Count];

        var random = new Random(42);
        for (int i = 0; i < Count; i++)
        {
            _int64Values[i] = random.NextInt64();
            _int32Values[i] = random.Next();
            _int16Values[i] = (short)random.Next();
        }

        // Allocate buffers large enough for any type
        _writeBuffer = new byte[Count * 8];
        _readBuffer = new byte[Count * 8];

        // Fill read buffer with big-endian data
        for (int i = 0; i < Count; i++)
        {
            BinaryPrimitives.WriteInt64BigEndian(_readBuffer.AsSpan(i * 8, 8), _int64Values[i]);
        }
    }

    // === Int64 Write Benchmarks ===

    [Benchmark]
    [BenchmarkCategory("Write", "Int64")]
    public void WriteInt64s_Scalar()
    {
        var span = _writeBuffer.AsSpan();
        for (int i = 0; i < _int64Values.Length; i++)
        {
            BinaryPrimitives.WriteInt64BigEndian(span.Slice(i * 8, 8), _int64Values[i]);
        }
    }

    [Benchmark]
    [BenchmarkCategory("Write", "Int64")]
    public void WriteInt64s_SIMD()
    {
        SimdBigEndian.WriteInt64sBigEndian(_writeBuffer, _int64Values);
    }

    // === Int32 Write Benchmarks ===

    [Benchmark]
    [BenchmarkCategory("Write", "Int32")]
    public void WriteInt32s_Scalar()
    {
        var span = _writeBuffer.AsSpan();
        for (int i = 0; i < _int32Values.Length; i++)
        {
            BinaryPrimitives.WriteInt32BigEndian(span.Slice(i * 4, 4), _int32Values[i]);
        }
    }

    [Benchmark]
    [BenchmarkCategory("Write", "Int32")]
    public void WriteInt32s_SIMD()
    {
        SimdBigEndian.WriteInt32sBigEndian(_writeBuffer, _int32Values);
    }

    // === Int16 Write Benchmarks ===

    [Benchmark]
    [BenchmarkCategory("Write", "Int16")]
    public void WriteInt16s_Scalar()
    {
        var span = _writeBuffer.AsSpan();
        for (int i = 0; i < _int16Values.Length; i++)
        {
            BinaryPrimitives.WriteInt16BigEndian(span.Slice(i * 2, 2), _int16Values[i]);
        }
    }

    [Benchmark]
    [BenchmarkCategory("Write", "Int16")]
    public void WriteInt16s_SIMD()
    {
        SimdBigEndian.WriteInt16sBigEndian(_writeBuffer, _int16Values);
    }

    // === Int64 Read Benchmarks ===

    [Benchmark]
    [BenchmarkCategory("Read", "Int64")]
    public long ReadInt64s_Scalar()
    {
        long sum = 0;
        var span = _readBuffer.AsSpan();
        for (int i = 0; i < Count; i++)
        {
            sum += BinaryPrimitives.ReadInt64BigEndian(span.Slice(i * 8, 8));
        }
        return sum;
    }

    [Benchmark]
    [BenchmarkCategory("Read", "Int64")]
    public long ReadInt64s_SIMD()
    {
        Span<long> values = stackalloc long[Count];
        SimdBigEndian.ReadInt64sBigEndian(_readBuffer, values);
        long sum = 0;
        for (int i = 0; i < Count; i++)
        {
            sum += values[i];
        }
        return sum;
    }

    // === Int32 Read Benchmarks ===

    [Benchmark]
    [BenchmarkCategory("Read", "Int32")]
    public int ReadInt32s_Scalar()
    {
        int sum = 0;
        var span = _readBuffer.AsSpan();
        for (int i = 0; i < Count; i++)
        {
            sum += BinaryPrimitives.ReadInt32BigEndian(span.Slice(i * 4, 4));
        }
        return sum;
    }

    [Benchmark]
    [BenchmarkCategory("Read", "Int32")]
    public int ReadInt32s_SIMD()
    {
        Span<int> values = stackalloc int[Count];
        SimdBigEndian.ReadInt32sBigEndian(_readBuffer, values);
        int sum = 0;
        for (int i = 0; i < Count; i++)
        {
            sum += values[i];
        }
        return sum;
    }

    // === Pair Write Benchmarks (common pattern: offset + timestamp) ===

    [Benchmark]
    [BenchmarkCategory("Pair")]
    public void Write2Int64s_Scalar()
    {
        BinaryPrimitives.WriteInt64BigEndian(_writeBuffer.AsSpan(0, 8), 12345678901234L);
        BinaryPrimitives.WriteInt64BigEndian(_writeBuffer.AsSpan(8, 8), 98765432109876L);
    }

    [Benchmark]
    [BenchmarkCategory("Pair")]
    public void Write2Int64s_SIMD()
    {
        SimdBigEndian.Write2Int64sBigEndian(_writeBuffer, 12345678901234L, 98765432109876L);
    }

    [Benchmark]
    [BenchmarkCategory("Pair")]
    public void Write4Int32s_Scalar()
    {
        BinaryPrimitives.WriteInt32BigEndian(_writeBuffer.AsSpan(0, 4), 123);
        BinaryPrimitives.WriteInt32BigEndian(_writeBuffer.AsSpan(4, 4), 456);
        BinaryPrimitives.WriteInt32BigEndian(_writeBuffer.AsSpan(8, 4), 789);
        BinaryPrimitives.WriteInt32BigEndian(_writeBuffer.AsSpan(12, 4), 101112);
    }

    [Benchmark]
    [BenchmarkCategory("Pair")]
    public void Write4Int32s_SIMD()
    {
        SimdBigEndian.Write4Int32sBigEndian(_writeBuffer, 123, 456, 789, 101112);
    }

    // === Pair Read Benchmarks ===

    [Benchmark]
    [BenchmarkCategory("Pair")]
    public (long, long) Read2Int64s_Scalar()
    {
        return (
            BinaryPrimitives.ReadInt64BigEndian(_readBuffer.AsSpan(0, 8)),
            BinaryPrimitives.ReadInt64BigEndian(_readBuffer.AsSpan(8, 8))
        );
    }

    [Benchmark]
    [BenchmarkCategory("Pair")]
    public (long, long) Read2Int64s_SIMD()
    {
        return SimdBigEndian.Read2Int64sBigEndian(_readBuffer);
    }

    [Benchmark]
    [BenchmarkCategory("Pair")]
    public (int, int, int, int) Read4Int32s_Scalar()
    {
        return (
            BinaryPrimitives.ReadInt32BigEndian(_readBuffer.AsSpan(0, 4)),
            BinaryPrimitives.ReadInt32BigEndian(_readBuffer.AsSpan(4, 4)),
            BinaryPrimitives.ReadInt32BigEndian(_readBuffer.AsSpan(8, 4)),
            BinaryPrimitives.ReadInt32BigEndian(_readBuffer.AsSpan(12, 4))
        );
    }

    [Benchmark]
    [BenchmarkCategory("Pair")]
    public (int, int, int, int) Read4Int32s_SIMD()
    {
        return SimdBigEndian.Read4Int32sBigEndian(_readBuffer);
    }
}
