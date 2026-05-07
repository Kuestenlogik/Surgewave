using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Core.Util;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Core.Tests;

/// <summary>
/// Unit tests for SIMD-optimized big-endian operations.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class SimdBigEndianTests
{
    private readonly ITestOutputHelper _output;

    public SimdBigEndianTests(ITestOutputHelper output)
    {
        _output = output;
        _output.WriteLine($"Implementation: {SimdBigEndian.Implementation}");
        _output.WriteLine($"Hardware Accelerated: {SimdBigEndian.IsHardwareAccelerated}");
    }

    private static readonly string[] ValidImplementations = ["AVX2", "SSSE3", "Scalar"];

    [Fact]
    public void Implementation_ReportsValidImplementation()
    {
        Assert.Contains(SimdBigEndian.Implementation, ValidImplementations);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(4)]
    [InlineData(8)]
    [InlineData(16)]
    public void WriteInt64sBigEndian_MatchesScalar(int count)
    {
        var values = new long[count];
        var random = new Random(42);
        for (int i = 0; i < count; i++)
            values[i] = random.NextInt64();

        var simdBuffer = new byte[count * 8];
        var scalarBuffer = new byte[count * 8];

        SimdBigEndian.WriteInt64sBigEndian(simdBuffer, values);

        for (int i = 0; i < count; i++)
            BinaryPrimitives.WriteInt64BigEndian(scalarBuffer.AsSpan(i * 8, 8), values[i]);

        Assert.Equal(scalarBuffer, simdBuffer);
    }

    [Fact]
    public void WriteInt64sBigEndian_EmptySpan_NoException()
    {
        var buffer = Array.Empty<byte>();
        SimdBigEndian.WriteInt64sBigEndian(buffer, ReadOnlySpan<long>.Empty);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    [InlineData(8)]
    [InlineData(16)]
    public void WriteInt32sBigEndian_MatchesScalar(int count)
    {
        var values = new int[count];
        var random = new Random(42);
        for (int i = 0; i < count; i++)
            values[i] = random.Next();

        var simdBuffer = new byte[count * 4];
        var scalarBuffer = new byte[count * 4];

        SimdBigEndian.WriteInt32sBigEndian(simdBuffer, values);

        for (int i = 0; i < count; i++)
            BinaryPrimitives.WriteInt32BigEndian(scalarBuffer.AsSpan(i * 4, 4), values[i]);

        Assert.Equal(scalarBuffer, simdBuffer);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    [InlineData(8)]
    [InlineData(16)]
    public void ReadInt64sBigEndian_MatchesScalar(int count)
    {
        var originalValues = new long[count];
        var random = new Random(42);
        for (int i = 0; i < count; i++)
            originalValues[i] = random.NextInt64();

        var buffer = new byte[count * 8];
        for (int i = 0; i < count; i++)
            BinaryPrimitives.WriteInt64BigEndian(buffer.AsSpan(i * 8, 8), originalValues[i]);

        var simdValues = new long[count];
        SimdBigEndian.ReadInt64sBigEndian(buffer, simdValues);

        Assert.Equal(originalValues, simdValues);
    }

    [Fact]
    public void ReadInt64sBigEndian_RoundTrip()
    {
        var original = new long[] { long.MinValue, -1, 0, 1, long.MaxValue };
        var buffer = new byte[original.Length * 8];

        SimdBigEndian.WriteInt64sBigEndian(buffer, original);

        var result = new long[original.Length];
        SimdBigEndian.ReadInt64sBigEndian(buffer, result);

        Assert.Equal(original, result);
    }

    [Fact]
    public void Write2Int64sBigEndian_MatchesScalar()
    {
        const long value1 = 0x0102030405060708L;
        const long value2 = 0x1112131415161718L;

        var simdBuffer = new byte[16];
        var scalarBuffer = new byte[16];

        SimdBigEndian.Write2Int64sBigEndian(simdBuffer, value1, value2);

        BinaryPrimitives.WriteInt64BigEndian(scalarBuffer.AsSpan(0, 8), value1);
        BinaryPrimitives.WriteInt64BigEndian(scalarBuffer.AsSpan(8, 8), value2);

        Assert.Equal(scalarBuffer, simdBuffer);
    }

    [Fact]
    public void Write4Int32sBigEndian_MatchesScalar()
    {
        const int value1 = 0x01020304;
        const int value2 = 0x11121314;
        const int value3 = 0x21222324;
        const int value4 = 0x31323334;

        var simdBuffer = new byte[16];
        var scalarBuffer = new byte[16];

        SimdBigEndian.Write4Int32sBigEndian(simdBuffer, value1, value2, value3, value4);

        BinaryPrimitives.WriteInt32BigEndian(scalarBuffer.AsSpan(0, 4), value1);
        BinaryPrimitives.WriteInt32BigEndian(scalarBuffer.AsSpan(4, 4), value2);
        BinaryPrimitives.WriteInt32BigEndian(scalarBuffer.AsSpan(8, 4), value3);
        BinaryPrimitives.WriteInt32BigEndian(scalarBuffer.AsSpan(12, 4), value4);

        Assert.Equal(scalarBuffer, simdBuffer);
    }

    [Fact]
    public void Read2Int64sBigEndian_MatchesScalar()
    {
        const long value1 = 0x0102030405060708L;
        const long value2 = 0x1112131415161718L;

        var buffer = new byte[16];
        BinaryPrimitives.WriteInt64BigEndian(buffer.AsSpan(0, 8), value1);
        BinaryPrimitives.WriteInt64BigEndian(buffer.AsSpan(8, 8), value2);

        var (read1, read2) = SimdBigEndian.Read2Int64sBigEndian(buffer);

        Assert.Equal(value1, read1);
        Assert.Equal(value2, read2);
    }

    [Fact]
    public void Read4Int32sBigEndian_MatchesScalar()
    {
        const int value1 = 0x01020304;
        const int value2 = 0x11121314;
        const int value3 = 0x21222324;
        const int value4 = 0x31323334;

        var buffer = new byte[16];
        BinaryPrimitives.WriteInt32BigEndian(buffer.AsSpan(0, 4), value1);
        BinaryPrimitives.WriteInt32BigEndian(buffer.AsSpan(4, 4), value2);
        BinaryPrimitives.WriteInt32BigEndian(buffer.AsSpan(8, 4), value3);
        BinaryPrimitives.WriteInt32BigEndian(buffer.AsSpan(12, 4), value4);

        var (read1, read2, read3, read4) = SimdBigEndian.Read4Int32sBigEndian(buffer);

        Assert.Equal(value1, read1);
        Assert.Equal(value2, read2);
        Assert.Equal(value3, read3);
        Assert.Equal(value4, read4);
    }

    [Fact]
    public void WriteReadInt64s_LargeDataSet()
    {
        const int count = 1024;
        var values = new long[count];
        var random = new Random(42);
        for (int i = 0; i < count; i++)
            values[i] = random.NextInt64();

        var buffer = new byte[count * 8];
        SimdBigEndian.WriteInt64sBigEndian(buffer, values);

        var result = new long[count];
        SimdBigEndian.ReadInt64sBigEndian(buffer, result);

        Assert.Equal(values, result);
    }
}
