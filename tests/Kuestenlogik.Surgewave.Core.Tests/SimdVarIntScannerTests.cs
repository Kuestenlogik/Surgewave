using Kuestenlogik.Surgewave.Core.Util;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Core.Tests;

/// <summary>
/// Unit tests for SIMD-optimized VarInt scanning operations.
/// Verifies correctness of VarInt reading, skipping, counting, and record scanning.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class SimdVarIntScannerTests
{
    private static readonly string[] ValidImplementations = ["AVX2", "SSE2", "Scalar"];
    private readonly ITestOutputHelper _output;

    public SimdVarIntScannerTests(ITestOutputHelper output)
    {
        _output = output;
        _output.WriteLine($"SIMD Implementation: {SimdVarIntScanner.Implementation}");
    }

    [Fact]
    public void Implementation_ReportsCorrectly()
    {
        var impl = SimdVarIntScanner.Implementation;
        Assert.NotNull(impl);
        Assert.NotEmpty(impl);
        Assert.Contains(impl, ValidImplementations);
        _output.WriteLine($"Active implementation: {impl}");
    }

    // === SkipVarInt Tests ===

    [Fact]
    public void SkipVarInt_EmptyBuffer_ReturnsZero()
    {
        Assert.Equal(0, SimdVarIntScanner.SkipVarInt(ReadOnlySpan<byte>.Empty));
    }

    [Theory]
    [InlineData(new byte[] { 0x00 }, 1)]
    [InlineData(new byte[] { 0x7F }, 1)]
    [InlineData(new byte[] { 0x80, 0x01 }, 2)]
    [InlineData(new byte[] { 0xFF, 0x7F }, 2)]
    [InlineData(new byte[] { 0x80, 0x80, 0x01 }, 3)]
    [InlineData(new byte[] { 0xFF, 0xFF, 0x7F }, 3)]
    [InlineData(new byte[] { 0x80, 0x80, 0x80, 0x01 }, 4)]
    [InlineData(new byte[] { 0x80, 0x80, 0x80, 0x80, 0x01 }, 5)]
    public void SkipVarInt_ReturnsCorrectLength(byte[] buffer, int expectedLength)
    {
        var result = SimdVarIntScanner.SkipVarInt(buffer);
        Assert.Equal(expectedLength, result);
    }

    [Fact]
    public void SkipVarInt_SingleByteValues_AllCorrect()
    {
        for (int i = 0; i < 128; i++)
        {
            var buffer = new byte[] { (byte)i };
            Assert.Equal(1, SimdVarIntScanner.SkipVarInt(buffer));
        }
    }

    // === ReadVarIntFast Tests ===

    [Fact]
    public void ReadVarIntFast_EmptyBuffer_ReturnsZeroLength()
    {
        var (value, length) = SimdVarIntScanner.ReadVarIntFast(ReadOnlySpan<byte>.Empty);
        Assert.Equal(0, value);
        Assert.Equal(0, length);
    }

    [Theory]
    [InlineData(new byte[] { 0x00 }, 0, 1)]
    [InlineData(new byte[] { 0x01 }, 1, 1)]
    [InlineData(new byte[] { 0x7F }, 127, 1)]
    [InlineData(new byte[] { 0x80, 0x01 }, 128, 2)]
    [InlineData(new byte[] { 0xAC, 0x02 }, 300, 2)]
    [InlineData(new byte[] { 0x80, 0x80, 0x01 }, 16384, 3)]
    public void ReadVarIntFast_ReturnsCorrectValue(byte[] buffer, int expectedValue, int expectedLength)
    {
        var (value, length) = SimdVarIntScanner.ReadVarIntFast(buffer);
        Assert.Equal(expectedValue, value);
        Assert.Equal(expectedLength, length);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(127)]
    [InlineData(128)]
    [InlineData(16383)]
    [InlineData(16384)]
    [InlineData(2097151)]
    [InlineData(2097152)]
    public void ReadVarIntFast_MatchesKafkaProtocolPrimitives(int value)
    {
        var buffer = new byte[5];
        var bytesWritten = KafkaProtocolPrimitives.WriteVarInt(buffer, value);

        var (simdValue, simdLength) = SimdVarIntScanner.ReadVarIntFast(buffer);
        var (scalarValue, scalarLength) = KafkaProtocolPrimitives.ReadVarInt(buffer);

        Assert.Equal(scalarValue, simdValue);
        Assert.Equal(scalarLength, simdLength);
        Assert.Equal(bytesWritten, simdLength);
    }

    // === FindVarIntTerminators Tests ===

    [Fact]
    public void FindVarIntTerminators16_AllTerminators_AllBitsSet()
    {
        // All bytes have bit 7 = 0, so all are terminators
        var buffer = new byte[16];
        for (int i = 0; i < 16; i++)
        {
            buffer[i] = (byte)(i % 128); // 0-127, all have bit 7 = 0
        }

        var result = SimdVarIntScanner.FindVarIntTerminators16(buffer);
        Assert.Equal(0xFFFFu, result);
    }

    [Fact]
    public void FindVarIntTerminators16_NoTerminators_NoBitsSet()
    {
        // All bytes have bit 7 = 1, so none are terminators
        var buffer = new byte[16];
        for (int i = 0; i < 16; i++)
        {
            buffer[i] = (byte)(0x80 | (i % 128));
        }

        var result = SimdVarIntScanner.FindVarIntTerminators16(buffer);
        Assert.Equal(0u, result);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(5)]
    [InlineData(15)]
    public void FindVarIntTerminators16_SingleTerminator_CorrectBit(int position)
    {
        var buffer = new byte[16];
        for (int i = 0; i < 16; i++)
        {
            buffer[i] = 0x80; // All continuation bytes
        }
        buffer[position] = 0x01; // Terminator at position

        var result = SimdVarIntScanner.FindVarIntTerminators16(buffer);
        Assert.Equal(1u << position, result);
    }

    [Fact]
    public void FindVarIntTerminators32_AllTerminators_AllBitsSet()
    {
        var buffer = new byte[32];
        for (int i = 0; i < 32; i++)
        {
            buffer[i] = (byte)(i % 128);
        }

        var result = SimdVarIntScanner.FindVarIntTerminators32(buffer);
        Assert.Equal(0xFFFFFFFFu, result);
    }

    [Fact]
    public void FindVarIntTerminators32_NoTerminators_NoBitsSet()
    {
        var buffer = new byte[32];
        for (int i = 0; i < 32; i++)
        {
            buffer[i] = (byte)(0x80 | (i % 128));
        }

        var result = SimdVarIntScanner.FindVarIntTerminators32(buffer);
        Assert.Equal(0u, result);
    }

    // === BatchReadVarInts Tests ===

    [Fact]
    public void BatchReadVarInts_EmptyBuffer_ReturnsZero()
    {
        var values = new int[10];
        var count = SimdVarIntScanner.BatchReadVarInts(ReadOnlySpan<byte>.Empty, values, out int bytesConsumed);

        Assert.Equal(0, count);
        Assert.Equal(0, bytesConsumed);
    }

    [Fact]
    public void BatchReadVarInts_SingleByteVarInts_ReadsAll()
    {
        var buffer = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        var values = new int[10];

        var count = SimdVarIntScanner.BatchReadVarInts(buffer, values, out int bytesConsumed);

        Assert.Equal(10, count);
        Assert.Equal(10, bytesConsumed);
        for (int i = 0; i < 10; i++)
        {
            Assert.Equal(i, values[i]);
        }
    }

    [Fact]
    public void BatchReadVarInts_MixedSizeVarInts_ReadsCorrectly()
    {
        // Create buffer with mixed VarInt sizes
        using var ms = new MemoryStream();
        var testValues = new[] { 0, 127, 128, 300, 16384, 100000 };
        var writeBuffer = new byte[5];

        foreach (var val in testValues)
        {
            var len = KafkaProtocolPrimitives.WriteVarInt(writeBuffer, val);
            ms.Write(writeBuffer, 0, len);
        }

        var buffer = ms.ToArray();
        var values = new int[testValues.Length];

        var count = SimdVarIntScanner.BatchReadVarInts(buffer, values, out int bytesConsumed);

        Assert.Equal(testValues.Length, count);
        Assert.Equal(buffer.Length, bytesConsumed);
        for (int i = 0; i < testValues.Length; i++)
        {
            Assert.Equal(testValues[i], values[i]);
        }
    }

    [Fact]
    public void BatchReadVarInts_LimitedOutputBuffer_ReadsUpToLimit()
    {
        var buffer = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        var values = new int[5]; // Only space for 5 values

        var count = SimdVarIntScanner.BatchReadVarInts(buffer, values, out int bytesConsumed);

        Assert.Equal(5, count);
        Assert.Equal(5, bytesConsumed);
    }

    // === CountVarInts Tests ===

    [Fact]
    public void CountVarInts_EmptyBuffer_ReturnsZero()
    {
        Assert.Equal(0, SimdVarIntScanner.CountVarInts(ReadOnlySpan<byte>.Empty));
    }

    [Fact]
    public void CountVarInts_SingleByteVarInts_CountsAll()
    {
        // Buffer of single-byte VarInts (all have bit 7 = 0)
        var buffer = new byte[100];
        for (int i = 0; i < 100; i++)
        {
            buffer[i] = (byte)(i % 128);
        }

        var count = SimdVarIntScanner.CountVarInts(buffer);
        Assert.Equal(100, count);
    }

    [Fact]
    public void CountVarInts_TwoByteVarInts_CountsCorrectly()
    {
        // Each VarInt is 2 bytes: 0x80 0x01
        var buffer = new byte[200];
        for (int i = 0; i < 100; i++)
        {
            buffer[i * 2] = 0x80;
            buffer[i * 2 + 1] = 0x01;
        }

        var count = SimdVarIntScanner.CountVarInts(buffer);
        Assert.Equal(100, count); // 100 terminators (one per VarInt)
    }

    [Fact]
    public void CountVarInts_MixedSizes_MatchesManualCount()
    {
        using var ms = new MemoryStream();
        var random = new Random(42);
        var writeBuffer = new byte[5];
        int expectedCount = 0;

        for (int i = 0; i < 100; i++)
        {
            int value = random.Next(0, 1000000);
            var len = KafkaProtocolPrimitives.WriteVarInt(writeBuffer, value);
            ms.Write(writeBuffer, 0, len);
            expectedCount++;
        }

        var buffer = ms.ToArray();
        var count = SimdVarIntScanner.CountVarInts(buffer);

        Assert.Equal(expectedCount, count);
    }

    // === ScanRecordOffsets Tests ===

    [Fact]
    public void ScanRecordOffsets_EmptyBuffer_ReturnsZero()
    {
        var offsets = new int[10];
        var found = SimdVarIntScanner.ScanRecordOffsets(ReadOnlySpan<byte>.Empty, offsets, 10);
        Assert.Equal(0, found);
    }

    [Fact]
    public void ScanRecordOffsets_SimpleRecords_FindsAllOffsets()
    {
        // Create simple records with zigzag-encoded lengths
        using var ms = new MemoryStream();
        var expectedOffsets = new List<int>();
        var recordBuffer = new byte[20];

        for (int i = 0; i < 5; i++)
        {
            expectedOffsets.Add((int)ms.Position);

            // Record length (zigzag encoded) - let's say each record is 10 bytes
            int recordLength = 10;
            int zigzag = (recordLength << 1) ^ (recordLength >> 31);
            var lenBytes = KafkaProtocolPrimitives.WriteVarInt(recordBuffer, zigzag);
            ms.Write(recordBuffer, 0, lenBytes);

            // Record content
            ms.Write(new byte[recordLength], 0, recordLength);
        }

        var buffer = ms.ToArray();
        var offsets = new int[10];

        var found = SimdVarIntScanner.ScanRecordOffsets(buffer, offsets, 5);

        Assert.Equal(5, found);
        for (int i = 0; i < expectedOffsets.Count; i++)
        {
            Assert.Equal(expectedOffsets[i], offsets[i]);
        }
    }

    [Fact]
    public void ScanRecordOffsetsSimd_MatchesScalarImplementation()
    {
        // Create realistic record data
        using var ms = new MemoryStream();
        var recordBuffer = new byte[20];

        for (int i = 0; i < 50; i++)
        {
            int recordLength = 10 + (i % 20);
            int zigzag = (recordLength << 1) ^ (recordLength >> 31);
            var lenBytes = KafkaProtocolPrimitives.WriteVarInt(recordBuffer, zigzag);
            ms.Write(recordBuffer, 0, lenBytes);
            ms.Write(new byte[recordLength], 0, recordLength);
        }

        var buffer = ms.ToArray();
        var scalarOffsets = new int[50];
        var simdOffsets = new int[50];

        var scalarFound = SimdVarIntScanner.ScanRecordOffsets(buffer, scalarOffsets, 50);
        var simdFound = SimdVarIntScanner.ScanRecordOffsetsSimd(buffer, simdOffsets, 50);

        Assert.Equal(scalarFound, simdFound);
        for (int i = 0; i < scalarFound; i++)
        {
            Assert.Equal(scalarOffsets[i], simdOffsets[i]);
        }
    }

    // === Cross-validation with scalar implementation ===

    [Fact]
    public void ReadVarIntFast_LargeRandomValues_MatchesScalar()
    {
        var random = new Random(42);
        var buffer = new byte[5];

        for (int i = 0; i < 1000; i++)
        {
            int value = random.Next(0, int.MaxValue / 2);
            KafkaProtocolPrimitives.WriteVarInt(buffer, value);

            var (simdValue, simdLen) = SimdVarIntScanner.ReadVarIntFast(buffer);
            var (scalarValue, scalarLen) = KafkaProtocolPrimitives.ReadVarInt(buffer);

            Assert.Equal(scalarValue, simdValue);
            Assert.Equal(scalarLen, simdLen);
        }
    }

    [Fact]
    public void SkipVarInt_MatchesReadVarIntLength()
    {
        var random = new Random(42);
        var buffer = new byte[5];

        for (int i = 0; i < 1000; i++)
        {
            int value = random.Next(0, int.MaxValue / 2);
            KafkaProtocolPrimitives.WriteVarInt(buffer, value);

            var skipLen = SimdVarIntScanner.SkipVarInt(buffer);
            var (_, readLen) = SimdVarIntScanner.ReadVarIntFast(buffer);

            Assert.Equal(readLen, skipLen);
        }
    }
}
