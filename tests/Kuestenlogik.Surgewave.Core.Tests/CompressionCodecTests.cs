using System.Buffers;
using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Core.Util;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Core.Tests;

[Trait("Category", TestCategories.Unit)]
public class CompressionCodecTests
{
    private static readonly byte[] TestData = "Hello, Kafka! This is a test message for compression."u8.ToArray();

    private static byte[] CreateLargeTestData()
    {
        var data = new byte[10000];
        for (int i = 0; i < data.Length; i++)
            data[i] = (byte)(i % 256);
        return data;
    }

    #region Pooled Decompression

    [Theory]
    [InlineData(KafkaConstants.Compression.Gzip)]
    [InlineData(KafkaConstants.Compression.Snappy)]
    [InlineData(KafkaConstants.Compression.Lz4)]
    [InlineData(KafkaConstants.Compression.Zstd)]
    public void DecompressPooled_LargeData_RoundTrip(int compressionType)
    {
        var largeData = CreateLargeTestData();
        var compressed = CompressionCodec.Compress(largeData, compressionType);

        var (buffer, length, isPooled) = CompressionCodec.DecompressPooled(compressed, compressionType);
        try
        {
            Assert.True(isPooled);
            Assert.Equal(largeData.Length, length);
            Assert.True(buffer.AsSpan(0, length).SequenceEqual(largeData));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    [Fact]
    public void DecompressPooled_Lz4_HighlyCompressible_ForcesWriterGrowth()
    {
        // Zeros compress far better than the 3x initial-capacity guess, so the pooled writer
        // has to grow several times — exercises its rent/copy/return chain.
        var data = new byte[1024 * 1024];
        var compressed = CompressionCodec.Compress(data, KafkaConstants.Compression.Lz4);

        var (buffer, length, isPooled) = CompressionCodec.DecompressPooled(compressed, KafkaConstants.Compression.Lz4);
        try
        {
            Assert.True(isPooled);
            Assert.Equal(data.Length, length);
            Assert.True(buffer.AsSpan(0, length).SequenceEqual(data));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    [Fact]
    public void DecompressPooled_Gzip_MultiMember_GrowsBeyondTrailerHint()
    {
        // The ISIZE trailer only describes the LAST member, so a concatenated stream makes the
        // size hint an underestimate and forces the writer to grow past it.
        var first = new byte[256 * 1024];
        Random.Shared.NextBytes(first);
        var second = new byte[4096];
        Random.Shared.NextBytes(second);

        var concatenated = new MemoryStream();
        concatenated.Write(CompressionCodec.Compress(first, KafkaConstants.Compression.Gzip));
        concatenated.Write(CompressionCodec.Compress(second, KafkaConstants.Compression.Gzip));

        var expected = new byte[first.Length + second.Length];
        first.CopyTo(expected, 0);
        second.CopyTo(expected, first.Length);

        var (buffer, length, isPooled) = CompressionCodec.DecompressPooled(
            concatenated.ToArray(), KafkaConstants.Compression.Gzip);
        try
        {
            Assert.True(isPooled);
            Assert.Equal(expected.Length, length);
            Assert.True(buffer.AsSpan(0, length).SequenceEqual(expected));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    [Fact]
    public void GetGzipSizeHint_HonestTrailer_IsExactSize()
    {
        var data = new byte[8192];
        Random.Shared.NextBytes(data);
        var compressed = CompressionCodec.Compress(data, KafkaConstants.Compression.Gzip);

        Assert.Equal(data.Length, CompressionCodec.GetGzipSizeHint(compressed));
    }

    [Fact]
    public void GetGzipSizeHint_ForgedTrailer_IsCappedByMaxDeflateRatio()
    {
        // The trailer is attacker-supplied on the produce path. A tiny frame claiming ~4 GiB must
        // not become a ~4 GiB rent: the hint is capped by what deflate could physically expand to.
        var data = new byte[64];
        var compressed = CompressionCodec.Compress(data, KafkaConstants.Compression.Gzip);
        BinaryPrimitives.WriteUInt32LittleEndian(compressed.AsSpan(compressed.Length - 4), uint.MaxValue);

        var hint = CompressionCodec.GetGzipSizeHint(compressed);

        Assert.True(hint <= compressed.Length * 1032,
            $"Hint {hint} must stay within the max deflate ratio for a {compressed.Length}-byte frame");
        Assert.True(hint < 1 << 26);
    }

    [Fact]
    public void DecompressPooled_Gzip_ForgedTrailer_RejectsFrameWithoutLeaking()
    {
        // GZipStream validates CRC32 and ISIZE, so a forged trailer is rejected outright. The
        // pooled writer must hand its rent back on that path rather than leak it.
        var data = new byte[8192];
        Random.Shared.NextBytes(data);
        var compressed = CompressionCodec.Compress(data, KafkaConstants.Compression.Gzip);
        BinaryPrimitives.WriteUInt32LittleEndian(compressed.AsSpan(compressed.Length - 4), uint.MaxValue);

        Assert.Throws<InvalidDataException>(() =>
            CompressionCodec.DecompressPooled(compressed, KafkaConstants.Compression.Gzip));
    }

    #endregion

    #region GZIP Tests

    [Fact]
    public void Gzip_CompressAndDecompress_RoundTrip()
    {
        var compressed = CompressionCodec.Compress(TestData, KafkaConstants.Compression.Gzip);
        var decompressed = CompressionCodec.Decompress(compressed, KafkaConstants.Compression.Gzip);

        Assert.Equal(TestData, decompressed);
        Assert.NotEmpty(compressed);
    }

    [Fact]
    public void Gzip_LargeData_RoundTrip()
    {
        var largeData = CreateLargeTestData();

        var compressed = CompressionCodec.Compress(largeData, KafkaConstants.Compression.Gzip);
        var decompressed = CompressionCodec.Decompress(compressed, KafkaConstants.Compression.Gzip);

        Assert.Equal(largeData, decompressed);
    }

    #endregion

    #region Snappy Tests

    [Fact]
    public void Snappy_CompressAndDecompress_RoundTrip()
    {
        var compressed = CompressionCodec.Compress(TestData, KafkaConstants.Compression.Snappy);
        var decompressed = CompressionCodec.Decompress(compressed, KafkaConstants.Compression.Snappy);

        Assert.Equal(TestData, decompressed);
    }

    [Fact]
    public void Snappy_LargeData_RoundTrip()
    {
        var largeData = CreateLargeTestData();

        var compressed = CompressionCodec.Compress(largeData, KafkaConstants.Compression.Snappy);
        var decompressed = CompressionCodec.Decompress(compressed, KafkaConstants.Compression.Snappy);

        Assert.Equal(largeData, decompressed);
        Assert.True(compressed.Length < largeData.Length);
    }

    #endregion

    #region LZ4 Tests

    [Fact]
    public void Lz4_CompressAndDecompress_RoundTrip()
    {
        var compressed = CompressionCodec.Compress(TestData, KafkaConstants.Compression.Lz4);
        var decompressed = CompressionCodec.Decompress(compressed, KafkaConstants.Compression.Lz4);

        Assert.Equal(TestData, decompressed);
    }

    [Fact]
    public void Lz4_LargeData_RoundTrip()
    {
        var largeData = CreateLargeTestData();

        var compressed = CompressionCodec.Compress(largeData, KafkaConstants.Compression.Lz4);
        var decompressed = CompressionCodec.Decompress(compressed, KafkaConstants.Compression.Lz4);

        Assert.Equal(largeData, decompressed);
        Assert.True(compressed.Length < largeData.Length);
    }

    #endregion

    #region ZSTD Tests

    [Fact]
    public void Zstd_CompressAndDecompress_RoundTrip()
    {
        var compressed = CompressionCodec.Compress(TestData, KafkaConstants.Compression.Zstd);
        var decompressed = CompressionCodec.Decompress(compressed, KafkaConstants.Compression.Zstd);

        Assert.Equal(TestData, decompressed);
    }

    [Fact]
    public void Zstd_LargeData_RoundTrip()
    {
        var largeData = CreateLargeTestData();

        var compressed = CompressionCodec.Compress(largeData, KafkaConstants.Compression.Zstd);
        var decompressed = CompressionCodec.Decompress(compressed, KafkaConstants.Compression.Zstd);

        Assert.Equal(largeData, decompressed);
        Assert.True(compressed.Length < largeData.Length);
    }

    #endregion

    #region None Tests

    [Fact]
    public void None_PassthroughData_Unchanged()
    {
        var originalData = new byte[] { 1, 2, 3, 4, 5 };

        var compressed = CompressionCodec.Compress(originalData, KafkaConstants.Compression.None);
        var decompressed = CompressionCodec.Decompress(compressed, KafkaConstants.Compression.None);

        Assert.Same(originalData, compressed);
        Assert.Same(compressed, decompressed);
    }

    #endregion

    #region Utility Tests

    [Fact]
    public void IsSupported_ReturnsCorrectValues()
    {
        Assert.True(CompressionCodec.IsSupported(KafkaConstants.Compression.None));
        Assert.True(CompressionCodec.IsSupported(KafkaConstants.Compression.Gzip));
        Assert.True(CompressionCodec.IsSupported(KafkaConstants.Compression.Snappy));
        Assert.True(CompressionCodec.IsSupported(KafkaConstants.Compression.Lz4));
        Assert.True(CompressionCodec.IsSupported(KafkaConstants.Compression.Zstd));
    }

    [Fact]
    public void GetCompressionName_ReturnsCorrectNames()
    {
        Assert.Equal("None", CompressionCodec.GetCompressionName(KafkaConstants.Compression.None));
        Assert.Equal("GZIP", CompressionCodec.GetCompressionName(KafkaConstants.Compression.Gzip));
        Assert.Equal("Snappy", CompressionCodec.GetCompressionName(KafkaConstants.Compression.Snappy));
        Assert.Equal("LZ4", CompressionCodec.GetCompressionName(KafkaConstants.Compression.Lz4));
        Assert.Equal("ZSTD", CompressionCodec.GetCompressionName(KafkaConstants.Compression.Zstd));
        Assert.Equal("Unknown(99)", CompressionCodec.GetCompressionName(99));
    }

    [Fact]
    public void GetCompressionTypeFromBatch_ExtractsCorrectType()
    {
        var batch = new byte[23];
        batch[21] = 0;
        batch[22] = KafkaConstants.Compression.Lz4;

        var compressionType = CompressionCodec.GetCompressionTypeFromBatch(batch);
        Assert.Equal(KafkaConstants.Compression.Lz4, compressionType);
    }

    [Fact]
    public void GetCompressionTypeFromBatch_TooSmall_ReturnsNone()
    {
        var batch = new byte[10];
        var compressionType = CompressionCodec.GetCompressionTypeFromBatch(batch);
        Assert.Equal(KafkaConstants.Compression.None, compressionType);
    }

    #endregion
}
