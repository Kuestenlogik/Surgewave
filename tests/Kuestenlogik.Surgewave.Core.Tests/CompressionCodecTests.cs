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
