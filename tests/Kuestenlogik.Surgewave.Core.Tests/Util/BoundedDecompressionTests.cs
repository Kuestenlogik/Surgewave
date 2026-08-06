using System.IO.Compression;
using Kuestenlogik.Surgewave.Core;
using Kuestenlogik.Surgewave.Core.Util;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Core.Tests.Util;

/// <summary>
/// Decompressing producer-supplied data has to have a ceiling (#134).
///
/// <para>A few kilobytes on the wire can become hundreds of megabytes in the broker: ratios past
/// 1000:1 are trivial to construct from repetitive input. All four codecs expanded without any
/// bound, and two of them — zstd and snappy — sized their allocation from a number written into the
/// frame by the sender.</para>
///
/// <para>These tests build actual bombs rather than asserting on configuration. The compressed
/// input is a few hundred bytes; what matters is that the refusal happens without materialising the
/// expansion.</para>
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class BoundedDecompressionTests
{
    // 8 MiB of zeros compresses to a few hundred bytes in every codec here.
    private const int BombSize = 8 * 1024 * 1024;
    private const long Budget = 64 * 1024;

    [Theory]
    [InlineData(KafkaConstants.Compression.Gzip)]
    [InlineData(KafkaConstants.Compression.Snappy)]
    [InlineData(KafkaConstants.Compression.Lz4)]
    [InlineData(KafkaConstants.Compression.Zstd)]
    public void ABombIsRefused(int compressionType)
    {
        var bomb = CompressionCodec.Compress(new byte[BombSize], compressionType);

        // Guard against a vacuous test: the input must actually amplify. The achievable ratio
        // differs sharply per codec — gzip, lz4 and zstd reach thousands to one on this input,
        // snappy only about twenty to one because its match window is 64 KiB — so the guard is a
        // floor on amplification, not a fixed compressed size.
        Assert.True(bomb.Length * 8L < BombSize,
            $"the test input is not a bomb: {bomb.Length} compressed bytes for {BombSize} uncompressed");

        var accepted = CompressionCodec.TryDecompressBounded(
            bomb, compressionType, Budget, out var buffer, out var length, out var isPooled);

        try
        {
            Assert.False(accepted, "an 8 MiB expansion was accepted against a 64 KiB budget");
            Assert.Equal(0, length);
        }
        finally
        {
            CompressionCodec.Release(buffer, isPooled);
        }
    }

    [Theory]
    [InlineData(KafkaConstants.Compression.Gzip)]
    [InlineData(KafkaConstants.Compression.Snappy)]
    [InlineData(KafkaConstants.Compression.Lz4)]
    [InlineData(KafkaConstants.Compression.Zstd)]
    public void DataThatFits_IsStillDecompressed(int compressionType)
    {
        // The bound must not turn into "reject compressed data": ordinary payloads go through
        // unchanged, byte for byte.
        var payload = new byte[4096];
        for (var i = 0; i < payload.Length; i++)
            payload[i] = (byte)(i % 251);

        var compressed = CompressionCodec.Compress(payload, compressionType);

        var accepted = CompressionCodec.TryDecompressBounded(
            compressed, compressionType, Budget, out var buffer, out var length, out var isPooled);

        try
        {
            Assert.True(accepted);
            Assert.Equal(payload.Length, length);
            Assert.True(payload.AsSpan().SequenceEqual(buffer.AsSpan(0, length)));
        }
        finally
        {
            CompressionCodec.Release(buffer, isPooled);
        }
    }

    [Theory]
    [InlineData(KafkaConstants.Compression.Gzip)]
    [InlineData(KafkaConstants.Compression.Snappy)]
    [InlineData(KafkaConstants.Compression.Lz4)]
    [InlineData(KafkaConstants.Compression.Zstd)]
    public void ExactlyAtTheBudget_IsAccepted(int compressionType)
    {
        // Off-by-one in the wrong direction would reject legitimate maximum-size records.
        var payload = new byte[Budget];
        var compressed = CompressionCodec.Compress(payload, compressionType);

        var accepted = CompressionCodec.TryDecompressBounded(
            compressed, compressionType, Budget, out var buffer, out var length, out var isPooled);

        try
        {
            Assert.True(accepted);
            Assert.Equal(Budget, length);
        }
        finally
        {
            CompressionCodec.Release(buffer, isPooled);
        }
    }

    [Theory]
    [InlineData(KafkaConstants.Compression.Snappy)]
    [InlineData(KafkaConstants.Compression.Zstd)]
    public void ADeclaredOversizeIsRefused_WithoutAllocatingTheExpansion(int compressionType)
    {
        // The property that actually protects the broker. Refusing AFTER expanding is no defence:
        // the memory has already been taken, which is the entire attack. snappy and zstd write the
        // uncompressed size into the frame, so the refusal can and must happen before a byte is
        // rented — and only an allocation measurement can tell the two apart, because the returned
        // verdict is "refused" either way.
        var bomb = CompressionCodec.Compress(new byte[BombSize], compressionType);

        // Warm up on a SMALL payload. Warming up on the bomb itself would defeat the measurement:
        // the rejected path rents from ArrayPool, and a pool that already holds an 8 MiB buffer
        // serves the next rent without allocating anything the GC counter would see.
        var small = CompressionCodec.Compress(new byte[1024], compressionType);
        CompressionCodec.TryDecompressBounded(small, compressionType, Budget, out var warm, out _, out var warmPooled);
        CompressionCodec.Release(warm, warmPooled);

        var before = GC.GetTotalAllocatedBytes(precise: true);
        var accepted = CompressionCodec.TryDecompressBounded(
            bomb, compressionType, Budget, out var buffer, out _, out var isPooled);
        var allocated = GC.GetTotalAllocatedBytes(precise: true) - before;

        CompressionCodec.Release(buffer, isPooled);

        Assert.False(accepted);
        Assert.True(allocated < BombSize / 8,
            $"refusing the frame allocated {allocated} bytes — the expansion was materialised before being rejected");
    }

    [Fact]
    public void GarbageIsRefusedRatherThanThrowing()
    {
        // The caller is on the produce path and must not have to catch: a frame that cannot be
        // decoded is the same outcome as one that is too big.
        var garbage = new byte[512];
        Random.Shared.NextBytes(garbage);

        foreach (var codec in new[]
                 {
                     KafkaConstants.Compression.Gzip,
                     KafkaConstants.Compression.Lz4
                 })
        {
            var accepted = CompressionCodec.TryDecompressBounded(
                garbage, codec, Budget, out var buffer, out _, out var isPooled);

            CompressionCodec.Release(buffer, isPooled);
            Assert.False(accepted);
        }
    }

    [Fact]
    public void AZeroBudgetRefusesEverything()
    {
        var compressed = CompressionCodec.Compress(new byte[16], KafkaConstants.Compression.Gzip);

        Assert.False(CompressionCodec.TryDecompressBounded(
            compressed, KafkaConstants.Compression.Gzip, 0, out _, out _, out _));
    }

    [Fact]
    public void UncompressedDataIsBoundedToo()
    {
        // Compression type None still goes through the same gate — an oversized batch should not
        // slip past just because it never needed expanding.
        var payload = new byte[Budget + 1];

        Assert.False(CompressionCodec.TryDecompressBounded(
            payload, KafkaConstants.Compression.None, Budget, out _, out _, out _));
    }
}
