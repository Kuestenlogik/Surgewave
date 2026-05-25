using System.Buffers.Binary;
using K4os.Compression.LZ4;

namespace Kuestenlogik.Surgewave.Protocol.Native;

/// <summary>
/// Compression support for Surgewave native protocol.
/// Uses LZ4 for fast compression with good ratio - optimal for low-latency messaging.
/// </summary>
public static class NativeCompressionCodec
{
    /// <summary>
    /// Minimum payload size to consider for compression (1KB).
    /// Smaller payloads don't benefit from compression.
    /// </summary>
    public const int MinCompressionSize = 1024;

    /// <summary>
    /// Compression level for LZ4 (L00_FAST for best speed).
    /// </summary>
    private const LZ4Level CompressionLevel = LZ4Level.L00_FAST;

    /// <summary>
    /// Compress payload using LZ4.
    /// Returns original data if compression doesn't reduce size.
    /// </summary>
    public static (byte[] Data, bool WasCompressed) Compress(ReadOnlySpan<byte> data)
    {
        if (data.Length < MinCompressionSize)
        {
            return (data.ToArray(), false);
        }

        var maxCompressedSize = LZ4Codec.MaximumOutputSize(data.Length);
        var compressed = new byte[maxCompressedSize];
        var compressedSize = LZ4Codec.Encode(data, compressed, CompressionLevel);

        // Only use compression if it actually reduces size
        if (compressedSize >= data.Length)
        {
            return (data.ToArray(), false);
        }

        return (compressed[..compressedSize], true);
    }

    /// <summary>
    /// Decompress LZ4 payload.
    /// </summary>
    public static byte[] Decompress(ReadOnlySpan<byte> compressedData, int originalSize)
    {
        var decompressed = new byte[originalSize];
        var decodedSize = LZ4Codec.Decode(compressedData, decompressed);

        if (decodedSize != originalSize)
        {
            throw new InvalidOperationException(
                $"Decompression size mismatch: expected {originalSize}, got {decodedSize}");
        }

        return decompressed;
    }

    /// <summary>
    /// Try to decompress, handling the original size being prepended to the data.
    /// Format: [originalSize:4 bytes][compressedData]
    /// </summary>
    public static byte[] DecompressWithHeader(ReadOnlySpan<byte> data)
    {
        if (data.Length < 4)
        {
            throw new InvalidOperationException("Compressed data too small - missing size header");
        }

        var originalSize = BinaryPrimitives.ReadInt32BigEndian(data[..4]);
        return Decompress(data[4..], originalSize);
    }

    /// <summary>
    /// Compress with a size header for decompression.
    /// Format: [originalSize:4 bytes][compressedData]
    /// </summary>
    public static (byte[] Data, bool WasCompressed) CompressWithHeader(ReadOnlySpan<byte> data)
    {
        if (data.Length < MinCompressionSize)
        {
            return (data.ToArray(), false);
        }

        var (compressed, wasCompressed) = Compress(data);

        if (!wasCompressed)
        {
            // Compress already called data.ToArray() - reuse that allocation instead of copying again
            return (compressed, false);
        }

        // Prepend original size
        var result = new byte[4 + compressed.Length];
        BinaryPrimitives.WriteInt32BigEndian(result, data.Length);
        compressed.CopyTo(result.AsSpan(4));

        return (result, true);
    }
}
