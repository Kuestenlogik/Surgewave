using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics.CodeAnalysis;
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
    /// Compresses data directly into a single pooled buffer with the 4-byte big-endian
    /// original-size header prepended: [originalSize:4][compressedData].
    /// </summary>
    /// <returns>
    /// <c>true</c> when compression wins: the frame occupies <c>pooledBuffer[0..frameLength)</c> and
    /// the CALLER owns the rent — it must return <paramref name="pooledBuffer"/> to
    /// <see cref="ArrayPool{T}.Shared"/> once the bytes are on the wire.
    /// <c>false</c> when the payload is too small or incompressible: nothing is rented,
    /// <paramref name="pooledBuffer"/> is null, and the caller sends the original payload
    /// unchanged — no copy at all.
    /// </returns>
    public static bool TryCompressWithHeader(
        ReadOnlySpan<byte> data,
        [NotNullWhen(true)] out byte[]? pooledBuffer,
        out int frameLength)
    {
        pooledBuffer = null;
        frameLength = 0;

        if (data.Length < MinCompressionSize)
        {
            return false;
        }

        var rented = ArrayPool<byte>.Shared.Rent(4 + LZ4Codec.MaximumOutputSize(data.Length));
        var compressedSize = LZ4Codec.Encode(data, rented.AsSpan(4), CompressionLevel);

        // Reject unless the framed result is strictly smaller than the original payload —
        // the 4-byte header is part of what goes on the wire, so it counts here.
        if (compressedSize <= 0 || compressedSize + 4 >= data.Length)
        {
            ArrayPool<byte>.Shared.Return(rented);
            return false;
        }

        BinaryPrimitives.WriteInt32BigEndian(rented, data.Length);
        pooledBuffer = rented;
        frameLength = 4 + compressedSize;
        return true;
    }
}
