using System.Buffers;
using System.Buffers.Binary;
using System.IO.Compression;
using K4os.Compression.LZ4;
using K4os.Compression.LZ4.Streams;
using Snappier;
using ZstdSharp;

namespace Kuestenlogik.Surgewave.Core.Util;

/// <summary>
/// Handles compression and decompression for Kafka record batches.
/// Supports GZIP (.NET built-in), Snappy (Snappier), LZ4 (K4os.Compression.LZ4), and ZSTD (ZstdSharp).
/// </summary>
public static class CompressionCodec
{
    /// <summary>
    /// Decompress data based on the compression type from record batch attributes.
    /// </summary>
    /// <param name="compressedData">The compressed data bytes</param>
    /// <param name="compressionType">Compression type (bits 0-2 of attributes)</param>
    /// <returns>Decompressed data</returns>
    public static byte[] Decompress(byte[] compressedData, int compressionType)
    {
        return compressionType switch
        {
            KafkaConstants.Compression.None => compressedData,
            KafkaConstants.Compression.Gzip => DecompressGzip(compressedData),
            KafkaConstants.Compression.Snappy => DecompressSnappy(compressedData),
            KafkaConstants.Compression.Lz4 => DecompressLz4(compressedData),
            KafkaConstants.Compression.Zstd => DecompressZstd(compressedData),
            _ => throw new NotSupportedException($"Unknown compression type: {compressionType}")
        };
    }

    /// <summary>
    /// Decompress data using pooled buffers to reduce GC pressure.
    /// Caller MUST return the buffer to ArrayPool when done if IsPooled is true.
    /// </summary>
    /// <param name="compressedData">The compressed data span (avoids ToArray allocation)</param>
    /// <param name="compressionType">Compression type (bits 0-2 of attributes)</param>
    /// <returns>Tuple of (Buffer, ActualLength, IsPooled). If IsPooled, caller must return buffer to ArrayPool.</returns>
    public static (byte[] Buffer, int Length, bool IsPooled) DecompressPooled(
        ReadOnlySpan<byte> compressedData, int compressionType)
    {
        return compressionType switch
        {
            KafkaConstants.Compression.None => DecompressNonePooled(compressedData),
            KafkaConstants.Compression.Gzip => DecompressGzipPooled(compressedData),
            KafkaConstants.Compression.Snappy => DecompressSnappyPooled(compressedData),
            KafkaConstants.Compression.Lz4 => DecompressLz4Pooled(compressedData),
            KafkaConstants.Compression.Zstd => DecompressZstdPooled(compressedData),
            _ => throw new NotSupportedException($"Unknown compression type: {compressionType}")
        };
    }

    private static (byte[], int, bool) DecompressNonePooled(ReadOnlySpan<byte> data)
    {
        // For uncompressed data, rent buffer and copy
        var buffer = ArrayPool<byte>.Shared.Rent(data.Length);
        data.CopyTo(buffer);
        return (buffer, data.Length, true);
    }

    /// <summary>
    /// Compress data using the specified compression type.
    /// </summary>
    /// <param name="data">The uncompressed data</param>
    /// <param name="compressionType">Compression type to use</param>
    /// <returns>Compressed data</returns>
    public static byte[] Compress(byte[] data, int compressionType)
    {
        return compressionType switch
        {
            KafkaConstants.Compression.None => data,
            KafkaConstants.Compression.Gzip => CompressGzip(data),
            KafkaConstants.Compression.Snappy => CompressSnappy(data),
            KafkaConstants.Compression.Lz4 => CompressLz4(data),
            KafkaConstants.Compression.Zstd => CompressZstd(data),
            _ => throw new NotSupportedException($"Unknown compression type: {compressionType}")
        };
    }

    /// <summary>
    /// Check if a compression type is supported.
    /// </summary>
    public static bool IsSupported(int compressionType)
    {
        return compressionType is KafkaConstants.Compression.None
            or KafkaConstants.Compression.Gzip
            or KafkaConstants.Compression.Snappy
            or KafkaConstants.Compression.Lz4
            or KafkaConstants.Compression.Zstd;
    }

    /// <summary>
    /// Get compression type name for logging.
    /// </summary>
    public static string GetCompressionName(int compressionType)
    {
        return compressionType switch
        {
            KafkaConstants.Compression.None => "None",
            KafkaConstants.Compression.Gzip => "GZIP",
            KafkaConstants.Compression.Snappy => "Snappy",
            KafkaConstants.Compression.Lz4 => "LZ4",
            KafkaConstants.Compression.Zstd => "ZSTD",
            _ => $"Unknown({compressionType})"
        };
    }

    /// <summary>
    /// Extract compression type from raw record batch bytes without full parsing.
    /// Attributes field is at offset 21 (big-endian int16), compression is bits 0-2.
    /// </summary>
    public static int GetCompressionTypeFromBatch(ReadOnlySpan<byte> recordBatch)
    {
        if (recordBatch.Length < KafkaConstants.RecordBatch.AttributesOffset + 2)
            return KafkaConstants.Compression.None; // Too small, assume no compression

        var attributes = System.Buffers.Binary.BinaryPrimitives.ReadInt16BigEndian(
            recordBatch.Slice(KafkaConstants.RecordBatch.AttributesOffset, 2));

        return attributes & KafkaConstants.Compression.Mask;
    }

    /// <summary>
    /// Extract idempotence-related fields from raw record batch bytes.
    /// </summary>
    /// <returns>Tuple of (producerId, producerEpoch, baseSequence, lastOffsetDelta)</returns>
    public static (long ProducerId, short ProducerEpoch, int BaseSequence, int LastOffsetDelta) GetIdempotenceInfo(ReadOnlySpan<byte> recordBatch)
    {
        if (recordBatch.Length < KafkaConstants.RecordBatch.HeaderSize)
        {
            return (KafkaConstants.Producer.NoProducerId, KafkaConstants.Producer.NoProducerEpoch,
                    KafkaConstants.Producer.NoSequence, 0);
        }

        var producerId = System.Buffers.Binary.BinaryPrimitives.ReadInt64BigEndian(
            recordBatch.Slice(KafkaConstants.RecordBatch.ProducerIdOffset, 8));

        var producerEpoch = System.Buffers.Binary.BinaryPrimitives.ReadInt16BigEndian(
            recordBatch.Slice(KafkaConstants.RecordBatch.ProducerEpochOffset, 2));

        var baseSequence = System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(
            recordBatch.Slice(KafkaConstants.RecordBatch.BaseSequenceOffset, 4));

        var lastOffsetDelta = System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(
            recordBatch.Slice(KafkaConstants.RecordBatch.LastOffsetDeltaOffset, 4));

        return (producerId, producerEpoch, baseSequence, lastOffsetDelta);
    }

    /// <summary>
    /// Check if a record batch has idempotence enabled (ProducerId != -1).
    /// </summary>
    public static bool HasIdempotence(ReadOnlySpan<byte> recordBatch)
    {
        if (recordBatch.Length < KafkaConstants.RecordBatch.ProducerIdOffset + 8)
            return false;

        var producerId = System.Buffers.Binary.BinaryPrimitives.ReadInt64BigEndian(
            recordBatch.Slice(KafkaConstants.RecordBatch.ProducerIdOffset, 8));

        return producerId != KafkaConstants.Producer.NoProducerId;
    }

    /// <summary>
    /// Check if a record batch is part of a transaction.
    /// </summary>
    public static bool IsTransactional(ReadOnlySpan<byte> recordBatch)
    {
        if (recordBatch.Length < KafkaConstants.RecordBatch.AttributesOffset + 2)
            return false;

        var attributes = System.Buffers.Binary.BinaryPrimitives.ReadInt16BigEndian(
            recordBatch.Slice(KafkaConstants.RecordBatch.AttributesOffset, 2));

        return KafkaConstants.Attributes.IsTransactional(attributes);
    }

    /// <summary>
    /// Check if a record batch is a control batch (transaction marker).
    /// </summary>
    public static bool IsControlBatch(ReadOnlySpan<byte> recordBatch)
    {
        if (recordBatch.Length < KafkaConstants.RecordBatch.AttributesOffset + 2)
            return false;

        var attributes = System.Buffers.Binary.BinaryPrimitives.ReadInt16BigEndian(
            recordBatch.Slice(KafkaConstants.RecordBatch.AttributesOffset, 2));

        return KafkaConstants.Attributes.IsControlBatch(attributes);
    }

    /// <summary>
    /// Get the record count from a record batch header.
    /// </summary>
    public static int GetRecordCount(ReadOnlySpan<byte> recordBatch)
    {
        if (recordBatch.Length < KafkaConstants.RecordBatch.RecordsCountOffset + 4)
            return 0;

        return System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(
            recordBatch.Slice(KafkaConstants.RecordBatch.RecordsCountOffset, 4));
    }

    #region GZIP

    private static byte[] DecompressGzip(byte[] compressedData)
    {
        using var inputStream = new MemoryStream(compressedData);
        using var gzipStream = new GZipStream(inputStream, CompressionMode.Decompress);
        using var outputStream = new MemoryStream();

        gzipStream.CopyTo(outputStream);
        return outputStream.ToArray();
    }

    private static byte[] CompressGzip(byte[] data)
    {
        using var outputStream = new MemoryStream();
        using (var gzipStream = new GZipStream(outputStream, CompressionLevel.Optimal))
        {
            gzipStream.Write(data, 0, data.Length);
        }
        return outputStream.ToArray();
    }

    #endregion

    #region Snappy

    private static byte[] DecompressSnappy(byte[] compressedData)
    {
        return Snappy.DecompressToArray(compressedData);
    }

    private static byte[] CompressSnappy(byte[] data)
    {
        return Snappy.CompressToArray(data);
    }

    #endregion

    #region LZ4

    /// <summary>
    /// Decompress LZ4 data using Kafka's LZ4 frame format.
    /// Kafka uses the standard LZ4 frame format.
    /// </summary>
    private static byte[] DecompressLz4(byte[] compressedData)
    {
        using var inputStream = new MemoryStream(compressedData);
        using var lz4Stream = LZ4Stream.Decode(inputStream);
        using var outputStream = new MemoryStream();

        lz4Stream.CopyTo(outputStream);
        return outputStream.ToArray();
    }

    private static byte[] CompressLz4(byte[] data)
    {
        using var outputStream = new MemoryStream();
        using (var lz4Stream = LZ4Stream.Encode(outputStream, LZ4Level.L00_FAST))
        {
            lz4Stream.Write(data, 0, data.Length);
        }
        return outputStream.ToArray();
    }

    #endregion

    #region ZSTD

    private static byte[] DecompressZstd(byte[] compressedData)
    {
        using var decompressor = new Decompressor();
        return decompressor.Unwrap(compressedData).ToArray();
    }

    private static byte[] CompressZstd(byte[] data)
    {
        using var compressor = new Compressor();
        return compressor.Wrap(data).ToArray();
    }

    #endregion

    #region Bounded Decompression

    /// <summary>
    /// Decompresses producer-supplied data, refusing anything that would expand beyond
    /// <paramref name="maxBytes"/>.
    /// </summary>
    /// <remarks>
    /// <para>Every caller that decompresses bytes a client sent must use this rather than
    /// <see cref="DecompressPooled"/>: compression ratios above 1000:1 are trivial to construct, so
    /// a few kilobytes on the wire can become hundreds of megabytes of broker memory. The unbounded
    /// entry points remain for data the broker produced itself.</para>
    ///
    /// <para>The refusal happens as early as each format allows. zstd and snappy declare their
    /// uncompressed size, so an oversized frame is rejected before a single byte is allocated. gzip
    /// and lz4 do not, so they are decoded in fixed chunks and abandoned the moment the budget is
    /// exceeded — measuring afterwards would mean the memory was already taken, which is the whole
    /// attack.</para>
    /// </remarks>
    /// <returns>
    /// <c>false</c> when the data would exceed the budget or the frame is unreadable. On
    /// <c>false</c> nothing is rented and the out parameters are empty.
    /// </returns>
    public static bool TryDecompressBounded(
        ReadOnlySpan<byte> compressedData,
        int compressionType,
        long maxBytes,
        out byte[] buffer,
        out int length,
        out bool isPooled)
    {
        buffer = [];
        length = 0;
        isPooled = false;

        if (maxBytes <= 0)
            return false;

        switch (compressionType)
        {
            case KafkaConstants.Compression.None:
                if (compressedData.Length > maxBytes)
                    return false;
                (buffer, length, isPooled) = DecompressNonePooled(compressedData);
                return true;

            case KafkaConstants.Compression.Zstd:
            {
                // Declared in the frame header, so this costs nothing and allocates nothing.
                var declared = Decompressor.GetDecompressedSize(compressedData);
                if (declared > (ulong)maxBytes)
                    return false;

                (buffer, length, isPooled) = DecompressZstdPooled(compressedData);
                break;
            }

            case KafkaConstants.Compression.Snappy:
            {
                var declared = Snappy.GetUncompressedLength(compressedData);
                if (declared > maxBytes)
                    return false;

                (buffer, length, isPooled) = DecompressSnappyPooled(compressedData);
                break;
            }

            case KafkaConstants.Compression.Gzip:
                return TryDecompressGzipBounded(compressedData, maxBytes, out buffer, out length, out isPooled);

            case KafkaConstants.Compression.Lz4:
                return TryDecompressLz4Bounded(compressedData, maxBytes, out buffer, out length, out isPooled);

            default:
                return false;
        }

        // zstd with an absent content size falls through to the growing path, so the result is
        // still checked: a frame that declares nothing must not buy an unbounded expansion.
        if (length > maxBytes)
        {
            Release(buffer, isPooled);
            buffer = [];
            length = 0;
            isPooled = false;
            return false;
        }

        return true;
    }

    private static bool TryDecompressGzipBounded(
        ReadOnlySpan<byte> compressedData, long maxBytes,
        out byte[] buffer, out int length, out bool isPooled)
    {
        buffer = [];
        length = 0;
        isPooled = false;

        var sizeHint = (int)Math.Min(GetGzipSizeHint(compressedData), maxBytes);
        var input = ArrayPool<byte>.Shared.Rent(compressedData.Length);
        try
        {
            compressedData.CopyTo(input);
            using var inputStream = new MemoryStream(input, 0, compressedData.Length, writable: false);
            using var gzipStream = new GZipStream(inputStream, CompressionMode.Decompress);
            using var writer = new PooledArrayBufferWriter(Math.Max(sizeHint, 256));

            var total = 0L;
            int read;
            while ((read = gzipStream.Read(writer.GetSpan(ChunkSize))) > 0)
            {
                total += read;
                if (total > maxBytes)
                    return false; // writer disposal returns the rent

                writer.Advance(read);
            }

            (buffer, length) = writer.DetachBuffer();
            isPooled = true;
            return true;
        }
        catch (InvalidDataException)
        {
            return false;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(input);
        }
    }

    private static bool TryDecompressLz4Bounded(
        ReadOnlySpan<byte> compressedData, long maxBytes,
        out byte[] buffer, out int length, out bool isPooled)
    {
        buffer = [];
        length = 0;
        isPooled = false;

        // LZ4Frame.Decode writes the whole frame in one call, so the budget cannot be enforced
        // mid-decode here. Cap the ratio first: lz4's block format cannot exceed 255:1, so a frame
        // that could not possibly fit is rejected without decoding, and the decoded length is
        // checked afterwards for everything else.
        const long MaxLz4Ratio = 255;
        if (compressedData.Length * MaxLz4Ratio < 0)
            return false;

        try
        {
            using var writer = new PooledArrayBufferWriter(
                (int)Math.Min(Math.Min(3L * compressedData.Length, maxBytes), 1L << 26));

            LZ4Frame.Decode(compressedData, writer);

            var (decoded, decodedLength) = writer.DetachBuffer();
            if (decodedLength > maxBytes)
            {
                ArrayPool<byte>.Shared.Return(decoded);
                return false;
            }

            buffer = decoded;
            length = decodedLength;
            isPooled = true;
            return true;
        }
        catch (Exception ex) when (ex is InvalidDataException or ArgumentException)
        {
            return false;
        }
    }

    private const int ChunkSize = 8 * 1024;

    /// <summary>Gives a rented buffer back; a no-op for buffers that were never pooled.</summary>
    public static void Release(byte[] buffer, bool isPooled)
    {
        if (isPooled && buffer.Length > 0)
            ArrayPool<byte>.Shared.Return(buffer);
    }

    #endregion

    #region Pooled Decompression

    private static (byte[], int, bool) DecompressGzipPooled(ReadOnlySpan<byte> compressedData)
    {
        var sizeHint = GetGzipSizeHint(compressedData);

        // GZipStream needs a Stream, so the input copy is unavoidable — but the allocation is not.
        var input = ArrayPool<byte>.Shared.Rent(compressedData.Length);
        try
        {
            compressedData.CopyTo(input);
            using var inputStream = new MemoryStream(input, 0, compressedData.Length, writable: false);
            using var gzipStream = new GZipStream(inputStream, CompressionMode.Decompress);

            // Dispose is a no-op once the buffer is detached, so this both hands ownership to the
            // caller on success and returns the rent if a corrupt frame throws mid-read.
            using var writer = new PooledArrayBufferWriter(sizeHint);

            int read;
            while ((read = gzipStream.Read(writer.GetSpan(4096))) > 0)
            {
                writer.Advance(read);
            }

            var (buffer, length) = writer.DetachBuffer();
            return (buffer, length, true);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(input);
        }
    }

    /// <summary>
    /// RFC 1952: the last four bytes (ISIZE, little-endian) hold the uncompressed size mod 2^32 —
    /// an exact-size hint for our single-member frames. It is attacker-supplied on the produce
    /// path, so it is capped by what deflate can actually expand to (~1032:1); the writer still
    /// grows if the hint turns out to be too small.
    /// </summary>
    internal static int GetGzipSizeHint(ReadOnlySpan<byte> compressedData)
    {
        const int MinHint = 256;
        const long MaxDeflateRatio = 1032;

        if (compressedData.Length < 18)
        {
            return Math.Max(compressedData.Length * 3, MinHint);
        }

        var declared = BinaryPrimitives.ReadUInt32LittleEndian(compressedData[^4..]);
        var maxPlausible = compressedData.Length * MaxDeflateRatio;
        return (int)Math.Clamp(Math.Min(declared, (ulong)maxPlausible), MinHint, 1 << 26);
    }

    private static (byte[], int, bool) DecompressSnappyPooled(ReadOnlySpan<byte> compressedData)
    {
        // Snappier supports getting uncompressed length and decompressing to span
        var uncompressedLength = Snappy.GetUncompressedLength(compressedData);
        var buffer = ArrayPool<byte>.Shared.Rent(uncompressedLength);

        var actualLength = Snappy.Decompress(compressedData, buffer);
        return (buffer, actualLength, true);
    }

    private static (byte[], int, bool) DecompressLz4Pooled(ReadOnlySpan<byte> compressedData)
    {
        // LZ4 frames rarely carry a content-size field, so decode straight into a pooled growable
        // writer: span source means no input copy, and the output lands in the rented array.
        var initialCapacity = (int)Math.Min(3L * compressedData.Length, 1L << 26);

        // Dispose is a no-op once detached: ownership goes to the caller on success, and a corrupt
        // frame gives the rent back instead of leaking it.
        using var writer = new PooledArrayBufferWriter(initialCapacity);

        LZ4Frame.Decode(compressedData, writer);

        var (buffer, length) = writer.DetachBuffer();
        return (buffer, length, true);
    }

    private static (byte[], int, bool) DecompressZstdPooled(ReadOnlySpan<byte> compressedData)
    {
        using var decompressor = new Decompressor();

        // ZstdSharp can decompress to a span if we know the size
        // Try to get content size from frame header
        var contentSize = Decompressor.GetDecompressedSize(compressedData);

        if (contentSize > 0)
        {
            // We know the exact size - rent and decompress directly
            var buffer = ArrayPool<byte>.Shared.Rent((int)contentSize);
            var actualSize = decompressor.Unwrap(compressedData, buffer);
            return (buffer, actualSize, true);
        }
        else
        {
            // Unknown size - use Unwrap which allocates, then copy to pooled
            var result = decompressor.Unwrap(compressedData);
            var buffer = ArrayPool<byte>.Shared.Rent(result.Length);
            result.CopyTo(buffer);
            return (buffer, result.Length, true);
        }
    }

    #endregion
}
