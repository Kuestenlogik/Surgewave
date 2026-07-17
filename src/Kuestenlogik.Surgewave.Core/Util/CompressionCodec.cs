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
