namespace Kuestenlogik.Surgewave.Protocol.Native.Serialization;

using System.Buffers.Binary;
using System.Text;

/// <summary>
/// Encodes / decodes a per-message header block on the native Surgewave
/// wire protocol. The block layout is independent of the Kafka RecordBatch
/// header encoding so the server can store the bytes verbatim without
/// re-parsing them in the hot path:
///
///   int32 count            // 0 if no headers
///   for each header:
///     int32 keyLength      // UTF-8 byte length, must be ≥ 0
///     bytes  keyUtf8
///     int32 valueLength    // -1 for null, 0 for empty, ≥ 0 otherwise
///     bytes  value         // omitted when length is -1
///
/// All integers are big-endian, matching the surrounding protocol.
/// </summary>
public static class NativeMessageHeaderCodec
{
    /// <summary>Computes how many bytes <see cref="Encode"/> would write.</summary>
    public static int EncodedSize(IReadOnlyDictionary<string, byte[]>? headers)
    {
        if (headers is null || headers.Count == 0) return 4;
        var total = 4;
        foreach (var (key, value) in headers)
        {
            total += 4 + Encoding.UTF8.GetByteCount(key);
            total += 4 + (value?.Length ?? 0);
        }
        return total;
    }

    /// <summary>Writes the header block to <paramref name="destination"/> and returns bytes written.</summary>
    public static int Encode(IReadOnlyDictionary<string, byte[]>? headers, Span<byte> destination)
    {
        if (headers is null || headers.Count == 0)
        {
            BinaryPrimitives.WriteInt32BigEndian(destination, 0);
            return 4;
        }

        var pos = 0;
        BinaryPrimitives.WriteInt32BigEndian(destination[pos..], headers.Count);
        pos += 4;
        foreach (var (key, value) in headers)
        {
            var keyByteCount = Encoding.UTF8.GetByteCount(key);
            BinaryPrimitives.WriteInt32BigEndian(destination[pos..], keyByteCount);
            pos += 4;
            Encoding.UTF8.GetBytes(key.AsSpan(), destination.Slice(pos, keyByteCount));
            pos += keyByteCount;

            if (value is null)
            {
                BinaryPrimitives.WriteInt32BigEndian(destination[pos..], -1);
                pos += 4;
            }
            else
            {
                BinaryPrimitives.WriteInt32BigEndian(destination[pos..], value.Length);
                pos += 4;
                value.CopyTo(destination[pos..]);
                pos += value.Length;
            }
        }
        return pos;
    }

    /// <summary>Decodes a header block starting at the current reader position.</summary>
    public static Dictionary<string, byte[]>? Decode(ReadOnlySpan<byte> source, out int bytesConsumed)
    {
        var pos = 0;
        var count = BinaryPrimitives.ReadInt32BigEndian(source);
        pos += 4;
        if (count <= 0)
        {
            bytesConsumed = pos;
            return null;
        }

        var headers = new Dictionary<string, byte[]>(count);
        for (var i = 0; i < count; i++)
        {
            var keyLen = BinaryPrimitives.ReadInt32BigEndian(source[pos..]);
            pos += 4;
            var key = Encoding.UTF8.GetString(source.Slice(pos, keyLen));
            pos += keyLen;
            var valLen = BinaryPrimitives.ReadInt32BigEndian(source[pos..]);
            pos += 4;
            if (valLen < 0)
            {
                headers[key] = null!;
            }
            else
            {
                var value = source.Slice(pos, valLen).ToArray();
                headers[key] = value;
                pos += valLen;
            }
        }
        bytesConsumed = pos;
        return headers;
    }

    /// <summary>
    /// Computes the byte length of an encoded header block without materializing keys or values —
    /// the broker's produce path only needs to skip the block, it never reads the headers (#83).
    /// Consumes exactly the same bytes as <see cref="Decode"/> and rejects the same malformed
    /// input: a bad length must throw here too, because a silently wrong block length would
    /// mis-frame every following message instead of failing the request.
    /// </summary>
    public static int GetBlockLength(ReadOnlySpan<byte> source)
    {
        var count = BinaryPrimitives.ReadInt32BigEndian(source);
        var pos = 4;
        if (count <= 0)
        {
            return pos;
        }

        for (var i = 0; i < count; i++)
        {
            var keyLen = BinaryPrimitives.ReadInt32BigEndian(source[pos..]);
            pos += 4;
            // Mirrors Decode's source.Slice(pos, keyLen) bounds check.
            if (keyLen < 0 || pos + keyLen > source.Length)
            {
                throw new ArgumentOutOfRangeException(
                    nameof(source), $"Header key length {keyLen} exceeds the {source.Length - pos} bytes remaining");
            }

            pos += keyLen;
            var valLen = BinaryPrimitives.ReadInt32BigEndian(source[pos..]);
            pos += 4;
            // Decode treats valLen < 0 as a null value and consumes no value bytes.
            if (valLen > 0)
            {
                if (pos + valLen > source.Length)
                {
                    throw new ArgumentOutOfRangeException(
                        nameof(source), $"Header value length {valLen} exceeds the {source.Length - pos} bytes remaining");
                }

                pos += valLen;
            }
        }

        return pos;
    }
}
