using System.Buffers.Binary;
using Google.Protobuf;

namespace Kuestenlogik.Surgewave.Api.Grpc.Server;

/// <summary>
/// Utility class for serializing and deserializing Kafka record headers.
/// Format: [count:int32][key_len:int32][key:bytes][value_len:int32][value:bytes]...
/// All integers are BIG-endian — this is the native-wire header block layout
/// that RecordBatchSerializer.WriteHeadersFromNativeBlock consumes. The
/// previous little-endian BinaryWriter encoding made every gRPC produce with
/// headers crash in the batch serializer (count read as 16M+).
/// </summary>
public static class HeaderSerializer
{
    /// <summary>
    /// Serializes headers to the native-wire header block format.
    /// </summary>
    public static byte[] Serialize(IReadOnlyDictionary<string, ByteString>? headers)
    {
        if (headers == null || headers.Count == 0)
            return [];

        var size = 4;
        foreach (var (key, value) in headers)
        {
            size += 8 + System.Text.Encoding.UTF8.GetByteCount(key) + value.Length;
        }

        var block = new byte[size];
        var pos = 0;
        BinaryPrimitives.WriteInt32BigEndian(block.AsSpan(pos), headers.Count);
        pos += 4;

        foreach (var (key, value) in headers)
        {
            var keyLen = System.Text.Encoding.UTF8.GetBytes(key, block.AsSpan(pos + 4));
            BinaryPrimitives.WriteInt32BigEndian(block.AsSpan(pos), keyLen);
            pos += 4 + keyLen;

            BinaryPrimitives.WriteInt32BigEndian(block.AsSpan(pos), value.Length);
            pos += 4;
            value.Span.CopyTo(block.AsSpan(pos));
            pos += value.Length;
        }

        return block;
    }

    /// <summary>
    /// Deserializes headers from the native-wire header block format.
    /// </summary>
    public static Dictionary<string, ByteString> Deserialize(ReadOnlySpan<byte> headerBytes)
    {
        var headers = new Dictionary<string, ByteString>();

        if (headerBytes.Length < 4)
            return headers;

        var pos = 0;
        var count = BinaryPrimitives.ReadInt32BigEndian(headerBytes);
        pos += 4;

        for (int i = 0; i < count; i++)
        {
            var keyLen = BinaryPrimitives.ReadInt32BigEndian(headerBytes[pos..]);
            pos += 4;
            var key = System.Text.Encoding.UTF8.GetString(headerBytes.Slice(pos, keyLen));
            pos += keyLen;

            var valueLen = BinaryPrimitives.ReadInt32BigEndian(headerBytes[pos..]);
            pos += 4;
            var value = ByteString.CopyFrom(headerBytes.Slice(pos, valueLen));
            pos += valueLen;

            headers[key] = value;
        }

        return headers;
    }
}
