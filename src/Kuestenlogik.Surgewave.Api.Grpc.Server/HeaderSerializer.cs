using Google.Protobuf;

namespace Kuestenlogik.Surgewave.Api.Grpc.Server;

/// <summary>
/// Utility class for serializing and deserializing Kafka record headers.
/// Format: [count:int32][key_len:int32][key:bytes][value_len:int32][value:bytes]...
/// </summary>
public static class HeaderSerializer
{
    /// <summary>
    /// Serializes headers to a binary format.
    /// </summary>
    public static byte[] Serialize(IReadOnlyDictionary<string, ByteString>? headers)
    {
        if (headers == null || headers.Count == 0)
            return [];

        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);

        writer.Write(headers.Count);
        foreach (var (key, value) in headers)
        {
            var keyBytes = System.Text.Encoding.UTF8.GetBytes(key);
            writer.Write(keyBytes.Length);
            writer.Write(keyBytes);
            writer.Write(value.Length);
            writer.Write(value.ToByteArray());
        }

        return ms.ToArray();
    }

    /// <summary>
    /// Deserializes headers from a binary format.
    /// </summary>
    public static Dictionary<string, ByteString> Deserialize(ReadOnlySpan<byte> headerBytes)
    {
        var headers = new Dictionary<string, ByteString>();

        if (headerBytes.Length == 0)
            return headers;

        using var ms = new MemoryStream(headerBytes.ToArray());
        using var reader = new BinaryReader(ms);

        var count = reader.ReadInt32();
        for (int i = 0; i < count; i++)
        {
            var keyLen = reader.ReadInt32();
            var key = System.Text.Encoding.UTF8.GetString(reader.ReadBytes(keyLen));
            var valueLen = reader.ReadInt32();
            var value = ByteString.CopyFrom(reader.ReadBytes(valueLen));
            headers[key] = value;
        }

        return headers;
    }
}
