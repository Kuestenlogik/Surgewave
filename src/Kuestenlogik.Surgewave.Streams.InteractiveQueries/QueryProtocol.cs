namespace Kuestenlogik.Surgewave.Streams.InteractiveQueries;

/// <summary>
/// Binary protocol for Remote Interactive Queries over TCP.
/// Frame: [4 bytes: length][1 byte: message type][payload]
/// </summary>
internal static class QueryProtocol
{
    // Request types
    public const byte MetadataRequest = 0x01;
    public const byte KeyValueGetRequest = 0x10;
    public const byte KeyValueRangeRequest = 0x11;
    public const byte KeyValueAllRequest = 0x12;
    public const byte KeyValueCountRequest = 0x13;

    // Response types
    public const byte MetadataResponse = 0x80;
    public const byte KeyValueGetResponse = 0x81;
    public const byte KeyValueRangeResponse = 0x82;
    public const byte KeyValueAllResponse = 0x83;
    public const byte KeyValueCountResponse = 0x84;
    public const byte ErrorResponse = 0xFF;

    // Status codes in responses
    public const byte StatusOk = 0x00;
    public const byte StatusNotFound = 0x01;
    public const byte StatusStoreNotFound = 0x02;
    public const byte StatusError = 0x03;

    /// <summary>
    /// Write a length-prefixed string.
    /// </summary>
    public static void WriteString(BinaryWriter writer, string value)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(value);
        writer.Write(bytes.Length);
        writer.Write(bytes);
    }

    /// <summary>
    /// Read a length-prefixed string.
    /// </summary>
    public static string ReadString(BinaryReader reader)
    {
        var len = reader.ReadInt32();
        var bytes = reader.ReadBytes(len);
        return System.Text.Encoding.UTF8.GetString(bytes);
    }

    /// <summary>
    /// Write a length-prefixed byte array.
    /// </summary>
    public static void WriteBytes(BinaryWriter writer, byte[] data)
    {
        writer.Write(data.Length);
        writer.Write(data);
    }

    /// <summary>
    /// Read a length-prefixed byte array.
    /// </summary>
    public static byte[] ReadBytes(BinaryReader reader)
    {
        var len = reader.ReadInt32();
        return reader.ReadBytes(len);
    }

    /// <summary>
    /// Murmur2 hash (same algorithm as Kafka default partitioner).
    /// Provides consistent hashing for key-to-partition mapping across instances.
    /// </summary>
    public static uint Murmur2(byte[] data)
    {
        const uint seed = 0x9747b28c;
        const uint m = 0x5bd1e995;
        const int r = 24;

        var length = data.Length;
        var h = seed ^ (uint)length;
        var offset = 0;

        while (length >= 4)
        {
            var k = (uint)(data[offset] | (data[offset + 1] << 8) | (data[offset + 2] << 16) | (data[offset + 3] << 24));
            k *= m;
            k ^= k >> r;
            k *= m;
            h *= m;
            h ^= k;
            offset += 4;
            length -= 4;
        }

        switch (length)
        {
            case 3: h ^= (uint)data[offset + 2] << 16; goto case 2;
            case 2: h ^= (uint)data[offset + 1] << 8; goto case 1;
            case 1: h ^= data[offset]; h *= m; break;
        }

        h ^= h >> 13;
        h *= m;
        h ^= h >> 15;

        return h;
    }
}
