using System.Buffers.Binary;
using System.Text;

namespace Confluent.Kafka;

/// <summary>
/// Built-in serializers for common types.
/// </summary>
public static class Serializers
{
    /// <summary>
    /// Null serializer (for keys that are always null).
    /// </summary>
    public static ISerializer<Null> Null { get; } = new NullSerializer();

    /// <summary>
    /// UTF-8 string serializer.
    /// </summary>
    public static ISerializer<string> Utf8 { get; } = new Utf8Serializer();

    /// <summary>
    /// Byte array pass-through serializer.
    /// </summary>
    public static ISerializer<byte[]> ByteArray { get; } = new ByteArraySerializer();

    /// <summary>
    /// Int32 serializer (big-endian).
    /// </summary>
    public static ISerializer<int> Int32 { get; } = new Int32Serializer();

    /// <summary>
    /// Int64 serializer (big-endian).
    /// </summary>
    public static ISerializer<long> Int64 { get; } = new Int64Serializer();

    /// <summary>
    /// Single/float serializer (big-endian).
    /// </summary>
    public static ISerializer<float> Single { get; } = new SingleSerializer();

    /// <summary>
    /// Double serializer (big-endian).
    /// </summary>
    public static ISerializer<double> Double { get; } = new DoubleSerializer();

    private sealed class NullSerializer : ISerializer<Null>
    {
        public byte[]? Serialize(Null data, SerializationContext context) => null;
    }

    private sealed class Utf8Serializer : ISerializer<string>
    {
        public byte[]? Serialize(string data, SerializationContext context) =>
            data is null ? null : Encoding.UTF8.GetBytes(data);
    }

    private sealed class ByteArraySerializer : ISerializer<byte[]>
    {
        public byte[]? Serialize(byte[] data, SerializationContext context) => data;
    }

    private sealed class Int32Serializer : ISerializer<int>
    {
        public byte[]? Serialize(int data, SerializationContext context)
        {
            var bytes = new byte[4];
            BinaryPrimitives.WriteInt32BigEndian(bytes, data);
            return bytes;
        }
    }

    private sealed class Int64Serializer : ISerializer<long>
    {
        public byte[]? Serialize(long data, SerializationContext context)
        {
            var bytes = new byte[8];
            BinaryPrimitives.WriteInt64BigEndian(bytes, data);
            return bytes;
        }
    }

    private sealed class SingleSerializer : ISerializer<float>
    {
        public byte[]? Serialize(float data, SerializationContext context)
        {
            var bytes = new byte[4];
            BinaryPrimitives.WriteSingleBigEndian(bytes, data);
            return bytes;
        }
    }

    private sealed class DoubleSerializer : ISerializer<double>
    {
        public byte[]? Serialize(double data, SerializationContext context)
        {
            var bytes = new byte[8];
            BinaryPrimitives.WriteDoubleBigEndian(bytes, data);
            return bytes;
        }
    }
}

/// <summary>
/// Represents a null key type.
/// </summary>
public sealed class Null
{
    private Null() { }

    /// <summary>
    /// Singleton instance.
    /// </summary>
    public static Null Instance { get; } = new();
}
