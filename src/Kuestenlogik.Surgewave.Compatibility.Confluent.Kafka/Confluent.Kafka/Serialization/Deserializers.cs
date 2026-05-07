using System.Buffers.Binary;
using System.Text;

namespace Confluent.Kafka;

/// <summary>
/// Built-in deserializers for common types.
/// </summary>
public static class Deserializers
{
    /// <summary>
    /// Null deserializer (returns Null.Instance).
    /// </summary>
    public static IDeserializer<Null> Null { get; } = new NullDeserializer();

    /// <summary>
    /// Ignores the data and returns default value.
    /// </summary>
    public static IDeserializer<Ignore> Ignore { get; } = new IgnoreDeserializer();

    /// <summary>
    /// UTF-8 string deserializer.
    /// </summary>
    public static IDeserializer<string> Utf8 { get; } = new Utf8Deserializer();

    /// <summary>
    /// Byte array pass-through deserializer.
    /// </summary>
    public static IDeserializer<byte[]> ByteArray { get; } = new ByteArrayDeserializer();

    /// <summary>
    /// Int32 deserializer (big-endian).
    /// </summary>
    public static IDeserializer<int> Int32 { get; } = new Int32Deserializer();

    /// <summary>
    /// Int64 deserializer (big-endian).
    /// </summary>
    public static IDeserializer<long> Int64 { get; } = new Int64Deserializer();

    /// <summary>
    /// Single/float deserializer (big-endian).
    /// </summary>
    public static IDeserializer<float> Single { get; } = new SingleDeserializer();

    /// <summary>
    /// Double deserializer (big-endian).
    /// </summary>
    public static IDeserializer<double> Double { get; } = new DoubleDeserializer();

    private sealed class NullDeserializer : IDeserializer<Null>
    {
        public Null Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context) =>
            Confluent.Kafka.Null.Instance;
    }

    private sealed class IgnoreDeserializer : IDeserializer<Ignore>
    {
        public Ignore Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context) =>
            Confluent.Kafka.Ignore.Instance;
    }

    private sealed class Utf8Deserializer : IDeserializer<string>
    {
        public string Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context) =>
            isNull || data.IsEmpty ? string.Empty : Encoding.UTF8.GetString(data);
    }

    private sealed class ByteArrayDeserializer : IDeserializer<byte[]>
    {
        public byte[] Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context) =>
            isNull ? [] : data.ToArray();
    }

    private sealed class Int32Deserializer : IDeserializer<int>
    {
        public int Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context) =>
            isNull || data.Length < 4 ? 0 : BinaryPrimitives.ReadInt32BigEndian(data);
    }

    private sealed class Int64Deserializer : IDeserializer<long>
    {
        public long Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context) =>
            isNull || data.Length < 8 ? 0L : BinaryPrimitives.ReadInt64BigEndian(data);
    }

    private sealed class SingleDeserializer : IDeserializer<float>
    {
        public float Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context) =>
            isNull || data.Length < 4 ? 0f : BinaryPrimitives.ReadSingleBigEndian(data);
    }

    private sealed class DoubleDeserializer : IDeserializer<double>
    {
        public double Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context) =>
            isNull || data.Length < 8 ? 0d : BinaryPrimitives.ReadDoubleBigEndian(data);
    }
}

/// <summary>
/// Type used when deserialized data should be ignored.
/// </summary>
public sealed class Ignore
{
    private Ignore() { }

    /// <summary>
    /// Singleton instance.
    /// </summary>
    public static Ignore Instance { get; } = new();
}
