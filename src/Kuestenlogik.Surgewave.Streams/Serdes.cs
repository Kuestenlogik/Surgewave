using System.Text;
using System.Text.Json;

namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// Built-in serializer/deserializer implementations for common types.
/// </summary>
/// <example>
/// <code>
/// var consumed = Consumed&lt;string, Order&gt;.With(Serdes.String(), Serdes.Json&lt;Order&gt;());
/// builder.Stream("orders", consumed);
/// </code>
/// </example>
public static class Serdes
{
    /// <summary>Creates a UTF-8 string serde.</summary>
    /// <returns>A string serde instance.</returns>
    public static ISerde<string> String() => StringSerde.Instance;

    /// <summary>Creates an Int32 serde using little-endian byte order.</summary>
    /// <returns>An Int32 serde instance.</returns>
    public static ISerde<int> Int32() => Int32Serde.Instance;

    /// <summary>Creates an Int64 serde using little-endian byte order.</summary>
    /// <returns>An Int64 serde instance.</returns>
    public static ISerde<long> Int64() => Int64Serde.Instance;

    /// <summary>Creates a Double serde using little-endian byte order.</summary>
    /// <returns>A Double serde instance.</returns>
    public static ISerde<double> Double() => DoubleSerde.Instance;

    /// <summary>Creates a pass-through byte array serde.</summary>
    /// <returns>A byte array serde instance.</returns>
    public static ISerde<byte[]> ByteArray() => ByteArraySerde.Instance;

    /// <summary>Creates a JSON serde using System.Text.Json with camelCase naming.</summary>
    /// <typeparam name="T">The type to serialize and deserialize.</typeparam>
    /// <returns>A JSON serde instance.</returns>
    public static ISerde<T> Json<T>() => new JsonSerde<T>();

    private sealed class StringSerde : ISerde<string>
    {
        public static readonly StringSerde Instance = new();
        public byte[] Serialize(string value) => Encoding.UTF8.GetBytes(value);
        public string Deserialize(byte[] data) => Encoding.UTF8.GetString(data);
    }

    private sealed class Int32Serde : ISerde<int>
    {
        public static readonly Int32Serde Instance = new();
        public byte[] Serialize(int value) => BitConverter.GetBytes(value);
        public int Deserialize(byte[] data) => BitConverter.ToInt32(data);
    }

    private sealed class Int64Serde : ISerde<long>
    {
        public static readonly Int64Serde Instance = new();
        public byte[] Serialize(long value) => BitConverter.GetBytes(value);
        public long Deserialize(byte[] data) => BitConverter.ToInt64(data);
    }

    private sealed class DoubleSerde : ISerde<double>
    {
        public static readonly DoubleSerde Instance = new();
        public byte[] Serialize(double value) => BitConverter.GetBytes(value);
        public double Deserialize(byte[] data) => BitConverter.ToDouble(data);
    }

    private sealed class ByteArraySerde : ISerde<byte[]>
    {
        public static readonly ByteArraySerde Instance = new();
        public byte[] Serialize(byte[] value) => value;
        public byte[] Deserialize(byte[] data) => data;
    }

    private sealed class JsonSerde<T> : ISerde<T>
    {
        private static readonly JsonSerializerOptions Options = new()
        {
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase
        };

        public byte[] Serialize(T value) => JsonSerializer.SerializeToUtf8Bytes(value, Options);
        public T Deserialize(byte[] data) => JsonSerializer.Deserialize<T>(data, Options)!;
    }
}
