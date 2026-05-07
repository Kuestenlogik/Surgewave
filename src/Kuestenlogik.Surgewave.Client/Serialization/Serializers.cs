using System.Text;
using System.Text.Json;

namespace Kuestenlogik.Surgewave.Client.Serialization;

/// <summary>
/// Built-in serializers and deserializers.
/// </summary>
public static class Serializers
{
    /// <summary>
    /// Null serializer (for producers without keys).
    /// </summary>
    public static ISerializer<Null> Null { get; } = new NullSerializer();

    /// <summary>
    /// UTF-8 string serializer.
    /// </summary>
    public static ISerializer<string> String { get; } = new StringSerializer();

    /// <summary>
    /// UTF-8 string deserializer.
    /// </summary>
    public static IDeserializer<string> StringDeserializer { get; } = new StringDeserializerImpl();

    /// <summary>
    /// Byte array pass-through serializer.
    /// </summary>
    public static ISerializer<byte[]> ByteArray { get; } = new ByteArraySerializer();

    /// <summary>
    /// Byte array pass-through deserializer.
    /// </summary>
    public static IDeserializer<byte[]> ByteArrayDeserializer { get; } = new ByteArrayDeserializerImpl();

    /// <summary>
    /// Int32 serializer (big-endian).
    /// </summary>
    public static ISerializer<int> Int32 { get; } = new Int32Serializer();

    /// <summary>
    /// Int32 deserializer (big-endian).
    /// </summary>
    public static IDeserializer<int> Int32Deserializer { get; } = new Int32DeserializerImpl();

    /// <summary>
    /// Int64 serializer (big-endian).
    /// </summary>
    public static ISerializer<long> Int64 { get; } = new Int64Serializer();

    /// <summary>
    /// Int64 deserializer (big-endian).
    /// </summary>
    public static IDeserializer<long> Int64Deserializer { get; } = new Int64DeserializerImpl();

    /// <summary>
    /// Guid serializer.
    /// </summary>
    public static ISerializer<Guid> Guid { get; } = new GuidSerializer();

    /// <summary>
    /// Guid deserializer.
    /// </summary>
    public static IDeserializer<Guid> GuidDeserializer { get; } = new GuidDeserializerImpl();

    /// <summary>
    /// Creates a JSON serializer for type T.
    /// </summary>
    public static ISerializer<T> Json<T>(JsonSerializerOptions? options = null)
        => new JsonSerializer<T>(options);

    /// <summary>
    /// Creates a JSON deserializer for type T.
    /// </summary>
    public static IDeserializer<T> JsonDeserializer<T>(JsonSerializerOptions? options = null)
        => new JsonDeserializerImpl<T>(options);

    /// <summary>
    /// Creates a polymorphic JSON serializer that includes type discriminators.
    /// Use this when you need to serialize derived types and deserialize them back to the correct type.
    /// </summary>
    /// <typeparam name="TBase">The base type.</typeparam>
    /// <param name="derivedTypes">The derived types to support.</param>
    /// <param name="options">Optional JSON serializer options.</param>
    /// <example>
    /// <code>
    /// // Define your types with JsonDerivedType attributes
    /// [JsonPolymorphic(TypeDiscriminatorPropertyName = "$type")]
    /// [JsonDerivedType(typeof(OrderCreated), "order.created")]
    /// [JsonDerivedType(typeof(OrderShipped), "order.shipped")]
    /// public abstract class OrderEvent { public string OrderId { get; set; } }
    ///
    /// // Or register types at runtime
    /// var serializer = Serializers.PolymorphicJson&lt;OrderEvent&gt;(
    ///     typeof(OrderCreated), typeof(OrderShipped));
    /// </code>
    /// </example>
    public static ISerializer<TBase> PolymorphicJson<TBase>(
        params Type[] derivedTypes) where TBase : class
        => PolymorphicJson<TBase>(null, derivedTypes);

    /// <summary>
    /// Creates a polymorphic JSON serializer with custom options.
    /// </summary>
    public static ISerializer<TBase> PolymorphicJson<TBase>(
        JsonSerializerOptions? options,
        params Type[] derivedTypes) where TBase : class
        => new PolymorphicJsonSerializer<TBase>(options, derivedTypes);

    /// <summary>
    /// Creates a polymorphic JSON deserializer that handles type discriminators.
    /// </summary>
    public static IDeserializer<TBase> PolymorphicJsonDeserializer<TBase>(
        params Type[] derivedTypes) where TBase : class
        => PolymorphicJsonDeserializer<TBase>(null, derivedTypes);

    /// <summary>
    /// Creates a polymorphic JSON deserializer with custom options.
    /// </summary>
    public static IDeserializer<TBase> PolymorphicJsonDeserializer<TBase>(
        JsonSerializerOptions? options,
        params Type[] derivedTypes) where TBase : class
        => new PolymorphicJsonDeserializerImpl<TBase>(options, derivedTypes);

    // ═══════════════════════════════════════════════════════════════
    // Internal implementations
    // ═══════════════════════════════════════════════════════════════

    private sealed class NullSerializer : ISerializer<Null>
    {
        public byte[]? Serialize(Null? data, string topic) => null;
    }

    private sealed class StringSerializer : ISerializer<string>
    {
        public byte[]? Serialize(string? data, string topic)
            => data == null ? null : Encoding.UTF8.GetBytes(data);
    }

    private sealed class StringDeserializerImpl : IDeserializer<string>
    {
        public string Deserialize(ReadOnlySpan<byte> data, string topic)
            => data.IsEmpty ? string.Empty : Encoding.UTF8.GetString(data);
    }

    private sealed class ByteArraySerializer : ISerializer<byte[]>
    {
        public byte[]? Serialize(byte[]? data, string topic) => data;
    }

    private sealed class ByteArrayDeserializerImpl : IDeserializer<byte[]>
    {
        public byte[] Deserialize(ReadOnlySpan<byte> data, string topic)
            => data.ToArray();
    }

    private sealed class Int32Serializer : ISerializer<int>
    {
        public byte[]? Serialize(int data, string topic)
        {
            var bytes = new byte[4];
            System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(bytes, data);
            return bytes;
        }
    }

    private sealed class Int32DeserializerImpl : IDeserializer<int>
    {
        public int Deserialize(ReadOnlySpan<byte> data, string topic)
            => System.Buffers.Binary.BinaryPrimitives.ReadInt32BigEndian(data);
    }

    private sealed class Int64Serializer : ISerializer<long>
    {
        public byte[]? Serialize(long data, string topic)
        {
            var bytes = new byte[8];
            System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(bytes, data);
            return bytes;
        }
    }

    private sealed class Int64DeserializerImpl : IDeserializer<long>
    {
        public long Deserialize(ReadOnlySpan<byte> data, string topic)
            => System.Buffers.Binary.BinaryPrimitives.ReadInt64BigEndian(data);
    }

    private sealed class GuidSerializer : ISerializer<Guid>
    {
        public byte[]? Serialize(Guid data, string topic)
            => data.ToByteArray();
    }

    private sealed class GuidDeserializerImpl : IDeserializer<Guid>
    {
        public Guid Deserialize(ReadOnlySpan<byte> data, string topic)
            => new Guid(data);
    }

    private sealed class JsonSerializer<T>(JsonSerializerOptions? options) : ISerializer<T>
    {
        private readonly JsonSerializerOptions _options = options ?? new JsonSerializerOptions();

        public byte[]? Serialize(T? data, string topic)
            => data == null ? null : System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(data, _options);
    }

    private sealed class JsonDeserializerImpl<T>(JsonSerializerOptions? options) : IDeserializer<T>
    {
        private readonly JsonSerializerOptions _options = options ?? new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        };

        public T Deserialize(ReadOnlySpan<byte> data, string topic)
            => System.Text.Json.JsonSerializer.Deserialize<T>(data, _options)!;
    }

    private sealed class PolymorphicJsonSerializer<TBase>(
        JsonSerializerOptions? options,
        Type[] derivedTypes) : ISerializer<TBase> where TBase : class
    {
        private readonly JsonSerializerOptions _options = CreateOptions(options, derivedTypes);

        public byte[]? Serialize(TBase? data, string topic)
            => data == null ? null : System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(data, data.GetType(), _options);

        private static JsonSerializerOptions CreateOptions(JsonSerializerOptions? baseOptions, Type[] derivedTypes)
        {
            var options = baseOptions != null
                ? new JsonSerializerOptions(baseOptions)
                : new JsonSerializerOptions();

            if (derivedTypes.Length > 0)
            {
                options.TypeInfoResolver = new PolymorphicTypeResolver<TBase>(derivedTypes);
            }

            return options;
        }
    }

    private sealed class PolymorphicJsonDeserializerImpl<TBase>(
        JsonSerializerOptions? options,
        Type[] derivedTypes) : IDeserializer<TBase> where TBase : class
    {
        private readonly JsonSerializerOptions _options = CreateOptions(options, derivedTypes);

        public TBase Deserialize(ReadOnlySpan<byte> data, string topic)
            => System.Text.Json.JsonSerializer.Deserialize<TBase>(data, _options)!;

        private static JsonSerializerOptions CreateOptions(JsonSerializerOptions? baseOptions, Type[] derivedTypes)
        {
            var options = baseOptions != null
                ? new JsonSerializerOptions(baseOptions)
                : new JsonSerializerOptions { PropertyNameCaseInsensitive = true };

            if (derivedTypes.Length > 0)
            {
                options.TypeInfoResolver = new PolymorphicTypeResolver<TBase>(derivedTypes);
            }

            return options;
        }
    }

    private sealed class PolymorphicTypeResolver<TBase>(Type[] derivedTypes)
        : System.Text.Json.Serialization.Metadata.DefaultJsonTypeInfoResolver
    {
        public override System.Text.Json.Serialization.Metadata.JsonTypeInfo GetTypeInfo(
            Type type,
            JsonSerializerOptions options)
        {
            var typeInfo = base.GetTypeInfo(type, options);

            if (type == typeof(TBase))
            {
                typeInfo.PolymorphismOptions = new System.Text.Json.Serialization.Metadata.JsonPolymorphismOptions
                {
                    TypeDiscriminatorPropertyName = "$type",
                    IgnoreUnrecognizedTypeDiscriminators = true,
                    UnknownDerivedTypeHandling = System.Text.Json.Serialization.JsonUnknownDerivedTypeHandling.FailSerialization
                };

                foreach (var derivedType in derivedTypes)
                {
                    typeInfo.PolymorphismOptions.DerivedTypes.Add(
                        new System.Text.Json.Serialization.Metadata.JsonDerivedType(
                            derivedType,
                            derivedType.Name));
                }
            }

            return typeInfo;
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
