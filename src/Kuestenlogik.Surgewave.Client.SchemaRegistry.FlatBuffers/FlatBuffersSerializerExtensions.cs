using Google.FlatBuffers;
using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Client.Native.Operations.Schema;

namespace Kuestenlogik.Surgewave.Client.SchemaRegistry.FlatBuffers;

/// <summary>
/// Extension methods for configuring FlatBuffers schema registry serializers.
/// </summary>
public static class FlatBuffersSerializerExtensions
{
    /// <summary>
    /// Configure the producer to use FlatBuffers serialization for values with schema registry integration.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The FlatBuffers table type.</typeparam>
    /// <param name="options">The producer options.</param>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    /// <param name="serializeFunc">Function to serialize the FlatBuffer object to bytes.</param>
    /// <param name="configure">Optional configuration action for the serializer config.</param>
    /// <returns>The producer options for chaining.</returns>
    public static ProducerOptions<TKey, TValue> WithFlatBuffersValueSerializer<TKey, TValue>(
        this ProducerOptions<TKey, TValue> options,
        ISchemaRegistryOperations schemaRegistry,
        Func<TValue, byte[]> serializeFunc,
        Action<FlatBuffersSerializerConfig>? configure = null)
        where TValue : struct, IFlatbufferObject
    {
        var config = new FlatBuffersSerializerConfig(schemaRegistry);
        configure?.Invoke(config);
        options.AsyncValueSerializer = new SchemaRegistryFlatBuffersSerializer<TValue>(config, serializeFunc);
        return options;
    }

    /// <summary>
    /// Configure the producer to use FlatBuffers serialization for keys with schema registry integration.
    /// </summary>
    /// <typeparam name="TKey">The FlatBuffers table type for keys.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="options">The producer options.</param>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    /// <param name="serializeFunc">Function to serialize the FlatBuffer object to bytes.</param>
    /// <param name="configure">Optional configuration action for the serializer config.</param>
    /// <returns>The producer options for chaining.</returns>
    public static ProducerOptions<TKey, TValue> WithFlatBuffersKeySerializer<TKey, TValue>(
        this ProducerOptions<TKey, TValue> options,
        ISchemaRegistryOperations schemaRegistry,
        Func<TKey, byte[]> serializeFunc,
        Action<FlatBuffersSerializerConfig>? configure = null)
        where TKey : struct, IFlatbufferObject
    {
        var config = new FlatBuffersSerializerConfig(schemaRegistry) { IsKey = true };
        configure?.Invoke(config);
        options.AsyncKeySerializer = new SchemaRegistryFlatBuffersSerializer<TKey>(config, serializeFunc);
        return options;
    }

    /// <summary>
    /// Configure the consumer to use FlatBuffers deserialization for values with schema registry integration.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The FlatBuffers table type.</typeparam>
    /// <param name="options">The consumer options.</param>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    /// <param name="deserializeFunc">Function to deserialize bytes to a FlatBuffer object.</param>
    /// <param name="configure">Optional configuration action for the deserializer config.</param>
    /// <returns>The consumer options for chaining.</returns>
    public static ConsumerOptions<TKey, TValue> WithFlatBuffersValueDeserializer<TKey, TValue>(
        this ConsumerOptions<TKey, TValue> options,
        ISchemaRegistryOperations schemaRegistry,
        Func<ByteBuffer, TValue> deserializeFunc,
        Action<FlatBuffersSerializerConfig>? configure = null)
        where TValue : struct, IFlatbufferObject
    {
        var config = new FlatBuffersSerializerConfig(schemaRegistry);
        configure?.Invoke(config);
        options.AsyncValueDeserializer = new SchemaRegistryFlatBuffersDeserializer<TValue>(config, deserializeFunc);
        return options;
    }

    /// <summary>
    /// Configure the consumer to use FlatBuffers deserialization for keys with schema registry integration.
    /// </summary>
    /// <typeparam name="TKey">The FlatBuffers table type for keys.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="options">The consumer options.</param>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    /// <param name="deserializeFunc">Function to deserialize bytes to a FlatBuffer object.</param>
    /// <param name="configure">Optional configuration action for the deserializer config.</param>
    /// <returns>The consumer options for chaining.</returns>
    public static ConsumerOptions<TKey, TValue> WithFlatBuffersKeyDeserializer<TKey, TValue>(
        this ConsumerOptions<TKey, TValue> options,
        ISchemaRegistryOperations schemaRegistry,
        Func<ByteBuffer, TKey> deserializeFunc,
        Action<FlatBuffersSerializerConfig>? configure = null)
        where TKey : struct, IFlatbufferObject
    {
        var config = new FlatBuffersSerializerConfig(schemaRegistry) { IsKey = true };
        configure?.Invoke(config);
        options.AsyncKeyDeserializer = new SchemaRegistryFlatBuffersDeserializer<TKey>(config, deserializeFunc);
        return options;
    }
}
