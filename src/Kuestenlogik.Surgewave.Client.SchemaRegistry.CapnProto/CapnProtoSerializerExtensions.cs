using Capnp;
using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Client.Native.Operations.Schema;

namespace Kuestenlogik.Surgewave.Client.SchemaRegistry.CapnProto;

/// <summary>
/// Extension methods for configuring Cap'n Proto schema registry serializers.
/// </summary>
public static class CapnProtoSerializerExtensions
{
    /// <summary>
    /// Configure the producer to use Cap'n Proto serialization for values with schema registry integration.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type. Must be a Cap'n Proto generated struct.</typeparam>
    /// <param name="options">The producer options.</param>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    /// <param name="configure">Optional configuration action for the serializer config.</param>
    /// <returns>The producer options for chaining.</returns>
    public static ProducerOptions<TKey, TValue> WithCapnProtoValueSerializer<TKey, TValue>(
        this ProducerOptions<TKey, TValue> options,
        ISchemaRegistryOperations schemaRegistry,
        Action<CapnProtoSerializerConfig>? configure = null) where TValue : class, ICapnpSerializable, new()
    {
        var config = new CapnProtoSerializerConfig(schemaRegistry);
        configure?.Invoke(config);
        options.AsyncValueSerializer = new SchemaRegistryCapnProtoSerializer<TValue>(config);
        return options;
    }

    /// <summary>
    /// Configure the producer to use Cap'n Proto serialization for keys with schema registry integration.
    /// </summary>
    /// <typeparam name="TKey">The key type. Must be a Cap'n Proto generated struct.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="options">The producer options.</param>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    /// <param name="configure">Optional configuration action for the serializer config.</param>
    /// <returns>The producer options for chaining.</returns>
    public static ProducerOptions<TKey, TValue> WithCapnProtoKeySerializer<TKey, TValue>(
        this ProducerOptions<TKey, TValue> options,
        ISchemaRegistryOperations schemaRegistry,
        Action<CapnProtoSerializerConfig>? configure = null) where TKey : class, ICapnpSerializable, new()
    {
        var config = new CapnProtoSerializerConfig(schemaRegistry) { IsKey = true };
        configure?.Invoke(config);
        options.AsyncKeySerializer = new SchemaRegistryCapnProtoSerializer<TKey>(config);
        return options;
    }

    /// <summary>
    /// Configure the consumer to use Cap'n Proto deserialization for values with schema registry integration.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type. Must be a Cap'n Proto generated struct.</typeparam>
    /// <param name="options">The consumer options.</param>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    /// <param name="configure">Optional configuration action for the deserializer config.</param>
    /// <returns>The consumer options for chaining.</returns>
    public static ConsumerOptions<TKey, TValue> WithCapnProtoValueDeserializer<TKey, TValue>(
        this ConsumerOptions<TKey, TValue> options,
        ISchemaRegistryOperations schemaRegistry,
        Action<CapnProtoSerializerConfig>? configure = null) where TValue : class, ICapnpSerializable, new()
    {
        var config = new CapnProtoSerializerConfig(schemaRegistry);
        configure?.Invoke(config);
        options.AsyncValueDeserializer = new SchemaRegistryCapnProtoDeserializer<TValue>(config);
        return options;
    }

    /// <summary>
    /// Configure the consumer to use Cap'n Proto deserialization for keys with schema registry integration.
    /// </summary>
    /// <typeparam name="TKey">The key type. Must be a Cap'n Proto generated struct.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="options">The consumer options.</param>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    /// <param name="configure">Optional configuration action for the deserializer config.</param>
    /// <returns>The consumer options for chaining.</returns>
    public static ConsumerOptions<TKey, TValue> WithCapnProtoKeyDeserializer<TKey, TValue>(
        this ConsumerOptions<TKey, TValue> options,
        ISchemaRegistryOperations schemaRegistry,
        Action<CapnProtoSerializerConfig>? configure = null) where TKey : class, ICapnpSerializable, new()
    {
        var config = new CapnProtoSerializerConfig(schemaRegistry) { IsKey = true };
        configure?.Invoke(config);
        options.AsyncKeyDeserializer = new SchemaRegistryCapnProtoDeserializer<TKey>(config);
        return options;
    }
}
