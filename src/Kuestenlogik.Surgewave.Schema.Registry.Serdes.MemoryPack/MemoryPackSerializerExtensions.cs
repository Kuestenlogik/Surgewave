using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Client.Native.Operations.Schema;

namespace Kuestenlogik.Surgewave.Schema.Registry.Serdes.MemoryPack;

/// <summary>
/// Extension methods for configuring MemoryPack schema registry serializers.
/// </summary>
public static class MemoryPackSerializerExtensions
{
    /// <summary>
    /// Configure the producer to use MemoryPack serialization for values with schema registry integration.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type. Must have [MemoryPackable] attribute.</typeparam>
    /// <param name="options">The producer options.</param>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    /// <param name="configure">Optional configuration action for the serializer config.</param>
    /// <returns>The producer options for chaining.</returns>
    public static ProducerOptions<TKey, TValue> WithMemoryPackValueSerializer<TKey, TValue>(
        this ProducerOptions<TKey, TValue> options,
        ISchemaRegistryOperations schemaRegistry,
        Action<MemoryPackSerializerConfig>? configure = null)
    {
        var config = new MemoryPackSerializerConfig(schemaRegistry);
        configure?.Invoke(config);
        options.AsyncValueSerializer = new SchemaRegistryMemoryPackSerializer<TValue>(config);
        return options;
    }

    /// <summary>
    /// Configure the producer to use MemoryPack serialization for keys with schema registry integration.
    /// </summary>
    /// <typeparam name="TKey">The key type. Must have [MemoryPackable] attribute.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="options">The producer options.</param>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    /// <param name="configure">Optional configuration action for the serializer config.</param>
    /// <returns>The producer options for chaining.</returns>
    public static ProducerOptions<TKey, TValue> WithMemoryPackKeySerializer<TKey, TValue>(
        this ProducerOptions<TKey, TValue> options,
        ISchemaRegistryOperations schemaRegistry,
        Action<MemoryPackSerializerConfig>? configure = null)
    {
        var config = new MemoryPackSerializerConfig(schemaRegistry) { IsKey = true };
        configure?.Invoke(config);
        options.AsyncKeySerializer = new SchemaRegistryMemoryPackSerializer<TKey>(config);
        return options;
    }

    /// <summary>
    /// Configure the consumer to use MemoryPack deserialization for values with schema registry integration.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type. Must have [MemoryPackable] attribute.</typeparam>
    /// <param name="options">The consumer options.</param>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    /// <param name="configure">Optional configuration action for the deserializer config.</param>
    /// <returns>The consumer options for chaining.</returns>
    public static ConsumerOptions<TKey, TValue> WithMemoryPackValueDeserializer<TKey, TValue>(
        this ConsumerOptions<TKey, TValue> options,
        ISchemaRegistryOperations schemaRegistry,
        Action<MemoryPackSerializerConfig>? configure = null)
    {
        var config = new MemoryPackSerializerConfig(schemaRegistry);
        configure?.Invoke(config);
        options.AsyncValueDeserializer = new SchemaRegistryMemoryPackDeserializer<TValue>(config);
        return options;
    }

    /// <summary>
    /// Configure the consumer to use MemoryPack deserialization for keys with schema registry integration.
    /// </summary>
    /// <typeparam name="TKey">The key type. Must have [MemoryPackable] attribute.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="options">The consumer options.</param>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    /// <param name="configure">Optional configuration action for the deserializer config.</param>
    /// <returns>The consumer options for chaining.</returns>
    public static ConsumerOptions<TKey, TValue> WithMemoryPackKeyDeserializer<TKey, TValue>(
        this ConsumerOptions<TKey, TValue> options,
        ISchemaRegistryOperations schemaRegistry,
        Action<MemoryPackSerializerConfig>? configure = null)
    {
        var config = new MemoryPackSerializerConfig(schemaRegistry) { IsKey = true };
        configure?.Invoke(config);
        options.AsyncKeyDeserializer = new SchemaRegistryMemoryPackDeserializer<TKey>(config);
        return options;
    }
}
