using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Client.Native.Operations.Schema;

namespace Kuestenlogik.Surgewave.Schema.Registry.Serdes.Hyperion;

/// <summary>
/// Extension methods for configuring Hyperion schema registry serializers.
/// </summary>
public static class HyperionSerializerExtensions
{
    /// <summary>
    /// Configure the producer to use Hyperion serialization for values with schema registry integration.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="options">The producer options.</param>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    /// <param name="configure">Optional configuration action for the serializer config.</param>
    /// <returns>The producer options for chaining.</returns>
    public static ProducerOptions<TKey, TValue> WithHyperionValueSerializer<TKey, TValue>(
        this ProducerOptions<TKey, TValue> options,
        ISchemaRegistryOperations schemaRegistry,
        Action<HyperionSerializerConfig>? configure = null)
    {
        var config = new HyperionSerializerConfig(schemaRegistry);
        configure?.Invoke(config);
        options.AsyncValueSerializer = new SchemaRegistryHyperionSerializer<TValue>(config);
        return options;
    }

    /// <summary>
    /// Configure the producer to use Hyperion serialization for keys with schema registry integration.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="options">The producer options.</param>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    /// <param name="configure">Optional configuration action for the serializer config.</param>
    /// <returns>The producer options for chaining.</returns>
    public static ProducerOptions<TKey, TValue> WithHyperionKeySerializer<TKey, TValue>(
        this ProducerOptions<TKey, TValue> options,
        ISchemaRegistryOperations schemaRegistry,
        Action<HyperionSerializerConfig>? configure = null)
    {
        var config = new HyperionSerializerConfig(schemaRegistry) { IsKey = true };
        configure?.Invoke(config);
        options.AsyncKeySerializer = new SchemaRegistryHyperionSerializer<TKey>(config);
        return options;
    }

    /// <summary>
    /// Configure the consumer to use Hyperion deserialization for values with schema registry integration.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="options">The consumer options.</param>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    /// <param name="configure">Optional configuration action for the deserializer config.</param>
    /// <returns>The consumer options for chaining.</returns>
    public static ConsumerOptions<TKey, TValue> WithHyperionValueDeserializer<TKey, TValue>(
        this ConsumerOptions<TKey, TValue> options,
        ISchemaRegistryOperations schemaRegistry,
        Action<HyperionSerializerConfig>? configure = null)
    {
        var config = new HyperionSerializerConfig(schemaRegistry);
        configure?.Invoke(config);
        options.AsyncValueDeserializer = new SchemaRegistryHyperionDeserializer<TValue>(config);
        return options;
    }

    /// <summary>
    /// Configure the consumer to use Hyperion deserialization for keys with schema registry integration.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="options">The consumer options.</param>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    /// <param name="configure">Optional configuration action for the deserializer config.</param>
    /// <returns>The consumer options for chaining.</returns>
    public static ConsumerOptions<TKey, TValue> WithHyperionKeyDeserializer<TKey, TValue>(
        this ConsumerOptions<TKey, TValue> options,
        ISchemaRegistryOperations schemaRegistry,
        Action<HyperionSerializerConfig>? configure = null)
    {
        var config = new HyperionSerializerConfig(schemaRegistry) { IsKey = true };
        configure?.Invoke(config);
        options.AsyncKeyDeserializer = new SchemaRegistryHyperionDeserializer<TKey>(config);
        return options;
    }
}
