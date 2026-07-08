using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Client.Native.Operations.Schema;

namespace Kuestenlogik.Surgewave.Schema.Registry.Serdes.Bond;

/// <summary>
/// Extension methods for configuring Bond schema registry serializers.
/// </summary>
public static class BondSerializerExtensions
{
    /// <summary>
    /// Configure the producer to use Bond serialization for values with schema registry integration.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="options">The producer options.</param>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    /// <param name="configure">Optional configuration action for the serializer config.</param>
    /// <returns>The producer options for chaining.</returns>
    public static ProducerOptions<TKey, TValue> WithBondValueSerializer<TKey, TValue>(
        this ProducerOptions<TKey, TValue> options,
        ISchemaRegistryOperations schemaRegistry,
        Action<BondSerializerConfig>? configure = null)
    {
        var config = new BondSerializerConfig(schemaRegistry);
        configure?.Invoke(config);
        options.AsyncValueSerializer = new SchemaRegistryBondSerializer<TValue>(config);
        return options;
    }

    /// <summary>
    /// Configure the producer to use Bond serialization for keys with schema registry integration.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="options">The producer options.</param>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    /// <param name="configure">Optional configuration action for the serializer config.</param>
    /// <returns>The producer options for chaining.</returns>
    public static ProducerOptions<TKey, TValue> WithBondKeySerializer<TKey, TValue>(
        this ProducerOptions<TKey, TValue> options,
        ISchemaRegistryOperations schemaRegistry,
        Action<BondSerializerConfig>? configure = null)
    {
        var config = new BondSerializerConfig(schemaRegistry) { IsKey = true };
        configure?.Invoke(config);
        options.AsyncKeySerializer = new SchemaRegistryBondSerializer<TKey>(config);
        return options;
    }

    /// <summary>
    /// Configure the consumer to use Bond deserialization for values with schema registry integration.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="options">The consumer options.</param>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    /// <param name="configure">Optional configuration action for the deserializer config.</param>
    /// <returns>The consumer options for chaining.</returns>
    public static ConsumerOptions<TKey, TValue> WithBondValueDeserializer<TKey, TValue>(
        this ConsumerOptions<TKey, TValue> options,
        ISchemaRegistryOperations schemaRegistry,
        Action<BondSerializerConfig>? configure = null)
    {
        var config = new BondSerializerConfig(schemaRegistry);
        configure?.Invoke(config);
        options.AsyncValueDeserializer = new SchemaRegistryBondDeserializer<TValue>(config);
        return options;
    }

    /// <summary>
    /// Configure the consumer to use Bond deserialization for keys with schema registry integration.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="options">The consumer options.</param>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    /// <param name="configure">Optional configuration action for the deserializer config.</param>
    /// <returns>The consumer options for chaining.</returns>
    public static ConsumerOptions<TKey, TValue> WithBondKeyDeserializer<TKey, TValue>(
        this ConsumerOptions<TKey, TValue> options,
        ISchemaRegistryOperations schemaRegistry,
        Action<BondSerializerConfig>? configure = null)
    {
        var config = new BondSerializerConfig(schemaRegistry) { IsKey = true };
        configure?.Invoke(config);
        options.AsyncKeyDeserializer = new SchemaRegistryBondDeserializer<TKey>(config);
        return options;
    }
}
