using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Client.Native.Operations.Schema;

namespace Kuestenlogik.Surgewave.Client.SchemaRegistry.Orleans;

/// <summary>
/// Extension methods for configuring Orleans schema registry serializers.
/// </summary>
public static class OrleansSerializerExtensions
{
    /// <summary>
    /// Configure the producer to use Orleans serialization for values with schema registry integration.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="options">The producer options.</param>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    /// <param name="configure">Optional configuration action for the serializer config.</param>
    /// <returns>The producer options for chaining.</returns>
    public static ProducerOptions<TKey, TValue> WithOrleansValueSerializer<TKey, TValue>(
        this ProducerOptions<TKey, TValue> options,
        ISchemaRegistryOperations schemaRegistry,
        Action<OrleansSerializerConfig>? configure = null)
    {
        var config = new OrleansSerializerConfig(schemaRegistry);
        configure?.Invoke(config);
        options.AsyncValueSerializer = new SchemaRegistryOrleansSerializer<TValue>(config);
        return options;
    }

    /// <summary>
    /// Configure the producer to use Orleans serialization for keys with schema registry integration.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="options">The producer options.</param>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    /// <param name="configure">Optional configuration action for the serializer config.</param>
    /// <returns>The producer options for chaining.</returns>
    public static ProducerOptions<TKey, TValue> WithOrleansKeySerializer<TKey, TValue>(
        this ProducerOptions<TKey, TValue> options,
        ISchemaRegistryOperations schemaRegistry,
        Action<OrleansSerializerConfig>? configure = null)
    {
        var config = new OrleansSerializerConfig(schemaRegistry) { IsKey = true };
        configure?.Invoke(config);
        options.AsyncKeySerializer = new SchemaRegistryOrleansSerializer<TKey>(config);
        return options;
    }

    /// <summary>
    /// Configure the consumer to use Orleans deserialization for values with schema registry integration.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="options">The consumer options.</param>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    /// <param name="configure">Optional configuration action for the deserializer config.</param>
    /// <returns>The consumer options for chaining.</returns>
    public static ConsumerOptions<TKey, TValue> WithOrleansValueDeserializer<TKey, TValue>(
        this ConsumerOptions<TKey, TValue> options,
        ISchemaRegistryOperations schemaRegistry,
        Action<OrleansSerializerConfig>? configure = null)
    {
        var config = new OrleansSerializerConfig(schemaRegistry);
        configure?.Invoke(config);
        options.AsyncValueDeserializer = new SchemaRegistryOrleansDeserializer<TValue>(config);
        return options;
    }

    /// <summary>
    /// Configure the consumer to use Orleans deserialization for keys with schema registry integration.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="options">The consumer options.</param>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    /// <param name="configure">Optional configuration action for the deserializer config.</param>
    /// <returns>The consumer options for chaining.</returns>
    public static ConsumerOptions<TKey, TValue> WithOrleansKeyDeserializer<TKey, TValue>(
        this ConsumerOptions<TKey, TValue> options,
        ISchemaRegistryOperations schemaRegistry,
        Action<OrleansSerializerConfig>? configure = null)
    {
        var config = new OrleansSerializerConfig(schemaRegistry) { IsKey = true };
        configure?.Invoke(config);
        options.AsyncKeyDeserializer = new SchemaRegistryOrleansDeserializer<TKey>(config);
        return options;
    }
}
