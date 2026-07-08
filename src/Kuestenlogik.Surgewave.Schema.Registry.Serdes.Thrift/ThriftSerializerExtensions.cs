using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Client.Native.Operations.Schema;

namespace Kuestenlogik.Surgewave.Schema.Registry.Serdes.Thrift;

/// <summary>
/// Extension methods for configuring Thrift schema registry serializers.
/// </summary>
public static class ThriftSerializerExtensions
{
    /// <summary>
    /// Configure the producer to use Thrift serialization for values with schema registry integration.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type. Must implement TBase.</typeparam>
    /// <param name="options">The producer options.</param>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    /// <param name="configure">Optional configuration action for the serializer config.</param>
    /// <returns>The producer options for chaining.</returns>
    public static ProducerOptions<TKey, TValue> WithThriftValueSerializer<TKey, TValue>(
        this ProducerOptions<TKey, TValue> options,
        ISchemaRegistryOperations schemaRegistry,
        Action<ThriftSerializerConfig>? configure = null) where TValue : global::Thrift.Protocol.TBase
    {
        var config = new ThriftSerializerConfig(schemaRegistry);
        configure?.Invoke(config);
        options.AsyncValueSerializer = new SchemaRegistryThriftSerializer<TValue>(config);
        return options;
    }

    /// <summary>
    /// Configure the producer to use Thrift serialization for keys with schema registry integration.
    /// </summary>
    /// <typeparam name="TKey">The key type. Must implement TBase.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="options">The producer options.</param>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    /// <param name="configure">Optional configuration action for the serializer config.</param>
    /// <returns>The producer options for chaining.</returns>
    public static ProducerOptions<TKey, TValue> WithThriftKeySerializer<TKey, TValue>(
        this ProducerOptions<TKey, TValue> options,
        ISchemaRegistryOperations schemaRegistry,
        Action<ThriftSerializerConfig>? configure = null) where TKey : global::Thrift.Protocol.TBase
    {
        var config = new ThriftSerializerConfig(schemaRegistry) { IsKey = true };
        configure?.Invoke(config);
        options.AsyncKeySerializer = new SchemaRegistryThriftSerializer<TKey>(config);
        return options;
    }

    /// <summary>
    /// Configure the consumer to use Thrift deserialization for values with schema registry integration.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type. Must implement TBase and have a parameterless constructor.</typeparam>
    /// <param name="options">The consumer options.</param>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    /// <param name="configure">Optional configuration action for the deserializer config.</param>
    /// <returns>The consumer options for chaining.</returns>
    public static ConsumerOptions<TKey, TValue> WithThriftValueDeserializer<TKey, TValue>(
        this ConsumerOptions<TKey, TValue> options,
        ISchemaRegistryOperations schemaRegistry,
        Action<ThriftSerializerConfig>? configure = null) where TValue : global::Thrift.Protocol.TBase, new()
    {
        var config = new ThriftSerializerConfig(schemaRegistry);
        configure?.Invoke(config);
        options.AsyncValueDeserializer = new SchemaRegistryThriftDeserializer<TValue>(config);
        return options;
    }

    /// <summary>
    /// Configure the consumer to use Thrift deserialization for keys with schema registry integration.
    /// </summary>
    /// <typeparam name="TKey">The key type. Must implement TBase and have a parameterless constructor.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="options">The consumer options.</param>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    /// <param name="configure">Optional configuration action for the deserializer config.</param>
    /// <returns>The consumer options for chaining.</returns>
    public static ConsumerOptions<TKey, TValue> WithThriftKeyDeserializer<TKey, TValue>(
        this ConsumerOptions<TKey, TValue> options,
        ISchemaRegistryOperations schemaRegistry,
        Action<ThriftSerializerConfig>? configure = null) where TKey : global::Thrift.Protocol.TBase, new()
    {
        var config = new ThriftSerializerConfig(schemaRegistry) { IsKey = true };
        configure?.Invoke(config);
        options.AsyncKeyDeserializer = new SchemaRegistryThriftDeserializer<TKey>(config);
        return options;
    }
}
