using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Client.Native.Operations.Schema;

namespace Kuestenlogik.Surgewave.Schema.Registry.Serdes.Avro;

/// <summary>
/// Extension methods for configuring Avro schema registry serializers.
/// </summary>
public static class AvroSerializerExtensions
{
    /// <summary>
    /// Configure the producer to use Avro serialization for values with schema registry integration.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="options">The producer options.</param>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    /// <param name="configure">Optional configuration action for the serializer config.</param>
    /// <returns>The producer options for chaining.</returns>
    public static ProducerOptions<TKey, TValue> WithAvroValueSerializer<TKey, TValue>(
        this ProducerOptions<TKey, TValue> options,
        ISchemaRegistryOperations schemaRegistry,
        Action<AvroSerializerConfig>? configure = null)
    {
        var config = new AvroSerializerConfig(schemaRegistry);
        configure?.Invoke(config);
        options.AsyncValueSerializer = new SchemaRegistryAvroSerializer<TValue>(config);
        return options;
    }

    /// <summary>
    /// Configure the producer to use Avro serialization for keys with schema registry integration.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="options">The producer options.</param>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    /// <param name="configure">Optional configuration action for the serializer config.</param>
    /// <returns>The producer options for chaining.</returns>
    public static ProducerOptions<TKey, TValue> WithAvroKeySerializer<TKey, TValue>(
        this ProducerOptions<TKey, TValue> options,
        ISchemaRegistryOperations schemaRegistry,
        Action<AvroSerializerConfig>? configure = null)
    {
        var config = new AvroSerializerConfig(schemaRegistry) { IsKey = true };
        configure?.Invoke(config);
        options.AsyncKeySerializer = new SchemaRegistryAvroSerializer<TKey>(config);
        return options;
    }

    /// <summary>
    /// Configure the consumer to use Avro deserialization for values with schema registry integration.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="options">The consumer options.</param>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    /// <param name="configure">Optional configuration action for the deserializer config.</param>
    /// <returns>The consumer options for chaining.</returns>
    public static ConsumerOptions<TKey, TValue> WithAvroValueDeserializer<TKey, TValue>(
        this ConsumerOptions<TKey, TValue> options,
        ISchemaRegistryOperations schemaRegistry,
        Action<AvroSerializerConfig>? configure = null)
    {
        var config = new AvroSerializerConfig(schemaRegistry);
        configure?.Invoke(config);
        options.AsyncValueDeserializer = new SchemaRegistryAvroDeserializer<TValue>(config);
        return options;
    }

    /// <summary>
    /// Configure the consumer to use Avro deserialization for keys with schema registry integration.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="options">The consumer options.</param>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    /// <param name="configure">Optional configuration action for the deserializer config.</param>
    /// <returns>The consumer options for chaining.</returns>
    public static ConsumerOptions<TKey, TValue> WithAvroKeyDeserializer<TKey, TValue>(
        this ConsumerOptions<TKey, TValue> options,
        ISchemaRegistryOperations schemaRegistry,
        Action<AvroSerializerConfig>? configure = null)
    {
        var config = new AvroSerializerConfig(schemaRegistry) { IsKey = true };
        configure?.Invoke(config);
        options.AsyncKeyDeserializer = new SchemaRegistryAvroDeserializer<TKey>(config);
        return options;
    }

    /// <summary>
    /// Configure the producer to use Avro serialization for both keys and values.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="options">The producer options.</param>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    /// <param name="configure">Optional configuration action applied to both serializer configs.</param>
    /// <returns>The producer options for chaining.</returns>
    public static ProducerOptions<TKey, TValue> WithAvroSerializers<TKey, TValue>(
        this ProducerOptions<TKey, TValue> options,
        ISchemaRegistryOperations schemaRegistry,
        Action<AvroSerializerConfig>? configure = null)
    {
        return options
            .WithAvroKeySerializer(schemaRegistry, configure)
            .WithAvroValueSerializer(schemaRegistry, configure);
    }

    /// <summary>
    /// Configure the consumer to use Avro deserialization for both keys and values.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="options">The consumer options.</param>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    /// <param name="configure">Optional configuration action applied to both deserializer configs.</param>
    /// <returns>The consumer options for chaining.</returns>
    public static ConsumerOptions<TKey, TValue> WithAvroDeserializers<TKey, TValue>(
        this ConsumerOptions<TKey, TValue> options,
        ISchemaRegistryOperations schemaRegistry,
        Action<AvroSerializerConfig>? configure = null)
    {
        return options
            .WithAvroKeyDeserializer(schemaRegistry, configure)
            .WithAvroValueDeserializer(schemaRegistry, configure);
    }
}
