using Google.Protobuf;
using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Client.Native.Operations.Schema;

namespace Kuestenlogik.Surgewave.Schema.Registry.Serdes.Protobuf;

/// <summary>
/// Extension methods for configuring Protobuf schema registry serializers.
/// </summary>
public static class ProtobufSerializerExtensions
{
    /// <summary>
    /// Configure the producer to use Protobuf serialization for values with schema registry integration.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The Protobuf message type.</typeparam>
    /// <param name="options">The producer options.</param>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    /// <param name="configure">Optional configuration action for the serializer config.</param>
    /// <returns>The producer options for chaining.</returns>
    public static ProducerOptions<TKey, TValue> WithProtobufValueSerializer<TKey, TValue>(
        this ProducerOptions<TKey, TValue> options,
        ISchemaRegistryOperations schemaRegistry,
        Action<ProtobufSerializerConfig>? configure = null)
        where TValue : IMessage<TValue>
    {
        var config = new ProtobufSerializerConfig(schemaRegistry);
        configure?.Invoke(config);
        options.AsyncValueSerializer = new SchemaRegistryProtobufSerializer<TValue>(config);
        return options;
    }

    /// <summary>
    /// Configure the producer to use Protobuf serialization for keys with schema registry integration.
    /// </summary>
    /// <typeparam name="TKey">The Protobuf message type for keys.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="options">The producer options.</param>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    /// <param name="configure">Optional configuration action for the serializer config.</param>
    /// <returns>The producer options for chaining.</returns>
    public static ProducerOptions<TKey, TValue> WithProtobufKeySerializer<TKey, TValue>(
        this ProducerOptions<TKey, TValue> options,
        ISchemaRegistryOperations schemaRegistry,
        Action<ProtobufSerializerConfig>? configure = null)
        where TKey : IMessage<TKey>
    {
        var config = new ProtobufSerializerConfig(schemaRegistry) { IsKey = true };
        configure?.Invoke(config);
        options.AsyncKeySerializer = new SchemaRegistryProtobufSerializer<TKey>(config);
        return options;
    }

    /// <summary>
    /// Configure the consumer to use Protobuf deserialization for values with schema registry integration.
    /// </summary>
    /// <typeparam name="TKey">The key type.</typeparam>
    /// <typeparam name="TValue">The Protobuf message type.</typeparam>
    /// <param name="options">The consumer options.</param>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    /// <param name="configure">Optional configuration action for the deserializer config.</param>
    /// <returns>The consumer options for chaining.</returns>
    public static ConsumerOptions<TKey, TValue> WithProtobufValueDeserializer<TKey, TValue>(
        this ConsumerOptions<TKey, TValue> options,
        ISchemaRegistryOperations schemaRegistry,
        Action<ProtobufSerializerConfig>? configure = null)
        where TValue : IMessage<TValue>, new()
    {
        var config = new ProtobufSerializerConfig(schemaRegistry);
        configure?.Invoke(config);
        options.AsyncValueDeserializer = new SchemaRegistryProtobufDeserializer<TValue>(config);
        return options;
    }

    /// <summary>
    /// Configure the consumer to use Protobuf deserialization for keys with schema registry integration.
    /// </summary>
    /// <typeparam name="TKey">The Protobuf message type for keys.</typeparam>
    /// <typeparam name="TValue">The value type.</typeparam>
    /// <param name="options">The consumer options.</param>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    /// <param name="configure">Optional configuration action for the deserializer config.</param>
    /// <returns>The consumer options for chaining.</returns>
    public static ConsumerOptions<TKey, TValue> WithProtobufKeyDeserializer<TKey, TValue>(
        this ConsumerOptions<TKey, TValue> options,
        ISchemaRegistryOperations schemaRegistry,
        Action<ProtobufSerializerConfig>? configure = null)
        where TKey : IMessage<TKey>, new()
    {
        var config = new ProtobufSerializerConfig(schemaRegistry) { IsKey = true };
        configure?.Invoke(config);
        options.AsyncKeyDeserializer = new SchemaRegistryProtobufDeserializer<TKey>(config);
        return options;
    }

    /// <summary>
    /// Configure the producer to use Protobuf serialization for both keys and values.
    /// </summary>
    /// <typeparam name="TKey">The Protobuf message type for keys.</typeparam>
    /// <typeparam name="TValue">The Protobuf message type for values.</typeparam>
    /// <param name="options">The producer options.</param>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    /// <param name="configure">Optional configuration action applied to both serializer configs.</param>
    /// <returns>The producer options for chaining.</returns>
    public static ProducerOptions<TKey, TValue> WithProtobufSerializers<TKey, TValue>(
        this ProducerOptions<TKey, TValue> options,
        ISchemaRegistryOperations schemaRegistry,
        Action<ProtobufSerializerConfig>? configure = null)
        where TKey : IMessage<TKey>
        where TValue : IMessage<TValue>
    {
        return options
            .WithProtobufKeySerializer(schemaRegistry, configure)
            .WithProtobufValueSerializer(schemaRegistry, configure);
    }

    /// <summary>
    /// Configure the consumer to use Protobuf deserialization for both keys and values.
    /// </summary>
    /// <typeparam name="TKey">The Protobuf message type for keys.</typeparam>
    /// <typeparam name="TValue">The Protobuf message type for values.</typeparam>
    /// <param name="options">The consumer options.</param>
    /// <param name="schemaRegistry">The schema registry operations client.</param>
    /// <param name="configure">Optional configuration action applied to both deserializer configs.</param>
    /// <returns>The consumer options for chaining.</returns>
    public static ConsumerOptions<TKey, TValue> WithProtobufDeserializers<TKey, TValue>(
        this ConsumerOptions<TKey, TValue> options,
        ISchemaRegistryOperations schemaRegistry,
        Action<ProtobufSerializerConfig>? configure = null)
        where TKey : IMessage<TKey>, new()
        where TValue : IMessage<TValue>, new()
    {
        return options
            .WithProtobufKeyDeserializer(schemaRegistry, configure)
            .WithProtobufValueDeserializer(schemaRegistry, configure);
    }
}
