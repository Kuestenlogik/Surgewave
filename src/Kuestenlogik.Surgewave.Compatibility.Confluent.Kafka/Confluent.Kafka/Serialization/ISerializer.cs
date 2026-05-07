namespace Confluent.Kafka;

/// <summary>
/// Defines a serializer for message keys or values.
/// </summary>
/// <typeparam name="T">The type to serialize.</typeparam>
public interface ISerializer<in T>
{
    /// <summary>
    /// Serialize an instance of type T to a byte array.
    /// </summary>
    /// <param name="data">The data to serialize.</param>
    /// <param name="context">The serialization context.</param>
    /// <returns>The serialized data, or null if data is null.</returns>
    byte[]? Serialize(T data, SerializationContext context);
}

/// <summary>
/// Async serializer interface for schema registry integration.
/// </summary>
/// <typeparam name="T">The type to serialize.</typeparam>
public interface IAsyncSerializer<in T>
{
    /// <summary>
    /// Serialize an instance of type T to a byte array asynchronously.
    /// </summary>
    Task<byte[]?> SerializeAsync(T data, SerializationContext context);
}

/// <summary>
/// Context information for serialization.
/// </summary>
public readonly struct SerializationContext
{
    /// <summary>
    /// Creates a new SerializationContext.
    /// </summary>
    public SerializationContext(MessageComponentType component, string topic, Headers? headers = null)
    {
        Component = component;
        Topic = topic;
        Headers = headers ?? new Headers();
    }

    /// <summary>
    /// The component being serialized (key or value).
    /// </summary>
    public MessageComponentType Component { get; }

    /// <summary>
    /// The topic the data will be sent to.
    /// </summary>
    public string Topic { get; }

    /// <summary>
    /// Message headers (may be modified during serialization).
    /// </summary>
    public Headers Headers { get; }
}

/// <summary>
/// Message component type.
/// </summary>
public enum MessageComponentType
{
    /// <summary>Message key.</summary>
    Key = 0,

    /// <summary>Message value.</summary>
    Value = 1
}
