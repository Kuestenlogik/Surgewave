namespace Kuestenlogik.Surgewave.Client.Serialization;

/// <summary>
/// Serializes objects of type T to byte arrays.
/// </summary>
/// <typeparam name="T">The type to serialize.</typeparam>
public interface ISerializer<in T>
{
    /// <summary>
    /// Serialize an object to a byte array.
    /// </summary>
    /// <param name="data">The object to serialize.</param>
    /// <param name="topic">The topic the data will be sent to (for context).</param>
    /// <returns>The serialized bytes, or null if data is null.</returns>
    byte[]? Serialize(T? data, string topic);
}

/// <summary>
/// Deserializes byte arrays to objects of type T.
/// </summary>
/// <typeparam name="T">The type to deserialize to.</typeparam>
public interface IDeserializer<out T>
{
    /// <summary>
    /// Deserialize a byte array to an object.
    /// </summary>
    /// <param name="data">The bytes to deserialize.</param>
    /// <param name="topic">The topic the data came from (for context).</param>
    /// <returns>The deserialized object.</returns>
    T Deserialize(ReadOnlySpan<byte> data, string topic);
}

/// <summary>
/// Async serializer for types that require async operations (e.g., schema registry lookups).
/// </summary>
public interface IAsyncSerializer<in T>
{
    /// <summary>
    /// Serialize an object to a byte array asynchronously.
    /// </summary>
    ValueTask<byte[]?> SerializeAsync(T? data, string topic, CancellationToken cancellationToken = default);
}

/// <summary>
/// Async deserializer for types that require async operations.
/// </summary>
public interface IAsyncDeserializer<T>
{
    /// <summary>
    /// Deserialize a byte array to an object asynchronously.
    /// </summary>
    ValueTask<T> DeserializeAsync(ReadOnlyMemory<byte> data, string topic, CancellationToken cancellationToken = default);
}
