namespace Confluent.Kafka;

/// <summary>
/// Defines a deserializer for message keys or values.
/// </summary>
/// <typeparam name="T">The type to deserialize to.</typeparam>
public interface IDeserializer<out T>
{
    /// <summary>
    /// Deserialize a byte array to an instance of type T.
    /// </summary>
    /// <param name="data">The data to deserialize.</param>
    /// <param name="isNull">Whether the data is null.</param>
    /// <param name="context">The deserialization context.</param>
    /// <returns>The deserialized data.</returns>
    T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context);
}

/// <summary>
/// Async deserializer interface for schema registry integration.
/// </summary>
/// <typeparam name="T">The type to deserialize to.</typeparam>
public interface IAsyncDeserializer<T>
{
    /// <summary>
    /// Deserialize a byte array to an instance of type T asynchronously.
    /// </summary>
    Task<T> DeserializeAsync(ReadOnlyMemory<byte> data, bool isNull, SerializationContext context);
}
