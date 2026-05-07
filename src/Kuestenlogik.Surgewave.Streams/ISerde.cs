namespace Kuestenlogik.Surgewave.Streams;

/// <summary>
/// Combined serializer/deserializer interface for stream processing.
/// Used to convert between typed values and byte arrays for storage and transport.
/// </summary>
/// <typeparam name="T">The type to serialize and deserialize.</typeparam>
public interface ISerde<T>
{
    /// <summary>Serializes a value to a byte array.</summary>
    /// <param name="value">The value to serialize.</param>
    /// <returns>The serialized byte representation.</returns>
    byte[] Serialize(T value);

    /// <summary>Deserializes a byte array to a typed value.</summary>
    /// <param name="data">The byte array to deserialize.</param>
    /// <returns>The deserialized value.</returns>
    T Deserialize(byte[] data);
}
