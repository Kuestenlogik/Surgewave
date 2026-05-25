namespace Kuestenlogik.Surgewave.Streams.ExceptionHandling;

/// <summary>
/// Handler for deserialization exceptions during stream processing.
/// </summary>
public interface IDeserializationExceptionHandler
{
    /// <summary>
    /// Handle a deserialization exception.
    /// </summary>
    /// <param name="topic">The topic the record came from.</param>
    /// <param name="partition">The partition the record came from.</param>
    /// <param name="offset">The offset of the record.</param>
    /// <param name="rawKey">The raw key bytes, or null if deserialization failed before key parsing.</param>
    /// <param name="rawValue">The raw value bytes, or null if deserialization failed before value parsing.</param>
    /// <param name="exception">The exception that occurred.</param>
    /// <returns>The response indicating how to proceed.</returns>
    DeserializationHandlerResponse Handle(
        string topic,
        int partition,
        long offset,
        byte[]? rawKey,
        byte[]? rawValue,
        Exception exception);
}
