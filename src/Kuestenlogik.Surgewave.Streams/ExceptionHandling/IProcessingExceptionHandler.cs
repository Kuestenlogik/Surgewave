namespace Kuestenlogik.Surgewave.Streams.ExceptionHandling;

/// <summary>
/// Handler for processing exceptions during stream processing.
/// </summary>
public interface IProcessingExceptionHandler
{
    /// <summary>
    /// Handle a processing exception.
    /// </summary>
    /// <param name="topic">The source topic of the record.</param>
    /// <param name="partition">The partition of the record.</param>
    /// <param name="offset">The offset of the record.</param>
    /// <param name="key">The deserialized key, or null if not available.</param>
    /// <param name="value">The deserialized value, or null if not available.</param>
    /// <param name="exception">The exception that occurred.</param>
    /// <returns>The response indicating how to proceed.</returns>
    ProcessingHandlerResponse Handle(
        string topic,
        int partition,
        long offset,
        object? key,
        object? value,
        Exception exception);
}
