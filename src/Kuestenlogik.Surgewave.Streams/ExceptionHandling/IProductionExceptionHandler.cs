namespace Kuestenlogik.Surgewave.Streams.ExceptionHandling;

/// <summary>
/// Handler for production exceptions during stream processing.
/// </summary>
public interface IProductionExceptionHandler
{
    /// <summary>
    /// Handle a production exception.
    /// </summary>
    /// <param name="topic">The topic the record was being produced to.</param>
    /// <param name="key">The serialized key bytes.</param>
    /// <param name="value">The serialized value bytes.</param>
    /// <param name="exception">The exception that occurred.</param>
    /// <returns>The response indicating how to proceed.</returns>
    ProductionHandlerResponse Handle(
        string topic,
        byte[]? key,
        byte[]? value,
        Exception exception);
}
