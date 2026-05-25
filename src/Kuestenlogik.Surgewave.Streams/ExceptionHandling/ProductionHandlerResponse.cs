namespace Kuestenlogik.Surgewave.Streams.ExceptionHandling;

/// <summary>
/// Response from a production exception handler.
/// </summary>
public enum ProductionHandlerResponse
{
    /// <summary>
    /// Retry the failed production.
    /// </summary>
    Retry,

    /// <summary>
    /// Fail processing and stop the stream task.
    /// </summary>
    Fail
}
