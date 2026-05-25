namespace Kuestenlogik.Surgewave.Streams.ExceptionHandling;

/// <summary>
/// Response from a processing exception handler.
/// </summary>
public enum ProcessingHandlerResponse
{
    /// <summary>
    /// Skip the failed record and continue processing.
    /// </summary>
    Skip,

    /// <summary>
    /// Fail processing and stop the stream task.
    /// </summary>
    Fail
}
