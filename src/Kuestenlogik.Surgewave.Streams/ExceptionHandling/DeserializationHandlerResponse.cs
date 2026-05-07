namespace Kuestenlogik.Surgewave.Streams.ExceptionHandling;

/// <summary>
/// Response from a deserialization exception handler.
/// </summary>
public enum DeserializationHandlerResponse
{
    /// <summary>
    /// Continue processing, skip the failed record.
    /// </summary>
    Continue,

    /// <summary>
    /// Fail processing and stop the stream task.
    /// </summary>
    Fail
}
