namespace Kuestenlogik.Surgewave.Streams.ExceptionHandling;

/// <summary>
/// Handler for uncaught exceptions that escape all per-record exception handlers
/// and cause a stream thread to die. Controls the application-level response.
///
/// Kafka Streams equivalent: StreamsUncaughtExceptionHandler (KIP-671).
/// </summary>
public interface IStreamsUncaughtExceptionHandler
{
    /// <summary>
    /// Called when a stream thread encounters an unrecoverable exception.
    /// Returns the desired response strategy.
    /// </summary>
    /// <param name="threadName">Name of the failed thread.</param>
    /// <param name="exception">The exception that killed the thread.</param>
    /// <returns>The desired response: ReplaceThread, ShutdownClient, or ShutdownApplication.</returns>
    StreamsUncaughtExceptionResponse Handle(string threadName, Exception exception);
}
