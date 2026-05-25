namespace Kuestenlogik.Surgewave.Streams.ExceptionHandling;

/// <summary>
/// Determines the application behavior when a stream thread encounters
/// an uncaught (fatal) exception that escapes all per-record handlers.
/// Equivalent to Kafka Streams' StreamsUncaughtExceptionHandler.
/// </summary>
public enum StreamsUncaughtExceptionResponse
{
    /// <summary>
    /// Replace the failed thread with a new one and continue processing.
    /// The dead thread's partitions are reassigned to the new thread.
    /// Best for transient failures (e.g., OOM, network blip).
    /// </summary>
    ReplaceThread,

    /// <summary>
    /// Shut down all threads for this StreamsApplication instance,
    /// but do NOT call Environment.Exit. Other parts of the application continue.
    /// Best for per-instance isolation in multi-app deployments.
    /// </summary>
    ShutdownClient,

    /// <summary>
    /// Shut down the entire application gracefully.
    /// Invokes CloseAsync, fires ShutdownStarted/ShutdownCompleted events.
    /// Best for single-app deployments where recovery requires a restart.
    /// </summary>
    ShutdownApplication
}
