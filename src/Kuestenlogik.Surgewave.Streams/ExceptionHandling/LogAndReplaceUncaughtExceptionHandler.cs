using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Streams.ExceptionHandling;

/// <summary>
/// Default handler: logs the exception and replaces the dead thread.
/// Most resilient strategy — production recommended.
/// </summary>
public sealed class LogAndReplaceUncaughtExceptionHandler : IStreamsUncaughtExceptionHandler
{
    public static readonly LogAndReplaceUncaughtExceptionHandler Instance = new();

    public StreamsUncaughtExceptionResponse Handle(string threadName, Exception exception)
    {
        return StreamsUncaughtExceptionResponse.ReplaceThread;
    }
}

/// <summary>
/// Logs the exception and shuts down this client instance (all threads).
/// Other application components continue running.
/// </summary>
public sealed class LogAndShutdownClientHandler : IStreamsUncaughtExceptionHandler
{
    public static readonly LogAndShutdownClientHandler Instance = new();

    public StreamsUncaughtExceptionResponse Handle(string threadName, Exception exception)
    {
        return StreamsUncaughtExceptionResponse.ShutdownClient;
    }
}

/// <summary>
/// Logs the exception and shuts down the entire application.
/// </summary>
public sealed class LogAndShutdownApplicationHandler : IStreamsUncaughtExceptionHandler
{
    public static readonly LogAndShutdownApplicationHandler Instance = new();

    public StreamsUncaughtExceptionResponse Handle(string threadName, Exception exception)
    {
        return StreamsUncaughtExceptionResponse.ShutdownApplication;
    }
}
