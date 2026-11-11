namespace Kuestenlogik.Surgewave.Core.Exceptions;

/// <summary>
/// A partition log cannot be served because its log directory is offline (#162) — Kafka's
/// <c>KafkaStorageException</c>, answered on the wire as <c>KAFKA_STORAGE_ERROR</c>.
/// </summary>
/// <remarks>
/// An <see cref="IOException"/>, so a caller that already handles I/O failures keeps handling this one. Thrown for the
/// operation that took the directory offline, wrapping the I/O failure, and for every later operation on a log in that
/// directory: a failed directory stays offline until the broker restarts, as in Kafka.
/// </remarks>
public sealed class LogDirOfflineException : IOException
{
    public LogDirOfflineException()
        : base("The log directory is offline")
    {
    }

    public LogDirOfflineException(string message)
        : base(message)
    {
    }

    public LogDirOfflineException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>The id of the offline log directory.</summary>
    public Guid DirectoryId { get; init; }
}
