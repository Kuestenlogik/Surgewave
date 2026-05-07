using Kuestenlogik.Surgewave.Client.Diagnostics;
using Kuestenlogik.Surgewave.Protocol.Kafka;

namespace Kuestenlogik.Surgewave.Client;

/// <summary>
/// Exception thrown when a produce operation fails.
/// </summary>
public class ProduceException : SurgewaveClientException, IRecoverableException
{
    /// <summary>
    /// The Kafka error code returned by the broker.
    /// </summary>
    public ErrorCode ErrorCode { get; }

    /// <summary>
    /// The topic that failed to produce to.
    /// </summary>
    public string? Topic { get; }

    /// <summary>
    /// The partition that failed.
    /// </summary>
    public int? Partition { get; }

    /// <summary>
    /// Gets a suggestion for how to recover from this error.
    /// </summary>
    public string? RecoverySuggestion => Diagnostics.RecoverySuggestion.ForErrorCode(ErrorCode);

    public ProduceException() { }

    public ProduceException(string message) : base(message)
    {
        ErrorCode = ErrorCode.Unknown;
    }

    public ProduceException(string message, Exception innerException) : base(message, innerException)
    {
        ErrorCode = ErrorCode.Unknown;
    }

    public ProduceException(ErrorCode errorCode, string? topic = null, int? partition = null)
        : base(FormatMessage(errorCode, topic, partition))
    {
        ErrorCode = errorCode;
        Topic = topic;
        Partition = partition;
    }

    public ProduceException(string message, ErrorCode errorCode, string? topic = null, int? partition = null)
        : base(message)
    {
        ErrorCode = errorCode;
        Topic = topic;
        Partition = partition;
    }

    private static string FormatMessage(ErrorCode errorCode, string? topic, int? partition)
    {
        var location = (topic, partition) switch
        {
            (not null, not null) => $" for {topic}-{partition}",
            (not null, null) => $" for topic '{topic}'",
            _ => ""
        };
        return $"Produce failed{location}: {errorCode} ({(int)errorCode})";
    }
}
