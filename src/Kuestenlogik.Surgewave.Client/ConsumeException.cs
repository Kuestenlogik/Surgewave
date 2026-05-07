using Kuestenlogik.Surgewave.Client.Diagnostics;
using Kuestenlogik.Surgewave.Protocol.Kafka;

namespace Kuestenlogik.Surgewave.Client;

/// <summary>
/// Exception thrown when a consume operation fails.
/// </summary>
public class ConsumeException : SurgewaveClientException, IRecoverableException
{
    /// <summary>
    /// The Kafka error code returned by the broker.
    /// </summary>
    public ErrorCode ErrorCode { get; }

    /// <summary>
    /// The topic that failed to consume from.
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

    public ConsumeException() { }

    public ConsumeException(string message) : base(message)
    {
        ErrorCode = ErrorCode.Unknown;
    }

    public ConsumeException(string message, Exception innerException) : base(message, innerException)
    {
        ErrorCode = ErrorCode.Unknown;
    }

    public ConsumeException(ErrorCode errorCode, string? topic = null, int? partition = null)
        : base(FormatMessage(errorCode, topic, partition))
    {
        ErrorCode = errorCode;
        Topic = topic;
        Partition = partition;
    }

    public ConsumeException(string message, ErrorCode errorCode, string? topic = null, int? partition = null)
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
            (not null, not null) => $" from {topic}-{partition}",
            (not null, null) => $" from topic '{topic}'",
            _ => ""
        };
        return $"Consume failed{location}: {errorCode} ({(int)errorCode})";
    }
}
