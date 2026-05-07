namespace Confluent.Kafka;

/// <summary>
/// Action to take when there is no initial offset or the current offset no longer exists.
/// </summary>
public enum AutoOffsetReset
{
    /// <summary>Start from the earliest available offset.</summary>
    Earliest = 0,

    /// <summary>Start from the latest offset (only new messages).</summary>
    Latest = 1,

    /// <summary>Throw an exception if no offset is found.</summary>
    Error = 2
}
