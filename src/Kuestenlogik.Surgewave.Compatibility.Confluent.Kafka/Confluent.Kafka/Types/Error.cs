namespace Confluent.Kafka;

/// <summary>
/// Represents an error that occurred during a Kafka operation.
/// </summary>
public class Error
{
    /// <summary>
    /// Creates a new Error instance.
    /// </summary>
    public Error(ErrorCode code, string? reason = null, bool isFatal = false)
    {
        Code = code;
        Reason = reason ?? code.ToString();
        IsFatal = isFatal;
    }

    /// <summary>
    /// The error code.
    /// </summary>
    public ErrorCode Code { get; }

    /// <summary>
    /// A human-readable reason for the error.
    /// </summary>
    public string Reason { get; }

    /// <summary>
    /// Whether this error is fatal (requires client restart).
    /// </summary>
    public bool IsFatal { get; }

    /// <summary>
    /// Whether this represents an error (Code != NoError).
    /// </summary>
    public bool IsError => Code != ErrorCode.NoError;

    /// <summary>
    /// Whether this is a broker error (code &gt; 0).
    /// </summary>
    public bool IsBrokerError => (int)Code > 0;

    /// <summary>
    /// Whether this is a local error (code &lt; 0).
    /// </summary>
    public bool IsLocalError => (int)Code < 0;

    /// <inheritdoc/>
    public override string ToString() => Reason;

    /// <summary>
    /// No error.
    /// </summary>
    public static Error NoError { get; } = new(ErrorCode.NoError);
}
