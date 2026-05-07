namespace Kuestenlogik.Surgewave.Client.Abstractions;

/// <summary>
/// Status of a message delivery.
/// </summary>
public enum DeliveryStatus
{
    /// <summary>
    /// Message was successfully delivered and acknowledged.
    /// </summary>
    Success,

    /// <summary>
    /// Message delivery failed.
    /// </summary>
    Error
}

/// <summary>
/// Report of a message delivery attempt.
/// </summary>
/// <typeparam name="TKey">The key type.</typeparam>
/// <typeparam name="TValue">The value type.</typeparam>
public sealed record DeliveryReport<TKey, TValue>
{
    /// <summary>
    /// The delivery status.
    /// </summary>
    public required DeliveryStatus Status { get; init; }

    /// <summary>
    /// The topic the message was produced to.
    /// </summary>
    public required string Topic { get; init; }

    /// <summary>
    /// The partition the message was produced to. -1 if unknown (on error before partition assignment).
    /// </summary>
    public int Partition { get; init; } = -1;

    /// <summary>
    /// The offset of the produced message. -1 if unknown (on error).
    /// </summary>
    public long Offset { get; init; } = -1;

    /// <summary>
    /// The timestamp of the produced message.
    /// </summary>
    public DateTimeOffset Timestamp { get; init; }

    /// <summary>
    /// The original message key.
    /// </summary>
    public TKey? Key { get; init; }

    /// <summary>
    /// The original message value.
    /// </summary>
    public TValue? Value { get; init; }

    /// <summary>
    /// The error that occurred during delivery, if any.
    /// </summary>
    public Exception? Error { get; init; }

    /// <summary>
    /// Message headers that were sent with the message.
    /// </summary>
    public IReadOnlyDictionary<string, byte[]>? Headers { get; init; }

    /// <summary>
    /// Creates a success delivery report from a produce result.
    /// </summary>
    public static DeliveryReport<TKey, TValue> Success(
        ProduceResult result,
        TKey? key,
        TValue? value,
        IReadOnlyDictionary<string, byte[]>? headers = null)
    {
        return new DeliveryReport<TKey, TValue>
        {
            Status = DeliveryStatus.Success,
            Topic = result.Topic,
            Partition = result.Partition,
            Offset = result.Offset,
            Timestamp = result.Timestamp,
            Key = key,
            Value = value,
            Headers = headers
        };
    }

    /// <summary>
    /// Creates an error delivery report.
    /// </summary>
    public static DeliveryReport<TKey, TValue> Failed(
        string topic,
        TKey? key,
        TValue? value,
        Exception error,
        IReadOnlyDictionary<string, byte[]>? headers = null)
    {
        return new DeliveryReport<TKey, TValue>
        {
            Status = DeliveryStatus.Error,
            Topic = topic,
            Partition = -1,
            Offset = -1,
            Timestamp = DateTimeOffset.UtcNow,
            Key = key,
            Value = value,
            Error = error,
            Headers = headers
        };
    }
}

/// <summary>
/// Delegate for handling delivery reports.
/// </summary>
/// <typeparam name="TKey">The key type.</typeparam>
/// <typeparam name="TValue">The value type.</typeparam>
/// <param name="report">The delivery report.</param>
public delegate void DeliveryHandler<TKey, TValue>(DeliveryReport<TKey, TValue> report);
