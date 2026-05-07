namespace Confluent.Kafka;

/// <summary>
/// Base exception for Kafka operations.
/// </summary>
public class KafkaException : Exception
{
    /// <summary>
    /// Creates a new KafkaException.
    /// </summary>
    public KafkaException(Error error)
        : base(error.Reason)
    {
        Error = error;
    }

    /// <summary>
    /// Creates a new KafkaException with an inner exception.
    /// </summary>
    public KafkaException(Error error, Exception innerException)
        : base(error.Reason, innerException)
    {
        Error = error;
    }

    /// <summary>
    /// The error associated with this exception.
    /// </summary>
    public Error Error { get; }
}

/// <summary>
/// Exception thrown when a produce operation fails.
/// </summary>
public class ProduceException<TKey, TValue> : KafkaException
{
    /// <summary>
    /// Creates a new ProduceException.
    /// </summary>
    public ProduceException(Error error, DeliveryResult<TKey, TValue> deliveryResult)
        : base(error)
    {
        DeliveryResult = deliveryResult;
    }

    /// <summary>
    /// The delivery result associated with this exception.
    /// </summary>
    public DeliveryResult<TKey, TValue> DeliveryResult { get; }
}

/// <summary>
/// Exception thrown when a consume operation fails.
/// </summary>
public class ConsumeException : KafkaException
{
    /// <summary>
    /// Creates a new ConsumeException.
    /// </summary>
    public ConsumeException(ConsumeResult<byte[], byte[]> consumeResult)
        : base(new Error(ErrorCode.Unknown, "Consume failed"))
    {
        ConsumptionResult = consumeResult;
    }

    /// <summary>
    /// Creates a new ConsumeException with a specific error.
    /// </summary>
    public ConsumeException(Error error)
        : base(error)
    {
        ConsumptionResult = null;
    }

    /// <summary>
    /// The consume result associated with this exception.
    /// </summary>
    public ConsumeResult<byte[], byte[]>? ConsumptionResult { get; }
}

/// <summary>
/// Exception thrown when message serialization fails.
/// </summary>
public class SerializationException : KafkaException
{
    /// <summary>
    /// Creates a new SerializationException.
    /// </summary>
    public SerializationException(string message)
        : base(new Error(ErrorCode.Local_InvalidArg, message))
    {
    }

    /// <summary>
    /// Creates a new SerializationException with an inner exception.
    /// </summary>
    public SerializationException(string message, Exception innerException)
        : base(new Error(ErrorCode.Local_InvalidArg, message), innerException)
    {
    }
}

/// <summary>
/// Exception thrown when message deserialization fails.
/// </summary>
public class DeserializationException : KafkaException
{
    /// <summary>
    /// Creates a new DeserializationException.
    /// </summary>
    public DeserializationException(string message)
        : base(new Error(ErrorCode.Local_InvalidArg, message))
    {
    }

    /// <summary>
    /// Creates a new DeserializationException with an inner exception.
    /// </summary>
    public DeserializationException(string message, Exception innerException)
        : base(new Error(ErrorCode.Local_InvalidArg, message), innerException)
    {
    }
}
