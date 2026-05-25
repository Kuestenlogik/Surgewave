namespace Kuestenlogik.Surgewave.Protocol.Amqp;

/// <summary>
/// Supported AMQP exchange types that control how messages are routed to queues.
/// </summary>
public enum AmqpExchangeType
{
    /// <summary>
    /// Direct exchange: routes messages to queues whose binding key exactly
    /// matches the message routing key.
    /// </summary>
    Direct,

    /// <summary>
    /// Fanout exchange: routes messages to all bound queues, ignoring routing key.
    /// </summary>
    Fanout,

    /// <summary>
    /// Topic exchange: routes messages to queues whose binding pattern matches
    /// the routing key using '*' (one word) and '#' (zero or more words) wildcards.
    /// </summary>
    Topic,

    /// <summary>
    /// Headers exchange: routes based on message header attributes (not fully
    /// implemented — treated as direct routing).
    /// </summary>
    Headers,
}
