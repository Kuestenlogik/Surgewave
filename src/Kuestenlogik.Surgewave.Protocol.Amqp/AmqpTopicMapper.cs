namespace Kuestenlogik.Surgewave.Protocol.Amqp;

/// <summary>
/// Maps AMQP exchange/routing-key pairs to Surgewave topic names and AMQP queues
/// to Surgewave consumer-group names.
/// </summary>
/// <remarks>
/// Mapping rules by exchange type:
/// <list type="bullet">
///   <item><b>Direct</b>  — routing key becomes the Surgewave topic name directly.</item>
///   <item><b>Fanout</b>  — exchange name becomes the Surgewave topic; all bound queues receive every message.</item>
///   <item><b>Topic</b>   — routing key patterns (*.orders.#) are matched with <see cref="GlobMatcher"/>.</item>
///   <item><b>Headers</b> — falls back to direct-style routing on the routing key.</item>
/// </list>
/// Queue names are used as Surgewave consumer-group names so that each AMQP queue
/// gets its own independent read offset.
/// </remarks>
public sealed class AmqpTopicMapper
{
    /// <summary>
    /// Maps an AMQP exchange + routing key to a Surgewave topic name.
    /// </summary>
    /// <param name="exchangeName">AMQP exchange name (empty string = default direct exchange).</param>
    /// <param name="routingKey">AMQP routing key.</param>
    /// <param name="exchangeType">The exchange type that controls routing semantics.</param>
    /// <returns>Surgewave topic name to produce/consume from.</returns>
    public static string MapToSurgewaveTopic(
        string exchangeName,
        string routingKey,
        AmqpExchangeType exchangeType)
    {
        return exchangeType switch
        {
            AmqpExchangeType.Fanout => NormalizeName(exchangeName),
            AmqpExchangeType.Direct or AmqpExchangeType.Headers => NormalizeName(
                string.IsNullOrEmpty(routingKey) ? exchangeName : routingKey),
            AmqpExchangeType.Topic => NormalizeName(
                string.IsNullOrEmpty(routingKey) ? exchangeName : routingKey),
            _ => NormalizeName(routingKey),
        };
    }

    /// <summary>
    /// Maps an AMQP queue name to a Surgewave consumer-group name.
    /// </summary>
    /// <param name="queueName">AMQP queue name.</param>
    /// <returns>Surgewave consumer-group identifier.</returns>
    public static string MapQueueToConsumerGroup(string queueName)
        => NormalizeName(queueName);

    /// <summary>
    /// Returns true when <paramref name="routingKey"/> matches the AMQP topic-exchange
    /// binding pattern <paramref name="bindingPattern"/>.
    /// Dots ('.') separate words; '*' matches exactly one word; '#' matches zero or more words.
    /// </summary>
    /// <param name="bindingPattern">The binding pattern registered for a queue (may contain wildcards).</param>
    /// <param name="routingKey">The routing key of a published message.</param>
    /// <returns>True if the routing key matches the binding pattern.</returns>
    public static bool MatchesTopicPattern(string bindingPattern, string routingKey)
    {
        // Convert AMQP topic wildcards to glob-style before matching:
        //   AMQP '*' = exactly one word → glob '?*' is not precise enough,
        //   so we do the matching ourselves to honour AMQP semantics.
        return AmqpTopicPatternMatcher.Matches(bindingPattern, routingKey);
    }

    /// <summary>
    /// Normalizes an AMQP name to a valid Surgewave topic/group identifier.
    /// Replaces dots, slashes, and spaces with hyphens; strips leading and trailing separators.
    /// </summary>
    internal static string NormalizeName(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
            return "default";

        // Replace characters not valid in Surgewave topic names
        var normalized = name
            .Replace('/', '-')
            .Replace(' ', '-')
            .Trim('-');

        return string.IsNullOrEmpty(normalized) ? "default" : normalized;
    }
}
