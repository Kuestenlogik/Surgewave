namespace Kuestenlogik.Surgewave.Protocol.Amqp;

/// <summary>
/// Implements AMQP topic-exchange routing key pattern matching.
/// Words are separated by dots ('.'). Two wildcards are supported:
/// <list type="bullet">
///   <item><c>*</c> — matches exactly one word.</item>
///   <item><c>#</c> — matches zero or more words (including dots).</item>
/// </list>
/// </summary>
internal static class AmqpTopicPatternMatcher
{
    /// <summary>
    /// Returns true when <paramref name="routingKey"/> matches <paramref name="pattern"/>.
    /// </summary>
    /// <param name="pattern">Binding pattern (may contain '*' and '#' wildcards).</param>
    /// <param name="routingKey">Published routing key.</param>
    public static bool Matches(string pattern, string routingKey)
    {
        if (pattern == "#")
            return true;

        if (string.Equals(pattern, routingKey, StringComparison.Ordinal))
            return true;

        var patternWords = pattern.Split('.');
        var keyWords = routingKey.Split('.');

        return MatchWords(patternWords, 0, keyWords, 0);
    }

    private static bool MatchWords(
        string[] pattern, int pi,
        string[] key, int ki)
    {
        while (pi < pattern.Length)
        {
            var word = pattern[pi];

            if (word == "#")
            {
                // '#' can consume zero or more key words — try all possibilities
                for (int consumed = ki; consumed <= key.Length; consumed++)
                {
                    if (MatchWords(pattern, pi + 1, key, consumed))
                        return true;
                }
                return false;
            }

            if (ki >= key.Length)
                return false; // ran out of key words

            if (word == "*")
            {
                // '*' consumes exactly one key word
                pi++;
                ki++;
                continue;
            }

            // Exact word match required
            if (!string.Equals(word, key[ki], StringComparison.Ordinal))
                return false;

            pi++;
            ki++;
        }

        // Pattern fully consumed — key must also be fully consumed
        return ki == key.Length;
    }
}
