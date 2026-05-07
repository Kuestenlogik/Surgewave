namespace Kuestenlogik.Surgewave.Protocol.Mqtt;

/// <summary>
/// MQTT topic filter matching with support for '+' (single-level) and '#' (multi-level) wildcards.
/// </summary>
public static class MqttTopicMatcher
{
    /// <summary>
    /// Check if an MQTT topic name matches a topic filter, supporting '+' and '#' wildcards.
    /// </summary>
    /// <param name="filter">Topic filter (may contain '+' and '#' wildcards).</param>
    /// <param name="topic">Actual topic name to match against.</param>
    /// <returns>True if the topic matches the filter.</returns>
    public static bool Matches(string filter, string topic)
    {
        // '#' matches everything
        if (filter == "#")
            return true;

        var filterLevels = filter.Split('/');
        var topicLevels = topic.Split('/');

        for (int i = 0; i < filterLevels.Length; i++)
        {
            var filterLevel = filterLevels[i];

            // Multi-level wildcard must be last and matches all remaining levels
            if (filterLevel == "#")
                return true;

            // No more topic levels but filter still has levels
            if (i >= topicLevels.Length)
                return false;

            // Single-level wildcard matches exactly one level
            if (filterLevel == "+")
                continue;

            // Exact match required
            if (!string.Equals(filterLevel, topicLevels[i], StringComparison.Ordinal))
                return false;
        }

        // Filter must have consumed all topic levels
        return filterLevels.Length == topicLevels.Length;
    }
}
