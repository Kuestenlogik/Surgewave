using System.Collections;
using System.Text.Json;

namespace Kuestenlogik.Surgewave.Streams.Sql;

/// <summary>
/// Reads messages from a Surgewave topic and yields rows as dictionaries.
/// Each message is deserialized from JSON into key-value pairs,
/// with metadata columns (_offset, _partition, _timestamp, _key) automatically added.
/// Evaluation is lazy — messages are read on enumeration.
/// </summary>
public sealed class SqlTopicSource : IEnumerable<Dictionary<string, object?>>
{
    private readonly Func<IEnumerable<RawTopicMessage>> _messageProvider;
    private readonly int? _limit;

    /// <summary>
    /// Creates a topic source from a message provider function.
    /// This constructor is used internally and by the broker integration.
    /// </summary>
    /// <param name="messageProvider">Function that yields raw messages from a topic.</param>
    /// <param name="limit">Optional maximum number of messages to read.</param>
    public SqlTopicSource(Func<IEnumerable<RawTopicMessage>> messageProvider, int? limit = null)
    {
        _messageProvider = messageProvider ?? throw new ArgumentNullException(nameof(messageProvider));
        _limit = limit;
    }

    /// <summary>
    /// Creates a topic source from a pre-built collection of raw messages.
    /// Useful for testing or when messages are already available in memory.
    /// </summary>
    public SqlTopicSource(IEnumerable<RawTopicMessage> messages, int? limit = null)
    {
        ArgumentNullException.ThrowIfNull(messages);
        _messageProvider = () => messages;
        _limit = limit;
    }

    public IEnumerator<Dictionary<string, object?>> GetEnumerator()
    {
        var count = 0;
        foreach (var message in _messageProvider())
        {
            if (_limit.HasValue && count >= _limit.Value)
                yield break;

            var row = DeserializeMessage(message);
            yield return row;
            count++;
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    private static Dictionary<string, object?> DeserializeMessage(RawTopicMessage message)
    {
        var row = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

        // Add metadata columns
        row["_offset"] = message.Offset;
        row["_partition"] = message.Partition;
        row["_timestamp"] = message.Timestamp;
        row["_key"] = message.Key;

        // Try to deserialize the value as JSON
        if (!string.IsNullOrEmpty(message.Value))
        {
            try
            {
                var doc = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(message.Value);
                if (doc != null)
                {
                    foreach (var (key, element) in doc)
                    {
                        row[key] = element.ValueKind switch
                        {
                            JsonValueKind.String => element.GetString(),
                            JsonValueKind.Number => element.TryGetInt64(out var l) ? (object)l : element.GetDouble(),
                            JsonValueKind.True => true,
                            JsonValueKind.False => false,
                            JsonValueKind.Null => null,
                            JsonValueKind.Array => element.GetRawText(),
                            JsonValueKind.Object => element.GetRawText(),
                            _ => element.GetRawText()
                        };
                    }
                }
            }
            catch (JsonException)
            {
                // Not valid JSON — store raw value
                row["_value"] = message.Value;
            }
        }

        // Add headers if present
        if (message.Headers is { Count: > 0 })
        {
            foreach (var (key, value) in message.Headers)
            {
                row[$"_header_{key}"] = value;
            }
        }

        return row;
    }
}

/// <summary>
/// Represents a raw message read from a Surgewave topic.
/// This is the bridge between the Surgewave transport layer and the SQL engine.
/// </summary>
public sealed record RawTopicMessage(
    long Offset,
    int Partition,
    DateTimeOffset Timestamp,
    string? Key,
    string? Value,
    IReadOnlyDictionary<string, string>? Headers = null);
