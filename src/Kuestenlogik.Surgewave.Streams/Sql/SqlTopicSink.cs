using System.Text.Json;

namespace Kuestenlogik.Surgewave.Streams.Sql;

/// <summary>
/// Writes SQL query result rows to a Surgewave topic as JSON messages.
/// Each row is serialized as a JSON object and forwarded to the configured write action.
/// </summary>
public sealed class SqlTopicSink : IAsyncDisposable
{
    private readonly Func<string?, string, Task> _writeAction;
    private readonly string _topic;
    private readonly string? _keyColumn;
    private int _pendingCount;
    private readonly Func<Task>? _flushAction;

    /// <summary>
    /// Creates a topic sink that writes rows to a Surgewave topic.
    /// </summary>
    /// <param name="topic">The target topic name.</param>
    /// <param name="writeAction">Function that writes (key, jsonValue) to the topic.</param>
    /// <param name="flushAction">Optional function to flush pending writes.</param>
    /// <param name="keyColumn">Optional column to use as the message key. If null, key is omitted.</param>
    public SqlTopicSink(
        string topic,
        Func<string?, string, Task> writeAction,
        Func<Task>? flushAction = null,
        string? keyColumn = null)
    {
        _topic = topic ?? throw new ArgumentNullException(nameof(topic));
        _writeAction = writeAction ?? throw new ArgumentNullException(nameof(writeAction));
        _flushAction = flushAction;
        _keyColumn = keyColumn;
    }

    /// <summary>
    /// The target topic name.
    /// </summary>
    public string Topic => _topic;

    /// <summary>
    /// Number of rows written since the last flush.
    /// </summary>
    public int PendingCount => _pendingCount;

    /// <summary>
    /// Writes a single result row to the topic as a JSON message.
    /// Metadata columns (prefixed with _) are excluded from the output unless they are the key.
    /// </summary>
    public async Task WriteRowAsync(Dictionary<string, object?> row)
    {
        // Extract key if configured
        string? key = null;
        if (_keyColumn != null && row.TryGetValue(_keyColumn, out var keyValue))
        {
            key = keyValue?.ToString();
        }

        // Build output JSON, excluding metadata columns
        var output = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (var (col, val) in row)
        {
            if (!col.StartsWith('_'))
            {
                output[col] = val;
            }
        }

        var json = JsonSerializer.Serialize(output, JsonSerializerOptionsInstance);
        await _writeAction(key, json);
        Interlocked.Increment(ref _pendingCount);
    }

    /// <summary>
    /// Writes multiple result rows to the topic.
    /// </summary>
    public async Task WriteBatchAsync(IEnumerable<Dictionary<string, object?>> rows)
    {
        foreach (var row in rows)
        {
            await WriteRowAsync(row);
        }
    }

    /// <summary>
    /// Flushes any pending writes to the underlying transport.
    /// </summary>
    public async Task FlushAsync()
    {
        if (_flushAction != null)
        {
            await _flushAction();
        }
        Interlocked.Exchange(ref _pendingCount, 0);
    }

    public async ValueTask DisposeAsync()
    {
        if (_pendingCount > 0)
        {
            await FlushAsync();
        }
    }

    private static readonly JsonSerializerOptions JsonSerializerOptionsInstance = new()
    {
        WriteIndented = false,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };
}
