using System.Text;
using System.Text.Json;

namespace Kuestenlogik.Surgewave.Connect.Nodes;

/// <summary>
/// Helper for creating error records with metadata for DLQ routing.
/// </summary>
public static class PipelineErrorRecord
{
    /// <summary>
    /// Create an error envelope with metadata from a failed record.
    /// </summary>
    public static (byte[] value, Dictionary<string, byte[]> headers) Create(
        SinkRecord original,
        string nodeId,
        Exception ex)
    {
        var timestamp = DateTimeOffset.UtcNow;

        var envelope = new Dictionary<string, object?>
        {
            ["originalKey"] = original.Key != null ? Convert.ToBase64String(original.Key) : null,
            ["originalValue"] = original.Value != null ? Convert.ToBase64String(original.Value) : null,
            ["originalTopic"] = original.Topic,
            ["originalPartition"] = original.Partition,
            ["originalOffset"] = original.Offset,
            ["error"] = ex.Message,
            ["errorType"] = ex.GetType().Name,
            ["nodeId"] = nodeId,
            ["timestamp"] = timestamp.ToString("o")
        };

        var value = JsonSerializer.SerializeToUtf8Bytes(envelope, JsonOptions.Default);

        var headers = new Dictionary<string, byte[]>
        {
            ["_error_node_id"] = Encoding.UTF8.GetBytes(nodeId),
            ["_error_type"] = Encoding.UTF8.GetBytes(ex.GetType().Name),
            ["_error_message"] = Encoding.UTF8.GetBytes(ex.Message),
            ["_error_timestamp"] = Encoding.UTF8.GetBytes(timestamp.ToUnixTimeMilliseconds().ToString())
        };

        return (value, headers);
    }

    private static class JsonOptions
    {
        public static readonly JsonSerializerOptions Default = new()
        {
            WriteIndented = false
        };
    }
}
