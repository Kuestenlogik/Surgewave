using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Gateway.WebSocket.Protocol;

/// <summary>
/// Subscribe request payload.
/// </summary>
public sealed class SubscribePayload
{
    [JsonPropertyName("topic")]
    public required string Topic { get; set; }

    /// <summary>
    /// Specific partitions to subscribe to. All partitions if null or empty.
    /// </summary>
    [JsonPropertyName("partitions")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public int[]? Partitions { get; set; }

    /// <summary>
    /// Starting offset: "latest", "earliest", or numeric offset.
    /// </summary>
    [JsonPropertyName("from_offset")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? FromOffset { get; set; }

    /// <summary>
    /// Consumer group for offset tracking.
    /// </summary>
    [JsonPropertyName("consumer_group")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? ConsumerGroup { get; set; }

    /// <summary>
    /// Maximum messages per batch.
    /// </summary>
    [JsonPropertyName("max_batch_size")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public int MaxBatchSize { get; set; } = 100;

    /// <summary>
    /// Maximum wait time in milliseconds before returning partial results.
    /// </summary>
    [JsonPropertyName("max_wait_ms")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public int MaxWaitMs { get; set; } = 1000;
}

/// <summary>
/// Unsubscribe request payload.
/// </summary>
public sealed class UnsubscribePayload
{
    [JsonPropertyName("subscription_id")]
    public required string SubscriptionId { get; set; }
}

/// <summary>
/// Produce request payload.
/// </summary>
public sealed class ProducePayload
{
    [JsonPropertyName("topic")]
    public required string Topic { get; set; }

    /// <summary>
    /// Target partition. Uses partitioner if not specified.
    /// </summary>
    [JsonPropertyName("partition")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public int? Partition { get; set; }

    /// <summary>
    /// Message key (base64 encoded).
    /// </summary>
    [JsonPropertyName("key")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Key { get; set; }

    /// <summary>
    /// Message value (base64 encoded).
    /// </summary>
    [JsonPropertyName("value")]
    public required string Value { get; set; }

    /// <summary>
    /// Optional message headers.
    /// </summary>
    [JsonPropertyName("headers")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public Dictionary<string, string>? Headers { get; set; }
}

/// <summary>
/// Single record in a produce batch.
/// </summary>
public sealed class ProduceBatchRecord
{
    [JsonPropertyName("partition")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public int? Partition { get; set; }

    [JsonPropertyName("key")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Key { get; set; }

    [JsonPropertyName("value")]
    public required string Value { get; set; }

    [JsonPropertyName("headers")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public Dictionary<string, string>? Headers { get; set; }
}

/// <summary>
/// Produce batch request payload.
/// </summary>
public sealed class ProduceBatchPayload
{
    [JsonPropertyName("topic")]
    public required string Topic { get; set; }

    [JsonPropertyName("records")]
    public required ProduceBatchRecord[] Records { get; set; }
}

/// <summary>
/// Offset commit entry.
/// </summary>
public sealed class CommitOffset
{
    [JsonPropertyName("topic")]
    public required string Topic { get; set; }

    [JsonPropertyName("partition")]
    public int Partition { get; set; }

    [JsonPropertyName("offset")]
    public long Offset { get; set; }
}

/// <summary>
/// Commit request payload.
/// </summary>
public sealed class CommitPayload
{
    [JsonPropertyName("consumer_group")]
    public required string ConsumerGroup { get; set; }

    [JsonPropertyName("offsets")]
    public required CommitOffset[] Offsets { get; set; }
}

/// <summary>
/// Admin request payload.
/// </summary>
public sealed class AdminPayload
{
    /// <summary>
    /// Admin action type.
    /// </summary>
    [JsonPropertyName("action")]
    public required string Action { get; set; }

    /// <summary>
    /// Topic name for topic-specific operations.
    /// </summary>
    [JsonPropertyName("topic")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Topic { get; set; }

    /// <summary>
    /// Consumer group ID for group-specific operations.
    /// </summary>
    [JsonPropertyName("group_id")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? GroupId { get; set; }

    /// <summary>
    /// Include internal topics in list operations.
    /// </summary>
    [JsonPropertyName("include_internal")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public bool IncludeInternal { get; set; }
}
