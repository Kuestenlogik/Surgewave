using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Schema.Registry.Inference;

/// <summary>
/// Response returned when inferring a schema from topic messages.
/// </summary>
public sealed class InferredSchemaResponse
{
    /// <summary>
    /// The topic from which the schema was inferred.
    /// </summary>
    [JsonPropertyName("topic")]
    public string Topic { get; set; } = "";

    /// <summary>
    /// The inferred JSON Schema as a string.
    /// </summary>
    [JsonPropertyName("schema")]
    public string Schema { get; set; } = "";

    /// <summary>
    /// Number of messages sampled.
    /// </summary>
    [JsonPropertyName("sampleCount")]
    public int SampleCount { get; set; }

    /// <summary>
    /// Number of valid JSON messages found in the sample.
    /// </summary>
    [JsonPropertyName("validMessageCount")]
    public int ValidMessageCount { get; set; }

    /// <summary>
    /// Field-level statistics showing how often each field was seen.
    /// </summary>
    [JsonPropertyName("fieldStats")]
    public List<FieldStatistic> FieldStats { get; set; } = [];

    /// <summary>
    /// When the inference was performed.
    /// </summary>
    [JsonPropertyName("inferredAt")]
    public DateTimeOffset InferredAt { get; set; } = DateTimeOffset.UtcNow;
}

/// <summary>
/// Statistics for a single field in an inferred schema.
/// </summary>
public sealed class FieldStatistic
{
    /// <summary>
    /// Dot-delimited path to the field (e.g. "address.city").
    /// </summary>
    [JsonPropertyName("path")]
    public string Path { get; set; } = "";

    /// <summary>
    /// The inferred type of the field.
    /// </summary>
    [JsonPropertyName("type")]
    public string Type { get; set; } = "";

    /// <summary>
    /// Optional format hint (date-time, email, uri, uuid, ipv4, ipv6).
    /// </summary>
    [JsonPropertyName("format")]
    public string? Format { get; set; }

    /// <summary>
    /// Whether the field can be null.
    /// </summary>
    [JsonPropertyName("nullable")]
    public bool Nullable { get; set; }

    /// <summary>
    /// Number of messages in which this field was observed.
    /// </summary>
    [JsonPropertyName("seenCount")]
    public int SeenCount { get; set; }

    /// <summary>
    /// Total messages sampled.
    /// </summary>
    [JsonPropertyName("totalCount")]
    public int TotalCount { get; set; }

    /// <summary>
    /// Whether this field is required (seen in all messages).
    /// </summary>
    [JsonPropertyName("required")]
    public bool Required { get; set; }
}

/// <summary>
/// Summary of an auto-inferred schema for listing purposes.
/// </summary>
public sealed class InferredSchemaSummary
{
    /// <summary>
    /// The topic name.
    /// </summary>
    [JsonPropertyName("topic")]
    public string Topic { get; set; } = "";

    /// <summary>
    /// The subject name in the registry (typically "{topic}-inferred-value").
    /// </summary>
    [JsonPropertyName("subject")]
    public string Subject { get; set; } = "";

    /// <summary>
    /// Number of fields in the inferred schema.
    /// </summary>
    [JsonPropertyName("fieldCount")]
    public int FieldCount { get; set; }

    /// <summary>
    /// Number of messages sampled.
    /// </summary>
    [JsonPropertyName("sampleCount")]
    public int SampleCount { get; set; }

    /// <summary>
    /// When the schema was last inferred.
    /// </summary>
    [JsonPropertyName("lastInferredAt")]
    public DateTimeOffset LastInferredAt { get; set; }

    /// <summary>
    /// Whether the schema is registered in the registry.
    /// </summary>
    [JsonPropertyName("registered")]
    public bool Registered { get; set; }

    /// <summary>
    /// Schema ID if registered, null otherwise.
    /// </summary>
    [JsonPropertyName("schemaId")]
    public int? SchemaId { get; set; }
}
