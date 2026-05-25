using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Control.Models;

/// <summary>
/// Schema information response.
/// </summary>
public sealed class SchemaModel
{
    [JsonPropertyName("subject")]
    public string Subject { get; set; } = "";

    [JsonPropertyName("id")]
    public int Id { get; set; }

    [JsonPropertyName("version")]
    public int Version { get; set; }

    [JsonPropertyName("schemaType")]
    public string SchemaType { get; set; } = "AVRO";

    [JsonPropertyName("schema")]
    public string Schema { get; set; } = "";

    [JsonPropertyName("references")]
    public List<SchemaReferenceModel>? References { get; set; }
}

/// <summary>
/// Schema reference for nested schemas.
/// </summary>
public sealed class SchemaReferenceModel
{
    [JsonPropertyName("name")]
    public string Name { get; set; } = "";

    [JsonPropertyName("subject")]
    public string Subject { get; set; } = "";

    [JsonPropertyName("version")]
    public int Version { get; set; }
}

/// <summary>
/// Request to register a new schema.
/// </summary>
public sealed class RegisterSchemaRequest
{
    [JsonPropertyName("schema")]
    public string? Schema { get; set; }

    [JsonPropertyName("schemaType")]
    public string? SchemaType { get; set; }

    [JsonPropertyName("references")]
    public List<SchemaReferenceModel>? References { get; set; }
}

/// <summary>
/// Response after registering a schema.
/// </summary>
public sealed class RegisterSchemaResponse
{
    [JsonPropertyName("id")]
    public int Id { get; set; }
}

/// <summary>
/// Compatibility configuration response.
/// </summary>
public sealed class CompatibilityConfigModel
{
    [JsonPropertyName("compatibilityLevel")]
    public string CompatibilityLevel { get; set; } = "BACKWARD";
}

/// <summary>
/// Compatibility check response.
/// </summary>
public sealed class CompatibilityCheckResult
{
    [JsonPropertyName("is_compatible")]
    public bool IsCompatible { get; set; }

    [JsonPropertyName("messages")]
    public List<string>? Messages { get; set; }
}

/// <summary>
/// Subject with version count for listing.
/// </summary>
public sealed class SubjectInfoModel
{
    public string Subject { get; set; } = "";
    public int VersionCount { get; set; }
    public string? LatestSchemaType { get; set; }
    public string? CompatibilityLevel { get; set; }
}

/// <summary>
/// Inferred schema response from the inference API.
/// </summary>
public sealed class InferredSchemaModel
{
    [JsonPropertyName("topic")]
    public string Topic { get; set; } = "";

    [JsonPropertyName("schema")]
    public string Schema { get; set; } = "";

    [JsonPropertyName("sampleCount")]
    public int SampleCount { get; set; }

    [JsonPropertyName("validMessageCount")]
    public int ValidMessageCount { get; set; }

    [JsonPropertyName("fieldStats")]
    public List<FieldStatisticModel> FieldStats { get; set; } = [];

    [JsonPropertyName("inferredAt")]
    public DateTimeOffset InferredAt { get; set; }
}

/// <summary>
/// Field-level statistics from schema inference.
/// </summary>
public sealed class FieldStatisticModel
{
    [JsonPropertyName("path")]
    public string Path { get; set; } = "";

    [JsonPropertyName("type")]
    public string Type { get; set; } = "";

    [JsonPropertyName("format")]
    public string? Format { get; set; }

    [JsonPropertyName("nullable")]
    public bool Nullable { get; set; }

    [JsonPropertyName("seenCount")]
    public int SeenCount { get; set; }

    [JsonPropertyName("totalCount")]
    public int TotalCount { get; set; }

    [JsonPropertyName("required")]
    public bool Required { get; set; }
}

/// <summary>
/// Response from GET /schemas/ids/{id} — schema by global ID.
/// </summary>
public sealed class SchemaByIdModel
{
    [JsonPropertyName("schema")]
    public string Schema { get; set; } = "";

    [JsonPropertyName("schemaType")]
    public string SchemaType { get; set; } = "AVRO";
}

/// <summary>
/// Subject-version pair returned from GET /schemas/ids/{id}/versions.
/// </summary>
public sealed class SubjectVersionModel
{
    [JsonPropertyName("subject")]
    public string Subject { get; set; } = "";

    [JsonPropertyName("version")]
    public int Version { get; set; }
}

/// <summary>
/// Summary of an auto-inferred schema.
/// </summary>
public sealed class InferredSchemaSummaryModel
{
    [JsonPropertyName("topic")]
    public string Topic { get; set; } = "";

    [JsonPropertyName("subject")]
    public string Subject { get; set; } = "";

    [JsonPropertyName("fieldCount")]
    public int FieldCount { get; set; }

    [JsonPropertyName("sampleCount")]
    public int SampleCount { get; set; }

    [JsonPropertyName("lastInferredAt")]
    public DateTimeOffset LastInferredAt { get; set; }

    [JsonPropertyName("registered")]
    public bool Registered { get; set; }

    [JsonPropertyName("schemaId")]
    public int? SchemaId { get; set; }
}
