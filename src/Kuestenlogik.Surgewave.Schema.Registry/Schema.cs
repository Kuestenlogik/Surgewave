namespace Kuestenlogik.Surgewave.Schema.Registry;

/// <summary>
/// Represents a schema in the registry.
/// </summary>
public sealed record Schema
{
    /// <summary>
    /// Global unique schema ID.
    /// </summary>
    public required int Id { get; init; }

    /// <summary>
    /// The subject this schema is registered under.
    /// </summary>
    public required string Subject { get; init; }

    /// <summary>
    /// Version number within the subject.
    /// </summary>
    public required int Version { get; init; }

    /// <summary>
    /// The schema type (AVRO, JSON, PROTOBUF, etc.).
    /// </summary>
    public required SchemaType SchemaType { get; init; }

    /// <summary>
    /// The schema definition string.
    /// </summary>
    public required string SchemaString { get; init; }

    /// <summary>
    /// References to other schemas (for PROTOBUF imports).
    /// </summary>
    public IReadOnlyList<SchemaReference>? References { get; init; }

    /// <summary>
    /// When the schema was registered.
    /// </summary>
    public DateTimeOffset RegisteredAt { get; init; } = DateTimeOffset.UtcNow;
}

/// <summary>
/// Schema type enumeration.
/// </summary>
public enum SchemaType
{
    /// <summary>
    /// Apache Avro schema.
    /// </summary>
    Avro,

    /// <summary>
    /// JSON Schema.
    /// </summary>
    Json,

    /// <summary>
    /// Protocol Buffers schema.
    /// </summary>
    Protobuf,

    /// <summary>
    /// FlatBuffers schema.
    /// </summary>
    FlatBuffers
}

/// <summary>
/// Reference to another schema (for imports).
/// </summary>
public sealed record SchemaReference(string Name, string Subject, int Version);

/// <summary>
/// Compatibility mode for schema evolution.
/// </summary>
public enum CompatibilityMode
{
    /// <summary>
    /// No compatibility checks.
    /// </summary>
    None,

    /// <summary>
    /// New schema can read data written by old schema.
    /// </summary>
    Backward,

    /// <summary>
    /// New schema can read data written by all previous schemas.
    /// </summary>
    BackwardTransitive,

    /// <summary>
    /// Old schema can read data written by new schema.
    /// </summary>
    Forward,

    /// <summary>
    /// All previous schemas can read data written by new schema.
    /// </summary>
    ForwardTransitive,

    /// <summary>
    /// Both backward and forward compatible.
    /// </summary>
    Full,

    /// <summary>
    /// Full compatibility with all previous versions.
    /// </summary>
    FullTransitive
}

/// <summary>
/// Subject-level configuration.
/// </summary>
public sealed record SubjectConfig
{
    /// <summary>
    /// The subject name.
    /// </summary>
    public required string Subject { get; init; }

    /// <summary>
    /// Compatibility mode for this subject.
    /// </summary>
    public CompatibilityMode Compatibility { get; set; } = CompatibilityMode.Backward;

    /// <summary>
    /// Whether the subject is soft-deleted.
    /// </summary>
    public bool IsDeleted { get; set; }
}

/// <summary>
/// Result of a compatibility check.
/// </summary>
public sealed record CompatibilityResult(bool IsCompatible, IReadOnlyList<string>? Messages = null);

/// <summary>
/// Schema metadata without the full schema string.
/// </summary>
public sealed record SchemaMetadata(string Subject, int Id, int Version, string SchemaType);
