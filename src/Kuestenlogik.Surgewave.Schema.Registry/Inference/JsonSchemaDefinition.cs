namespace Kuestenlogik.Surgewave.Schema.Registry.Inference;

/// <summary>
/// Internal representation of an inferred JSON Schema.
/// Built incrementally from sampled messages.
/// </summary>
public sealed class JsonSchemaDefinition
{
    /// <summary>
    /// The JSON Schema type (object, array, string, number, integer, boolean, null).
    /// </summary>
    public string Type { get; set; } = "object";

    /// <summary>
    /// Properties for object types, keyed by property name.
    /// </summary>
    public Dictionary<string, JsonSchemaProperty> Properties { get; set; } = [];

    /// <summary>
    /// Set of property names that are required (present in every sampled message).
    /// </summary>
    public HashSet<string> Required { get; set; } = [];

    /// <summary>
    /// Number of messages sampled to produce this schema.
    /// </summary>
    public int SampleCount { get; set; }

    /// <summary>
    /// Items schema for array types.
    /// </summary>
    public JsonSchemaDefinition? Items { get; set; }
}

/// <summary>
/// Represents a single property in an inferred JSON Schema.
/// </summary>
public sealed class JsonSchemaProperty
{
    /// <summary>
    /// The JSON type of this property (string, number, integer, boolean, object, array, null).
    /// </summary>
    public string Type { get; set; } = "string";

    /// <summary>
    /// Optional format hint (date-time, email, uri, uuid, ipv4, ipv6).
    /// </summary>
    public string? Format { get; set; }

    /// <summary>
    /// Whether this property can be null.
    /// </summary>
    public bool Nullable { get; set; }

    /// <summary>
    /// Items schema when this property is an array.
    /// </summary>
    public JsonSchemaDefinition? Items { get; set; }

    /// <summary>
    /// Nested object schema when this property is an object.
    /// </summary>
    public JsonSchemaDefinition? ObjectSchema { get; set; }

    /// <summary>
    /// Number of messages in which this property was observed.
    /// </summary>
    public int SeenCount { get; set; }
}
