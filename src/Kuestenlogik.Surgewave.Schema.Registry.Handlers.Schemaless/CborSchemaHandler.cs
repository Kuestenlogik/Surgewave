namespace Kuestenlogik.Surgewave.Schema.Registry.Handlers;

/// <summary>
/// Schema type handler for CBOR (schemaless binary) serialization.
/// CBOR is self-describing and handles versioning internally,
/// so validation always succeeds and compatibility is always maintained.
/// </summary>
public sealed class CborSchemaHandler : ISchemaTypeHandler
{
    /// <inheritdoc />
    public string TypeName => "CBOR";

    /// <inheritdoc />
    /// <remarks>
    /// CBOR is schemaless — any schema string is accepted.
    /// The schema string is typically a type name hint for display purposes
    /// (e.g. "MyApp.OrderEvent").
    /// </remarks>
    public (bool IsValid, string? Error) Validate(string schemaString)
    {
        return (true, null);
    }

    /// <inheritdoc />
    /// <remarks>
    /// CBOR is self-describing and handles versioning internally,
    /// so all schemas are always compatible regardless of mode.
    /// </remarks>
    public CompatibilityResult CheckCompatibility(
        string newSchemaString,
        IReadOnlyList<Schema> existingSchemas,
        CompatibilityMode mode)
    {
        return new CompatibilityResult(true);
    }
}
