namespace Kuestenlogik.Surgewave.Schema.Registry;

/// <summary>
/// Interface for schema type handlers that provide validation and compatibility checking.
/// Implement this interface to add support for custom schema formats.
/// </summary>
public interface ISchemaTypeHandler
{
    /// <summary>
    /// The schema type name (e.g., "AVRO", "JSON", "PROTOBUF").
    /// This is used in the REST API and stored with schemas.
    /// </summary>
    string TypeName { get; }

    /// <summary>
    /// Validates that a schema string is well-formed.
    /// </summary>
    /// <param name="schemaString">The schema definition string.</param>
    /// <returns>A tuple indicating if valid and an optional error message.</returns>
    (bool IsValid, string? Error) Validate(string schemaString);

    /// <summary>
    /// Checks if a new schema is compatible with existing schemas.
    /// </summary>
    /// <param name="newSchemaString">The new schema to check.</param>
    /// <param name="existingSchemas">Existing schemas to check against.</param>
    /// <param name="mode">The compatibility mode to use.</param>
    /// <returns>The result of the compatibility check.</returns>
    CompatibilityResult CheckCompatibility(
        string newSchemaString,
        IReadOnlyList<Schema> existingSchemas,
        CompatibilityMode mode);
}
