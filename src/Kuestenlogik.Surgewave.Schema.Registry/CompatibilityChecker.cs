using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Schema.Registry;

/// <summary>
/// Checks schema compatibility using registered schema type handlers.
/// This class delegates to ISchemaTypeHandler implementations for actual validation
/// and compatibility checking, allowing for extensibility.
/// </summary>
public sealed class CompatibilityChecker
{
    private readonly ILogger<CompatibilityChecker> _logger;
    private readonly ISchemaTypeHandlerRegistry _handlerRegistry;

    public CompatibilityChecker(
        ILogger<CompatibilityChecker> logger,
        ISchemaTypeHandlerRegistry handlerRegistry)
    {
        _logger = logger;
        _handlerRegistry = handlerRegistry;
    }

    /// <summary>
    /// Checks if a new schema is compatible with existing schemas based on compatibility mode.
    /// </summary>
    public CompatibilityResult CheckCompatibility(
        string newSchemaString,
        SchemaType schemaType,
        IReadOnlyList<Schema> existingSchemas,
        CompatibilityMode mode)
    {
        return CheckCompatibility(newSchemaString, schemaType.ToString().ToUpperInvariant(), existingSchemas, mode);
    }

    /// <summary>
    /// Checks if a new schema is compatible with existing schemas based on compatibility mode.
    /// Uses string-based type name for extensibility with custom schema types.
    /// </summary>
    public CompatibilityResult CheckCompatibility(
        string newSchemaString,
        string schemaTypeName,
        IReadOnlyList<Schema> existingSchemas,
        CompatibilityMode mode)
    {
        if (mode == CompatibilityMode.None || existingSchemas.Count == 0)
        {
            return new CompatibilityResult(true);
        }

        var handler = _handlerRegistry.GetHandler(schemaTypeName);
        if (handler == null)
        {
            _logger.LogWarning("No handler found for schema type {SchemaType}, skipping compatibility check", schemaTypeName);
            return new CompatibilityResult(true);
        }

        try
        {
            return handler.CheckCompatibility(newSchemaString, existingSchemas, mode);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error checking compatibility for {SchemaType}", schemaTypeName);
            return new CompatibilityResult(false, [$"Error parsing schema: {ex.Message}"]);
        }
    }

    /// <summary>
    /// Validates that a schema string is well-formed.
    /// </summary>
    public (bool IsValid, string? Error) ValidateSchema(string schemaString, SchemaType schemaType)
    {
        return ValidateSchema(schemaString, schemaType.ToString().ToUpperInvariant());
    }

    /// <summary>
    /// Validates that a schema string is well-formed.
    /// Uses string-based type name for extensibility with custom schema types.
    /// </summary>
    public (bool IsValid, string? Error) ValidateSchema(string schemaString, string schemaTypeName)
    {
        var handler = _handlerRegistry.GetHandler(schemaTypeName);
        if (handler == null)
        {
            return (false, $"Unsupported schema type: {schemaTypeName}");
        }

        try
        {
            return handler.Validate(schemaString);
        }
        catch (Exception ex)
        {
            return (false, ex.Message);
        }
    }

    /// <summary>
    /// Gets all supported schema type names.
    /// </summary>
    public IEnumerable<string> GetSupportedTypes() => _handlerRegistry.GetSupportedTypes();

    /// <summary>
    /// Checks if a schema type is supported.
    /// </summary>
    public bool IsTypeSupported(string schemaTypeName) => _handlerRegistry.IsSupported(schemaTypeName);
}
