namespace Kuestenlogik.Surgewave.Schema.Registry;

/// <summary>
/// Registry for schema type handlers.
/// </summary>
public interface ISchemaTypeHandlerRegistry
{
    /// <summary>
    /// Gets all registered handler type names.
    /// </summary>
    IEnumerable<string> GetSupportedTypes();

    /// <summary>
    /// Gets a handler by type name.
    /// </summary>
    /// <param name="typeName">The schema type name (case-insensitive).</param>
    /// <returns>The handler, or null if not found.</returns>
    ISchemaTypeHandler? GetHandler(string typeName);

    /// <summary>
    /// Checks if a handler is registered for the given type name.
    /// </summary>
    bool IsSupported(string typeName);
}
