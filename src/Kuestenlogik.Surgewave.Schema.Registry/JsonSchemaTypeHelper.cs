namespace Kuestenlogik.Surgewave.Schema.Registry;

/// <summary>
/// Shared helper for extracting the base (non-null) type from JSON Schema union type strings.
/// </summary>
internal static class JsonSchemaTypeHelper
{
    /// <summary>
    /// Extracts the base type from a union type string (e.g., "string|null" returns "string").
    /// Returns the type as-is if it does not contain a pipe separator.
    /// </summary>
    public static string GetBaseType(string type)
    {
        if (!type.Contains('|')) return type.Trim();
        var parts = type.Split('|', StringSplitOptions.TrimEntries);
        foreach (var part in parts)
            if (!part.Equals("null", StringComparison.OrdinalIgnoreCase))
                return part;
        return type;
    }
}
