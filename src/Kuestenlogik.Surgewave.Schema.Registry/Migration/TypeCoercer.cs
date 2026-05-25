using System.Globalization;
using System.Text.Json;

namespace Kuestenlogik.Surgewave.Schema.Registry.Migration;

/// <summary>
/// Handles automatic type coercion between JSON Schema types during schema migration.
/// Supports common conversions: int/string, double/int, bool/string, null/default, any/string.
/// </summary>
public static class TypeCoercer
{
    /// <summary>
    /// Coerce a JSON element value from one JSON Schema type to another.
    /// Returns the coerced value as a JsonElement-compatible object, or null if coercion is not possible.
    /// </summary>
    /// <param name="value">The source value as a <see cref="JsonElement"/>.</param>
    /// <param name="fromType">The JSON Schema type of the source (e.g., "integer", "string").</param>
    /// <param name="toType">The JSON Schema type of the target.</param>
    /// <returns>The coerced value, or null if coercion fails.</returns>
    public static object? Coerce(JsonElement value, string fromType, string toType)
    {
        var fromBase = GetBaseType(fromType);
        var toBase = GetBaseType(toType);

        if (string.Equals(fromBase, toBase, StringComparison.Ordinal))
        {
            return value;
        }

        if (value.ValueKind == JsonValueKind.Null)
        {
            return GetDefaultForType(toBase);
        }

        return (fromBase, toBase) switch
        {
            ("integer", "string") => value.GetInt64().ToString(CultureInfo.InvariantCulture),
            ("integer", "number") => value.GetDouble(),
            ("integer", "boolean") => value.GetInt64() != 0,

            ("number", "string") => value.GetDouble().ToString(CultureInfo.InvariantCulture),
            ("number", "integer") => (long)value.GetDouble(),
            ("number", "boolean") => value.GetDouble() != 0.0,

            ("string", "integer") => TryParseInt(value.GetString()),
            ("string", "number") => TryParseDouble(value.GetString()),
            ("string", "boolean") => TryParseBool(value.GetString()),

            ("boolean", "string") => value.GetBoolean() ? "true" : "false",
            ("boolean", "integer") => value.GetBoolean() ? 1L : 0L,
            ("boolean", "number") => value.GetBoolean() ? 1.0 : 0.0,

            // Fallback: anything to string via raw text
            (_, "string") => value.ToString(),

            _ => null
        };
    }

    /// <summary>
    /// Check whether coercion is possible between two JSON Schema types.
    /// </summary>
    /// <param name="fromType">Source JSON Schema type.</param>
    /// <param name="toType">Target JSON Schema type.</param>
    /// <returns>True if automatic coercion is supported.</returns>
    public static bool CanCoerce(string fromType, string toType)
    {
        var fromBase = GetBaseType(fromType);
        var toBase = GetBaseType(toType);

        if (string.Equals(fromBase, toBase, StringComparison.Ordinal))
        {
            return true;
        }

        // Any type can be coerced to string
        if (string.Equals(toBase, "string", StringComparison.Ordinal))
        {
            return true;
        }

        return (fromBase, toBase) switch
        {
            ("integer", "number") => true,
            ("integer", "boolean") => true,
            ("number", "integer") => true,
            ("number", "boolean") => true,
            ("string", "integer") => true,
            ("string", "number") => true,
            ("string", "boolean") => true,
            ("boolean", "integer") => true,
            ("boolean", "number") => true,
            _ => false
        };
    }

    /// <summary>
    /// Get the default value for a JSON Schema type.
    /// </summary>
    /// <param name="jsonType">The JSON Schema type (e.g., "string", "integer").</param>
    /// <returns>A default value suitable for JSON serialization.</returns>
    public static object? GetDefaultForType(string jsonType)
    {
        var baseType = GetBaseType(jsonType);

        return baseType switch
        {
            "string" => "",
            "integer" => 0L,
            "number" => 0.0,
            "boolean" => false,
            "array" => Array.Empty<object>(),
            "object" => new Dictionary<string, object>(),
            _ => null
        };
    }

    private static string GetBaseType(string type) => JsonSchemaTypeHelper.GetBaseType(type);

    private static object? TryParseInt(string? value)
    {
        if (value is null)
        {
            return 0L;
        }

        return long.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out var result)
            ? result
            : 0L;
    }

    private static object? TryParseDouble(string? value)
    {
        if (value is null)
        {
            return 0.0;
        }

        return double.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out var result)
            ? result
            : 0.0;
    }

    private static object? TryParseBool(string? value)
    {
        if (value is null)
        {
            return false;
        }

        return value.ToLowerInvariant() switch
        {
            "true" or "1" or "yes" => true,
            "false" or "0" or "no" => false,
            _ => false
        };
    }
}
