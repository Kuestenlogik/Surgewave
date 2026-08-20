using System.Text.Json;
using Kuestenlogik.Surgewave.Schema.Registry.Vectors;

namespace Kuestenlogik.Surgewave.Schema.Registry.Handlers;

/// <summary>
/// Vector declarations in JSON Schema (#14): <c>"format": "vector"</c> plus
/// <c>"x-vector-dim"</c> (required, positive integer) and <c>"x-vector-dtype"</c> (optional,
/// default f32) on an array of number/integer. NJsonSchema carries both <c>format</c> and
/// unknown keywords (ExtensionData) through its roundtrip, so the annotation survives
/// normalization; validation walks the raw JSON so a vector nested anywhere (definitions,
/// items, oneOf) is checked, not only what the object-model walk happens to visit.
/// </summary>
internal static class JsonSchemaVectors
{
    internal const string DimKeyword = "x-vector-dim";
    internal const string DtypeKeyword = "x-vector-dtype";

    /// <summary>Validates every vector declaration in the raw schema JSON.</summary>
    internal static List<string> ValidateRaw(string schemaJson)
    {
        var errors = new List<string>();
        using var doc = JsonDocument.Parse(schemaJson);
        WalkRaw(doc.RootElement, "$", errors);
        return errors;
    }

    private static void WalkRaw(JsonElement node, string path, List<string> errors)
    {
        switch (node.ValueKind)
        {
            case JsonValueKind.Array:
                var i = 0;
                foreach (var item in node.EnumerateArray())
                {
                    WalkRaw(item, $"{path}[{i++}]", errors);
                }
                return;

            case JsonValueKind.Object:
                break;

            default:
                return;
        }

        if (IsVectorNode(node))
        {
            ValidateVectorNode(node, path, errors);
        }

        foreach (var prop in node.EnumerateObject())
        {
            WalkRaw(prop.Value, $"{path}.{prop.Name}", errors);
        }
    }

    private static bool IsVectorNode(JsonElement node) =>
        (node.TryGetProperty("format", out var fmt) &&
         fmt.ValueKind == JsonValueKind.String &&
         string.Equals(fmt.GetString(), "vector", StringComparison.OrdinalIgnoreCase)) ||
        node.TryGetProperty(DimKeyword, out _);

    private static void ValidateVectorNode(JsonElement node, string path, List<string> errors)
    {
        if (!node.TryGetProperty("type", out var type) ||
            type.ValueKind != JsonValueKind.String ||
            !string.Equals(type.GetString(), "array", StringComparison.Ordinal))
        {
            errors.Add($"{path}: a vector must be declared on \"type\": \"array\"");
            return;
        }

        if (!node.TryGetProperty("items", out var items) ||
            items.ValueKind != JsonValueKind.Object ||
            !items.TryGetProperty("type", out var itemType) ||
            itemType.ValueKind != JsonValueKind.String ||
            itemType.GetString() is not ("number" or "integer"))
        {
            errors.Add($"{path}: vector items must be of type \"number\" or \"integer\"");
            return;
        }

        if (!node.TryGetProperty(DimKeyword, out var dim) || !dim.TryGetInt64(out var dimValue))
        {
            errors.Add($"{path}: vector requires an integer \"{DimKeyword}\" keyword");
            return;
        }

        if (VectorType.ValidateDim(dimValue) is { } dimError)
        {
            errors.Add($"{path}: {dimError}");
            return;
        }

        if (node.TryGetProperty(DtypeKeyword, out var dtype) &&
            (dtype.ValueKind != JsonValueKind.String || !VectorType.TryParseDtype(dtype.GetString(), out _)))
        {
            errors.Add($"{path}: \"{DtypeKeyword}\" must be one of f32, f64, f16, i8, u8");
        }
    }

    /// <summary>
    /// Extracts the vector declaration from an NJsonSchema node for the compatibility check;
    /// null when the node is not a vector. Assumes the schema already passed
    /// <see cref="ValidateRaw"/>, so malformed values simply yield null here.
    /// </summary>
    internal static VectorType? FromSchema(NJsonSchema.JsonSchema schema)
    {
        var hasFormat = string.Equals(schema.Format, "vector", StringComparison.OrdinalIgnoreCase);
        var dimRaw = schema.ExtensionData is { } ext && ext.TryGetValue(DimKeyword, out var d) ? d : null;
        if (!hasFormat && dimRaw is null)
        {
            return null;
        }

        if (!TryToInt(dimRaw, out var dim) || VectorType.ValidateDim(dim) is not null)
        {
            return null;
        }

        var dtype = VectorDtype.F32;
        if (schema.ExtensionData is { } ext2 &&
            ext2.TryGetValue(DtypeKeyword, out var dt) &&
            !VectorType.TryParseDtype(dt?.ToString(), out dtype))
        {
            return null;
        }

        return new VectorType(dim, dtype);
    }

    private static bool TryToInt(object? value, out int result)
    {
        switch (value)
        {
            case int i: result = i; return true;
            case long l and >= 1 and <= int.MaxValue: result = (int)l; return true;
            default:
                return int.TryParse(value?.ToString(), out result);
        }
    }
}
