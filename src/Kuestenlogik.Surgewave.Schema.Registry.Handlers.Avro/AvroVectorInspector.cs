using System.Text.Json;
using Kuestenlogik.Surgewave.Schema.Registry.Vectors;

namespace Kuestenlogik.Surgewave.Schema.Registry.Handlers;

/// <summary>
/// Reads vector declarations (<c>"logicalType": "vector"</c>, #14) straight from the raw Avro
/// schema JSON. Deliberately independent of Chr.Avro: the Avro spec requires readers to ignore
/// unknown logical types, so Chr.Avro's abstract model drops the annotation and its
/// <c>dim</c> attribute — exactly the data this feature validates. Walking the JSON ourselves
/// also keeps the handler's behavior stable if the library ever changes its logical-type model.
/// </summary>
internal static class AvroVectorInspector
{
    internal sealed record Inspection(
        IReadOnlyDictionary<string, VectorType> Vectors,
        IReadOnlySet<string> FieldPaths,
        IReadOnlyList<string> Errors);

    /// <summary>
    /// Walks the schema and collects every record-field path plus the vector declared at each
    /// path. A vector is valid only as an array of "float" (f32) or "double" (f64) with a
    /// positive integer "dim"; anything else lands in <c>Errors</c>.
    /// </summary>
    internal static Inspection Inspect(string schemaJson)
    {
        var vectors = new Dictionary<string, VectorType>(StringComparer.Ordinal);
        var fieldPaths = new HashSet<string>(StringComparer.Ordinal);
        var errors = new List<string>();

        using var doc = JsonDocument.Parse(schemaJson);
        Walk(doc.RootElement, "$", vectors, fieldPaths, errors);
        return new Inspection(vectors, fieldPaths, errors);
    }

    private static void Walk(
        JsonElement node, string path,
        Dictionary<string, VectorType> vectors, HashSet<string> fieldPaths, List<string> errors)
    {
        switch (node.ValueKind)
        {
            case JsonValueKind.Array: // Union: jeder Zweig kann den Vektor tragen.
                foreach (var branch in node.EnumerateArray())
                {
                    Walk(branch, path, vectors, fieldPaths, errors);
                }
                return;

            case JsonValueKind.Object:
                break;

            default: // Primitive als String ("float", Namensreferenzen) tragen nie Attribute.
                return;
        }

        if (node.TryGetProperty("logicalType", out var lt) &&
            lt.ValueKind == JsonValueKind.String &&
            string.Equals(lt.GetString(), "vector", StringComparison.OrdinalIgnoreCase))
        {
            ReadVector(node, path, vectors, errors);
        }

        if (!node.TryGetProperty("type", out var type))
        {
            return;
        }

        if (type.ValueKind is JsonValueKind.Object or JsonValueKind.Array)
        {
            // Feld-Form {"name": x, "type": {...}} oder geschachtelte Typ-Objekte.
            Walk(type, path, vectors, fieldPaths, errors);
            return;
        }

        switch (type.GetString())
        {
            case "record":
                if (node.TryGetProperty("fields", out var fields) && fields.ValueKind == JsonValueKind.Array)
                {
                    foreach (var field in fields.EnumerateArray())
                    {
                        if (field.ValueKind != JsonValueKind.Object ||
                            !field.TryGetProperty("name", out var name) ||
                            name.ValueKind != JsonValueKind.String)
                        {
                            continue;
                        }

                        var fieldPath = $"{path}.{name.GetString()}";
                        fieldPaths.Add(fieldPath);
                        if (field.TryGetProperty("type", out var fieldType))
                        {
                            Walk(fieldType, fieldPath, vectors, fieldPaths, errors);
                        }
                    }
                }
                return;

            case "array":
                if (node.TryGetProperty("items", out var items))
                {
                    Walk(items, path, vectors, fieldPaths, errors);
                }
                return;

            case "map":
                if (node.TryGetProperty("values", out var values))
                {
                    Walk(values, path, vectors, fieldPaths, errors);
                }
                return;
        }
    }

    private static void ReadVector(
        JsonElement node, string path, Dictionary<string, VectorType> vectors, List<string> errors)
    {
        if (!node.TryGetProperty("type", out var type) ||
            type.ValueKind != JsonValueKind.String ||
            !string.Equals(type.GetString(), "array", StringComparison.Ordinal))
        {
            errors.Add($"{path}: logicalType \"vector\" is only valid on an Avro array of float or double");
            return;
        }

        VectorDtype dtype;
        var itemName = ItemTypeName(node);
        switch (itemName)
        {
            case "float": dtype = VectorDtype.F32; break;
            case "double": dtype = VectorDtype.F64; break;
            default:
                errors.Add($"{path}: vector items must be \"float\" or \"double\", got \"{itemName ?? "?"}\"");
                return;
        }

        if (!node.TryGetProperty("dim", out var dim) || !dim.TryGetInt64(out var dimValue))
        {
            errors.Add($"{path}: vector requires an integer \"dim\" attribute");
            return;
        }

        if (VectorType.ValidateDim(dimValue) is { } dimError)
        {
            errors.Add($"{path}: {dimError}");
            return;
        }

        vectors[path] = new VectorType((int)dimValue, dtype);
    }

    private static string? ItemTypeName(JsonElement arrayNode)
    {
        if (!arrayNode.TryGetProperty("items", out var items))
        {
            return null;
        }

        return items.ValueKind switch
        {
            JsonValueKind.String => items.GetString(),
            JsonValueKind.Object when items.TryGetProperty("type", out var t) &&
                                      t.ValueKind == JsonValueKind.String => t.GetString(),
            _ => null,
        };
    }
}
