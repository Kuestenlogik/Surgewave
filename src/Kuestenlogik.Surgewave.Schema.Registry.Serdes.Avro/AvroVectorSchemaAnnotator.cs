using System.Reflection;
using System.Text.Json.Nodes;
using Kuestenlogik.Surgewave.Schema.Registry.Client;

namespace Kuestenlogik.Surgewave.Schema.Registry.Serdes.Avro;

/// <summary>
/// Stamps <c>[SurgewaveVector(dim)]</c> members into the generated Avro schema as
/// <c>"logicalType": "vector", "dim": N</c> on the field's array type (#14). Chr.Avro has no
/// extension point for unknown logical types, so the annotation is applied to the schema JSON
/// after <c>JsonSchemaWriter</c> — per Avro spec readers that don't know the logical type fall
/// back to the underlying array, so the annotated schema stays valid Avro everywhere.
/// </summary>
internal static class AvroVectorSchemaAnnotator
{
    /// <summary>
    /// Returns the schema with vector annotations applied; the input unchanged when
    /// <paramref name="type"/> declares no vector members.
    /// </summary>
    internal static string Annotate(string schemaJson, Type type)
    {
        var vectors = CollectVectorMembers(type);
        if (vectors.Count == 0)
        {
            return schemaJson;
        }

        if (JsonNode.Parse(schemaJson) is not JsonObject root ||
            root["fields"] is not JsonArray fields)
        {
            throw new InvalidOperationException(
                $"[SurgewaveVector] on '{type.Name}' requires an Avro record schema, " +
                "but the generated schema has no fields.");
        }

        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var field in fields.OfType<JsonObject>())
        {
            var name = field["name"]?.GetValue<string>();
            if (name is null || !vectors.TryGetValue(name, out var vector))
            {
                continue;
            }

            seen.Add(name);
            ApplyToField(field, name, vector, type);
        }

        var missing = vectors.Keys.Where(k => !seen.Contains(k)).ToList();
        if (missing.Count > 0)
        {
            throw new InvalidOperationException(
                $"[SurgewaveVector] members not found in the generated Avro schema for " +
                $"'{type.Name}': {string.Join(", ", missing)}");
        }

        return root.ToJsonString();
    }

    private static void ApplyToField(JsonObject field, string name, (int Dim, string ItemType) vector, Type type)
    {
        // Feld-Typ kann das Array-Objekt direkt sein oder eine Union ["null", {array}].
        var typeNode = field["type"];
        var arrayNode = typeNode switch
        {
            JsonObject obj when IsArrayNode(obj) => obj,
            JsonArray union => union.OfType<JsonObject>().FirstOrDefault(IsArrayNode),
            _ => null,
        };

        if (arrayNode is null || arrayNode["items"]?.GetValue<string>() != vector.ItemType)
        {
            throw new InvalidOperationException(
                $"[SurgewaveVector] on '{type.Name}.{name}' requires the field to serialize " +
                $"as an Avro array of {vector.ItemType}.");
        }

        arrayNode["logicalType"] = "vector";
        arrayNode["dim"] = vector.Dim;
    }

    private static bool IsArrayNode(JsonObject node) =>
        node["type"]?.GetValue<string>() == "array";

    /// <summary>Feldname (case-insensitiv) → (dim, Avro-Elementtyp "float"/"double").</summary>
    private static Dictionary<string, (int Dim, string ItemType)> CollectVectorMembers(Type type)
    {
        var result = new Dictionary<string, (int, string)>(StringComparer.OrdinalIgnoreCase);
        foreach (var member in type.GetMembers(BindingFlags.Public | BindingFlags.Instance))
        {
            if (member.GetCustomAttribute<SurgewaveVectorAttribute>() is not { } attr)
            {
                continue;
            }

            var memberType = member switch
            {
                PropertyInfo p => p.PropertyType,
                FieldInfo f => f.FieldType,
                _ => null,
            };
            if (memberType is null)
            {
                continue;
            }

            var itemType = ElementType(memberType) switch
            {
                var t when t == typeof(float) => "float",
                var t when t == typeof(double) => "double",
                _ => throw new InvalidOperationException(
                    $"[SurgewaveVector] on '{type.Name}.{member.Name}': element type must be " +
                    "float or double."),
            };

            result[member.Name] = (attr.Dim, itemType);
        }

        return result;
    }

    private static Type? ElementType(Type type)
    {
        if (type.IsArray)
        {
            return type.GetElementType();
        }

        if (type.IsGenericType && type.GetGenericArguments() is [var single])
        {
            return single; // List<float>, IReadOnlyList<float>, ReadOnlyMemory<float>, …
        }

        return null;
    }
}
