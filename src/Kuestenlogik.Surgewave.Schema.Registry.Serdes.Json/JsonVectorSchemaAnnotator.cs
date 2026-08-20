using System.Reflection;
using Kuestenlogik.Surgewave.Schema.Registry.Client;

namespace Kuestenlogik.Surgewave.Schema.Registry.Serdes.Json;

/// <summary>
/// Stamps <c>[SurgewaveVector(dim)]</c> members into the generated JSON Schema as
/// <c>"format": "vector"</c> with <c>x-vector-dim</c> and <c>x-vector-dtype</c> (#14), so the
/// Schema Registry can enforce that dim and dtype never change across versions. NJsonSchema
/// carries both keywords through its roundtrip, so the annotation survives registry
/// normalization.
/// </summary>
internal static class JsonVectorSchemaAnnotator
{
    internal static NJsonSchema.JsonSchema Apply(NJsonSchema.JsonSchema schema, Type type)
    {
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

            var dtype = ElementType(memberType) switch
            {
                var t when t == typeof(float) => "f32",
                var t when t == typeof(double) => "f64",
                _ => throw new InvalidOperationException(
                    $"[SurgewaveVector] on '{type.Name}.{member.Name}': element type must be " +
                    "float or double."),
            };

            var property = FindProperty(schema, member.Name) ?? throw new InvalidOperationException(
                $"[SurgewaveVector] member '{type.Name}.{member.Name}' not found in the " +
                "generated JSON Schema.");

            property.Format = "vector";
            property.ExtensionData ??= new Dictionary<string, object?>(StringComparer.Ordinal);
            property.ExtensionData["x-vector-dim"] = attr.Dim;
            property.ExtensionData["x-vector-dtype"] = dtype;
        }

        return schema;
    }

    private static NJsonSchema.JsonSchemaProperty? FindProperty(NJsonSchema.JsonSchema schema, string memberName)
    {
        foreach (var (name, property) in schema.ActualProperties)
        {
            if (string.Equals(name, memberName, StringComparison.OrdinalIgnoreCase))
            {
                return property;
            }
        }

        return null;
    }

    private static Type? ElementType(Type type)
    {
        if (type.IsArray)
        {
            return type.GetElementType();
        }

        if (type.IsGenericType && type.GetGenericArguments() is [var single])
        {
            return single;
        }

        return null;
    }
}
