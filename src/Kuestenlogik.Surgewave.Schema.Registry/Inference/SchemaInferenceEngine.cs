using System.Text;
using System.Text.Json;

namespace Kuestenlogik.Surgewave.Schema.Registry.Inference;

/// <summary>
/// Core engine that analyzes JSON messages and derives a JSON Schema.
/// Supports single-message inference, batch inference, and schema merging.
/// Thread-safe for concurrent usage.
/// </summary>
public sealed class SchemaInferenceEngine
{
    private static readonly JsonSerializerOptions s_serializeOptions = new() { WriteIndented = true };

    /// <summary>
    /// Infer a JSON Schema from a single JSON message.
    /// </summary>
    /// <param name="messageValue">Raw UTF-8 JSON bytes of the message value.</param>
    /// <returns>An inferred schema definition, or null if the message is not valid JSON.</returns>
    public JsonSchemaDefinition? InferFromMessage(ReadOnlySpan<byte> messageValue)
    {
        if (messageValue.IsEmpty)
        {
            return null;
        }

        try
        {
            var reader = new Utf8JsonReader(messageValue);
            using var doc = JsonDocument.ParseValue(ref reader);
            var schema = InferFromElement(doc.RootElement);
            schema.SampleCount = 1;
            return schema;
        }
        catch (JsonException)
        {
            return null;
        }
    }

    /// <summary>
    /// Infer a JSON Schema from a single JSON message provided as ReadOnlyMemory.
    /// </summary>
    public JsonSchemaDefinition? InferFromMessage(ReadOnlyMemory<byte> messageValue)
    {
        return InferFromMessage(messageValue.Span);
    }

    /// <summary>
    /// Infer schema from a batch of messages for better accuracy.
    /// Merges all individual message schemas into a unified schema.
    /// </summary>
    /// <param name="messages">Collection of raw UTF-8 JSON message bytes.</param>
    /// <returns>A merged schema representing all messages, or null if no valid messages.</returns>
    public JsonSchemaDefinition? InferFromBatch(IReadOnlyList<ReadOnlyMemory<byte>> messages)
    {
        if (messages.Count == 0)
        {
            return null;
        }

        JsonSchemaDefinition? merged = null;

        foreach (var message in messages)
        {
            var schema = InferFromMessage(message.Span);
            if (schema is null)
            {
                continue;
            }

            merged = merged is null ? schema : MergeSchemas(merged, schema);
        }

        return merged;
    }

    /// <summary>
    /// Merge two inferred schemas into a unified schema.
    /// Handles type widening (int to long to double), optional fields, and arrays.
    /// </summary>
    /// <param name="existing">The existing accumulated schema.</param>
    /// <param name="newSchema">A newly inferred schema to merge in.</param>
    /// <returns>The merged schema.</returns>
    public JsonSchemaDefinition MergeSchemas(JsonSchemaDefinition existing, JsonSchemaDefinition newSchema)
    {
        var merged = new JsonSchemaDefinition
        {
            Type = WidenType(existing.Type, newSchema.Type),
            SampleCount = existing.SampleCount + newSchema.SampleCount
        };

        if (merged.Type != "object")
        {
            // For non-object root types, merge items if array
            if (merged.Type == "array" && existing.Items is not null && newSchema.Items is not null)
            {
                merged.Items = MergeSchemas(existing.Items, newSchema.Items);
            }
            else
            {
                merged.Items = existing.Items ?? newSchema.Items;
            }

            return merged;
        }

        // Merge properties from both schemas
        var allPropertyNames = new HashSet<string>(existing.Properties.Keys);
        allPropertyNames.UnionWith(newSchema.Properties.Keys);

        foreach (var propName in allPropertyNames)
        {
            var existsInExisting = existing.Properties.TryGetValue(propName, out var existingProp);
            var existsInNew = newSchema.Properties.TryGetValue(propName, out var newProp);

            if (existsInExisting && existsInNew)
            {
                // Property exists in both: merge types
                merged.Properties[propName] = MergeProperties(existingProp!, newProp!);
            }
            else if (existsInExisting)
            {
                // Property only in existing: it becomes optional
                var cloned = CloneProperty(existingProp!);
                merged.Properties[propName] = cloned;
            }
            else
            {
                // Property only in new: it becomes optional
                var cloned = CloneProperty(newProp!);
                merged.Properties[propName] = cloned;
            }
        }

        // A field is required only if it was present in ALL sampled messages
        foreach (var propName in allPropertyNames)
        {
            var inExisting = existing.Properties.ContainsKey(propName);
            var inNew = newSchema.Properties.ContainsKey(propName);
            var wasRequiredInExisting = existing.Required.Contains(propName);
            var wasRequiredInNew = newSchema.Required.Contains(propName);

            // Required only if it was required in existing AND present/required in new
            if (inExisting && inNew && wasRequiredInExisting && wasRequiredInNew)
            {
                merged.Required.Add(propName);
            }
        }

        return merged;
    }

    /// <summary>
    /// Serialize an inferred schema to a JSON Schema string.
    /// </summary>
    public static string ToJsonSchemaString(JsonSchemaDefinition definition)
    {
        var schemaObject = BuildJsonSchemaObject(definition);
        schemaObject["$schema"] = "https://json-schema.org/draft/2020-12/schema";
        return JsonSerializer.Serialize(schemaObject, s_serializeOptions);
    }

    private static Dictionary<string, object> BuildJsonSchemaObject(JsonSchemaDefinition definition)
    {
        var schema = new Dictionary<string, object>
        {
            ["type"] = definition.Type
        };

        if (definition.Type == "object" && definition.Properties.Count > 0)
        {
            var properties = new Dictionary<string, object>();
            foreach (var (name, prop) in definition.Properties)
            {
                properties[name] = BuildPropertyObject(prop);
            }
            schema["properties"] = properties;

            if (definition.Required.Count > 0)
            {
                schema["required"] = definition.Required.OrderBy(r => r, StringComparer.Ordinal).ToList();
            }
        }

        if (definition.Type == "array" && definition.Items is not null)
        {
            schema["items"] = BuildJsonSchemaObject(definition.Items);
        }

        return schema;
    }

    private static Dictionary<string, object> BuildPropertyObject(JsonSchemaProperty property)
    {
        var obj = new Dictionary<string, object>();

        if (property.Nullable)
        {
            obj["type"] = new[] { property.Type, "null" };
        }
        else
        {
            obj["type"] = property.Type;
        }

        if (property.Format is not null)
        {
            obj["format"] = property.Format;
        }

        if (property.Type == "array" && property.Items is not null)
        {
            obj["items"] = BuildJsonSchemaObject(property.Items);
        }

        if (property.Type == "object" && property.ObjectSchema is not null)
        {
            if (property.ObjectSchema.Properties.Count > 0)
            {
                var properties = new Dictionary<string, object>();
                foreach (var (name, prop) in property.ObjectSchema.Properties)
                {
                    properties[name] = BuildPropertyObject(prop);
                }
                obj["properties"] = properties;

                if (property.ObjectSchema.Required.Count > 0)
                {
                    obj["required"] = property.ObjectSchema.Required
                        .OrderBy(r => r, StringComparer.Ordinal).ToList();
                }
            }
        }

        return obj;
    }

    private JsonSchemaDefinition InferFromElement(JsonElement element)
    {
        var definition = new JsonSchemaDefinition();

        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                definition.Type = "object";
                foreach (var property in element.EnumerateObject())
                {
                    var prop = InferPropertyFromElement(property.Value);
                    prop.SeenCount = 1;
                    definition.Properties[property.Name] = prop;
                    definition.Required.Add(property.Name);
                }
                break;

            case JsonValueKind.Array:
                definition.Type = "array";
                definition.Items = InferArrayItemSchema(element);
                break;

            case JsonValueKind.String:
                definition.Type = "string";
                break;

            case JsonValueKind.Number:
                definition.Type = InferNumberType(element);
                break;

            case JsonValueKind.True:
            case JsonValueKind.False:
                definition.Type = "boolean";
                break;

            case JsonValueKind.Null:
                definition.Type = "null";
                break;

            default:
                definition.Type = "string";
                break;
        }

        return definition;
    }

    private JsonSchemaProperty InferPropertyFromElement(JsonElement element)
    {
        var property = new JsonSchemaProperty();

        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                property.Type = "object";
                property.ObjectSchema = InferFromElement(element);
                break;

            case JsonValueKind.Array:
                property.Type = "array";
                property.Items = InferArrayItemSchema(element);
                break;

            case JsonValueKind.String:
                property.Type = "string";
                property.Format = FormatDetector.DetectFormat(element.GetString()!);
                break;

            case JsonValueKind.Number:
                property.Type = InferNumberType(element);
                break;

            case JsonValueKind.True:
            case JsonValueKind.False:
                property.Type = "boolean";
                break;

            case JsonValueKind.Null:
                property.Type = "string"; // Default null to string, mark as nullable
                property.Nullable = true;
                break;

            default:
                property.Type = "string";
                break;
        }

        return property;
    }

    private JsonSchemaDefinition? InferArrayItemSchema(JsonElement arrayElement)
    {
        JsonSchemaDefinition? itemSchema = null;

        foreach (var item in arrayElement.EnumerateArray())
        {
            var elementSchema = InferFromElement(item);
            elementSchema.SampleCount = 1;
            itemSchema = itemSchema is null ? elementSchema : MergeSchemas(itemSchema, elementSchema);
        }

        return itemSchema;
    }

    private static string InferNumberType(JsonElement element)
    {
        // Try to determine if integer or floating-point
        if (element.TryGetInt32(out _))
        {
            return "integer";
        }
        if (element.TryGetInt64(out _))
        {
            return "integer";
        }
        return "number";
    }

    private JsonSchemaProperty MergeProperties(JsonSchemaProperty existing, JsonSchemaProperty incoming)
    {
        var merged = new JsonSchemaProperty
        {
            Type = WidenType(existing.Type, incoming.Type),
            Nullable = existing.Nullable || incoming.Nullable,
            SeenCount = existing.SeenCount + incoming.SeenCount,
            Format = MergeFormat(existing.Format, incoming.Format, existing.Type, incoming.Type)
        };

        // Merge nested objects
        if (merged.Type == "object")
        {
            if (existing.ObjectSchema is not null && incoming.ObjectSchema is not null)
            {
                merged.ObjectSchema = MergeSchemas(existing.ObjectSchema, incoming.ObjectSchema);
            }
            else
            {
                merged.ObjectSchema = existing.ObjectSchema ?? incoming.ObjectSchema;
            }
        }

        // Merge array items
        if (merged.Type == "array")
        {
            if (existing.Items is not null && incoming.Items is not null)
            {
                merged.Items = MergeSchemas(existing.Items, incoming.Items);
            }
            else
            {
                merged.Items = existing.Items ?? incoming.Items;
            }
        }

        return merged;
    }

    private static JsonSchemaProperty CloneProperty(JsonSchemaProperty source)
    {
        return new JsonSchemaProperty
        {
            Type = source.Type,
            Format = source.Format,
            Nullable = source.Nullable,
            Items = source.Items,
            ObjectSchema = source.ObjectSchema,
            SeenCount = source.SeenCount
        };
    }

    /// <summary>
    /// Widen types when conflicts arise.
    /// integer + number = number
    /// any + string = string (string is the widest type)
    /// any + null = nullable
    /// </summary>
    private static string WidenType(string type1, string type2)
    {
        if (type1 == type2)
        {
            return type1;
        }

        // null merges don't change type
        if (type1 == "null") return type2;
        if (type2 == "null") return type1;

        // integer + number = number
        if ((type1 == "integer" && type2 == "number") ||
            (type1 == "number" && type2 == "integer"))
        {
            return "number";
        }

        // Any other conflict: widen to string
        return "string";
    }

    /// <summary>
    /// Merge format hints. Keep the format only if both agree.
    /// </summary>
    private static string? MergeFormat(string? format1, string? format2, string type1, string type2)
    {
        // If types differ, format becomes meaningless
        if (type1 != type2)
        {
            return null;
        }

        if (format1 == format2)
        {
            return format1;
        }

        // Formats disagree: drop the hint
        return null;
    }
}
