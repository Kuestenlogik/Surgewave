using System.Collections.Concurrent;
using System.Text.Json;
using Kuestenlogik.Surgewave.Control.Models.Pipeline;

namespace Kuestenlogik.Surgewave.Control.Services;

/// <summary>
/// Service for retrieving and parsing schemas for structured data preview.
/// </summary>
public sealed class SchemaPreviewService
{
    private readonly ISchemaRegistryClient _schemaClient;
    private readonly ConcurrentDictionary<string, SchemaFieldInfo[]?> _schemaCache = new();

    public SchemaPreviewService(ISchemaRegistryClient schemaClient)
    {
        _schemaClient = schemaClient;
    }

    /// <summary>
    /// Try to get schema fields for a topic.
    /// </summary>
    public async Task<SchemaFieldInfo[]?> GetSchemaForTopicAsync(string topicName)
    {
        if (_schemaCache.TryGetValue(topicName, out var cached))
            return cached;

        try
        {
            var subject = $"{topicName}-value";
            var schema = await _schemaClient.GetLatestSchemaAsync(subject);
            if (schema == null)
            {
                _schemaCache[topicName] = null;
                return null;
            }

            var fields = ParseSchemaFields(schema.Schema, schema.SchemaType);
            _schemaCache[topicName] = fields;
            return fields;
        }
        catch
        {
            _schemaCache[topicName] = null;
            return null;
        }
    }

    /// <summary>
    /// Parse schema text to extract field info.
    /// </summary>
    public static SchemaFieldInfo[] ParseSchemaFields(string schemaText, string? schemaType)
    {
        try
        {
            if (string.Equals(schemaType, "AVRO", StringComparison.OrdinalIgnoreCase))
            {
                return ParseAvroFields(schemaText);
            }

            // Default: JSON Schema
            return ParseJsonSchemaFields(schemaText);
        }
        catch
        {
            return [];
        }
    }

    private static SchemaFieldInfo[] ParseJsonSchemaFields(string schemaText)
    {
        using var doc = JsonDocument.Parse(schemaText);
        var root = doc.RootElement;

        if (!root.TryGetProperty("properties", out var properties))
            return [];

        var required = new HashSet<string>();
        if (root.TryGetProperty("required", out var requiredArr) &&
            requiredArr.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in requiredArr.EnumerateArray())
            {
                if (item.GetString() is { } r)
                    required.Add(r);
            }
        }

        var fields = new List<SchemaFieldInfo>();
        foreach (var prop in properties.EnumerateObject())
        {
            var type = "string";
            string? description = null;

            if (prop.Value.TryGetProperty("type", out var typeProp))
                type = typeProp.GetString() ?? "string";
            if (prop.Value.TryGetProperty("description", out var descProp))
                description = descProp.GetString();

            fields.Add(new SchemaFieldInfo(prop.Name, type, description, required.Contains(prop.Name)));
        }

        return fields.ToArray();
    }

    private static SchemaFieldInfo[] ParseAvroFields(string schemaText)
    {
        using var doc = JsonDocument.Parse(schemaText);
        var root = doc.RootElement;

        if (!root.TryGetProperty("fields", out var fieldsArr) ||
            fieldsArr.ValueKind != JsonValueKind.Array)
            return [];

        var fields = new List<SchemaFieldInfo>();
        foreach (var field in fieldsArr.EnumerateArray())
        {
            var name = field.TryGetProperty("name", out var nameProp) ? nameProp.GetString() ?? "" : "";
            var type = "string";
            var isRequired = true;
            string? description = null;

            if (field.TryGetProperty("type", out var typeProp))
            {
                if (typeProp.ValueKind == JsonValueKind.String)
                {
                    type = typeProp.GetString() ?? "string";
                }
                else if (typeProp.ValueKind == JsonValueKind.Array)
                {
                    // Union type like ["null", "string"]
                    var types = new List<string>();
                    foreach (var t in typeProp.EnumerateArray())
                    {
                        if (t.GetString() is { } ts)
                            types.Add(ts);
                    }
                    isRequired = !types.Contains("null");
                    type = string.Join("|", types.Where(t => t != "null"));
                }
            }

            if (field.TryGetProperty("doc", out var docProp))
                description = docProp.GetString();

            fields.Add(new SchemaFieldInfo(name, type, description, isRequired));
        }

        return fields.ToArray();
    }

    /// <summary>
    /// Deserialize a raw value using schema fields.
    /// </summary>
    public static Dictionary<string, string?>? DeserializeWithSchema(string rawValue, SchemaFieldInfo[] fields)
    {
        try
        {
            using var doc = JsonDocument.Parse(rawValue);
            var result = new Dictionary<string, string?>();

            foreach (var field in fields)
            {
                if (doc.RootElement.TryGetProperty(field.Name, out var value))
                {
                    result[field.Name] = value.ValueKind == JsonValueKind.Null
                        ? null
                        : value.ToString();
                }
                else
                {
                    result[field.Name] = null;
                }
            }

            return result;
        }
        catch
        {
            return null;
        }
    }
}
