using NJsonSchema;
using Kuestenlogik.Surgewave.Schema.Registry;

namespace Kuestenlogik.Surgewave.Schema.Registry.Handlers;

/// <summary>
/// Schema type handler for JSON Schema.
/// Uses NJsonSchema for parsing and compatibility checking.
/// </summary>
public sealed class JsonSchemaHandler : ISchemaTypeHandler
{
    public string TypeName => "JSON";

    public (bool IsValid, string? Error) Validate(string schemaString)
    {
        try
        {
            NJsonSchema.JsonSchema.FromJsonAsync(schemaString).GetAwaiter().GetResult();
            return (true, null);
        }
        catch (Exception ex)
        {
            return (false, ex.Message);
        }
    }

    public CompatibilityResult CheckCompatibility(
        string newSchemaString,
        IReadOnlyList<Schema> existingSchemas,
        CompatibilityMode mode)
    {
        if (mode == CompatibilityMode.None || existingSchemas.Count == 0)
        {
            return new CompatibilityResult(true);
        }

        var errors = new List<string>();

        try
        {
            var newSchema = NJsonSchema.JsonSchema.FromJsonAsync(newSchemaString).GetAwaiter().GetResult();

            foreach (var existing in existingSchemas)
            {
                var existingSchema = NJsonSchema.JsonSchema.FromJsonAsync(existing.SchemaString).GetAwaiter().GetResult();

                var (backward, forward) = mode switch
                {
                    CompatibilityMode.Backward or CompatibilityMode.BackwardTransitive => (true, false),
                    CompatibilityMode.Forward or CompatibilityMode.ForwardTransitive => (false, true),
                    CompatibilityMode.Full or CompatibilityMode.FullTransitive => (true, true),
                    _ => (false, false)
                };

                if (backward)
                {
                    var backwardErrors = CheckBackwardCompatibility(newSchema, existingSchema);
                    errors.AddRange(backwardErrors.Select(e => $"Backward compatibility error with version {existing.Version}: {e}"));
                }

                if (forward)
                {
                    var forwardErrors = CheckBackwardCompatibility(existingSchema, newSchema);
                    errors.AddRange(forwardErrors.Select(e => $"Forward compatibility error with version {existing.Version}: {e}"));
                }
            }
        }
        catch (Exception ex)
        {
            errors.Add($"Error parsing JSON Schema: {ex.Message}");
        }

        return new CompatibilityResult(errors.Count == 0, errors.Count > 0 ? errors : null);
    }

    private static List<string> CheckBackwardCompatibility(NJsonSchema.JsonSchema readerSchema, NJsonSchema.JsonSchema writerSchema)
    {
        var errors = new List<string>();

        // Check type compatibility
        if (readerSchema.Type != writerSchema.Type && readerSchema.Type != JsonObjectType.None && writerSchema.Type != JsonObjectType.None)
        {
            errors.Add($"Type mismatch: reader expects {readerSchema.Type}, writer has {writerSchema.Type}");
        }

        // Check required properties - reader shouldn't require more than writer
        if (readerSchema.RequiredProperties.Count > 0 && writerSchema.RequiredProperties.Count > 0)
        {
            foreach (var required in readerSchema.RequiredProperties)
            {
                if (!writerSchema.RequiredProperties.Contains(required) &&
                    !writerSchema.Properties.ContainsKey(required))
                {
                    // Check if there's a default
                    if (readerSchema.Properties.TryGetValue(required, out var prop) && prop.Default == null)
                    {
                        errors.Add($"Reader requires property '{required}' that writer doesn't have");
                    }
                }
            }
        }

        // Check that reader can read all writer properties
        foreach (var (name, writerProp) in writerSchema.Properties)
        {
            if (readerSchema.Properties.TryGetValue(name, out var readerProp))
            {
                // Check nested compatibility
                var nestedErrors = CheckBackwardCompatibility(readerProp, writerProp);
                errors.AddRange(nestedErrors.Select(e => $"Property '{name}': {e}"));
            }
        }

        return errors;
    }
}
