using System.Text.RegularExpressions;

namespace Kuestenlogik.Surgewave.Schema.Registry.Handlers;

/// <summary>
/// Schema type handler for Thrift IDL schemas.
/// Validates Thrift schema structure and checks field-level compatibility.
/// </summary>
public sealed partial class ThriftSchemaHandler : ISchemaTypeHandler
{
    /// <inheritdoc />
    public string TypeName => "THRIFT";

    /// <inheritdoc />
    public (bool IsValid, string? Error) Validate(string schemaString)
    {
        try
        {
            var structs = ParseThriftStructs(schemaString);

            if (structs.Count == 0)
            {
                return (false, "Invalid Thrift schema: no struct definitions found");
            }

            // Validate field IDs
            foreach (var (structName, fields) in structs)
            {
                var fieldIds = fields.Select(f => f.FieldId).ToList();
                var duplicateIds = fieldIds.GroupBy(id => id).Where(g => g.Count() > 1).Select(g => g.Key);
                if (duplicateIds.Any())
                {
                    return (false, $"Duplicate field IDs in struct '{structName}': {string.Join(", ", duplicateIds)}");
                }

                var fieldNames = fields.Select(f => f.Name).ToList();
                var duplicateNames = fieldNames.GroupBy(n => n).Where(g => g.Count() > 1).Select(g => g.Key);
                if (duplicateNames.Any())
                {
                    return (false, $"Duplicate field names in struct '{structName}': {string.Join(", ", duplicateNames)}");
                }
            }

            return (true, null);
        }
        catch (Exception ex)
        {
            return (false, $"Failed to parse Thrift schema: {ex.Message}");
        }
    }

    /// <inheritdoc />
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
        var newStructs = ParseThriftStructs(newSchemaString);

        foreach (var existing in existingSchemas)
        {
            var existingStructs = ParseThriftStructs(existing.SchemaString);

            var (backward, forward) = mode switch
            {
                CompatibilityMode.Backward or CompatibilityMode.BackwardTransitive => (true, false),
                CompatibilityMode.Forward or CompatibilityMode.ForwardTransitive => (false, true),
                CompatibilityMode.Full or CompatibilityMode.FullTransitive => (true, true),
                _ => (false, false)
            };

            foreach (var (structName, existingFields) in existingStructs)
            {
                if (!newStructs.TryGetValue(structName, out var newFields))
                {
                    if (backward)
                    {
                        errors.Add($"Backward compatibility error with version {existing.Version}: struct '{structName}' was removed");
                    }
                    continue;
                }

                var existingById = existingFields.ToDictionary(f => f.FieldId);
                var newById = newFields.ToDictionary(f => f.FieldId);

                if (backward)
                {
                    foreach (var (fieldId, existingField) in existingById)
                    {
                        if (newById.TryGetValue(fieldId, out var newField))
                        {
                            if (!AreThriftTypesCompatible(newField.Type, existingField.Type))
                            {
                                errors.Add($"Backward compatibility error with version {existing.Version}: " +
                                    $"Field {fieldId} in '{structName}' type changed from {existingField.Type} to {newField.Type}");
                            }
                        }
                    }
                }

                if (forward)
                {
                    foreach (var (fieldId, newField) in newById)
                    {
                        if (existingById.TryGetValue(fieldId, out var existingField))
                        {
                            if (!AreThriftTypesCompatible(existingField.Type, newField.Type))
                            {
                                errors.Add($"Forward compatibility error with version {existing.Version}: " +
                                    $"Field {fieldId} in '{structName}' type changed from {newField.Type} to {existingField.Type}");
                            }
                        }
                    }
                }
            }
        }

        return new CompatibilityResult(errors.Count == 0, errors.Count > 0 ? errors : null);
    }

    private static Dictionary<string, List<ThriftField>> ParseThriftStructs(string schemaString)
    {
        var structs = new Dictionary<string, List<ThriftField>>(StringComparer.Ordinal);

        foreach (Match match in StructRegex().Matches(schemaString))
        {
            var structName = match.Groups[1].Value;
            var body = match.Groups[2].Value;
            var fields = new List<ThriftField>();

            foreach (Match fieldMatch in FieldRegex().Matches(body))
            {
                var fieldId = int.Parse(fieldMatch.Groups[1].Value);
                var requiredness = fieldMatch.Groups[2].Value;
                var type = fieldMatch.Groups[3].Value.Trim();
                var name = fieldMatch.Groups[4].Value;

                fields.Add(new ThriftField(fieldId, name, type, requiredness));
            }

            structs[structName] = fields;
        }

        return structs;
    }

    private static bool AreThriftTypesCompatible(string type1, string type2)
    {
        if (string.Equals(type1, type2, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        // Thrift wire type compatibility groups
        var wireTypeGroups = new[]
        {
            new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "byte", "i8", "i16", "i32", "i64" },
            new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "string", "binary" }
        };

        foreach (var group in wireTypeGroups)
        {
            if (group.Contains(type1) && group.Contains(type2))
            {
                return true;
            }
        }

        return false;
    }

    [GeneratedRegex(@"struct\s+(\w+)\s*\{([^{}]*(?:\{[^{}]*\}[^{}]*)*)\}", RegexOptions.Singleline)]
    private static partial Regex StructRegex();

    [GeneratedRegex(@"(\d+)\s*:\s*(optional|required)?\s*([\w.<>\s,]+?)\s+(\w+)\s*[;,]")]
    private static partial Regex FieldRegex();

    private sealed record ThriftField(int FieldId, string Name, string Type, string Requiredness);
}
