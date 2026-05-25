using System.Text.RegularExpressions;

namespace Kuestenlogik.Surgewave.Schema.Registry.Handlers;

/// <summary>
/// Schema type handler for Cap'n Proto schemas.
/// Validates Cap'n Proto schema structure and checks field-level compatibility.
/// </summary>
public sealed partial class CapnProtoSchemaHandler : ISchemaTypeHandler
{
    /// <inheritdoc />
    public string TypeName => "CAPNPROTO";

    /// <inheritdoc />
    public (bool IsValid, string? Error) Validate(string schemaString)
    {
        try
        {
            var structs = ParseCapnProtoStructs(schemaString);

            if (structs.Count == 0)
            {
                return (false, "Invalid Cap'n Proto schema: no struct definitions found");
            }

            // Validate field ordinals
            foreach (var (structName, fields) in structs)
            {
                var ordinals = fields.Select(f => f.Ordinal).ToList();
                var duplicateOrdinals = ordinals.GroupBy(o => o).Where(g => g.Count() > 1).Select(g => g.Key);
                if (duplicateOrdinals.Any())
                {
                    return (false, $"Duplicate field ordinals in struct '{structName}': {string.Join(", ", duplicateOrdinals)}");
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
            return (false, $"Failed to parse Cap'n Proto schema: {ex.Message}");
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
        var newStructs = ParseCapnProtoStructs(newSchemaString);

        foreach (var existing in existingSchemas)
        {
            var existingStructs = ParseCapnProtoStructs(existing.SchemaString);

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

                var existingByOrdinal = existingFields.ToDictionary(f => f.Ordinal);
                var newByOrdinal = newFields.ToDictionary(f => f.Ordinal);

                if (backward)
                {
                    foreach (var (ordinal, existingField) in existingByOrdinal)
                    {
                        if (newByOrdinal.TryGetValue(ordinal, out var newField))
                        {
                            if (!AreCapnProtoTypesCompatible(newField.Type, existingField.Type))
                            {
                                errors.Add($"Backward compatibility error with version {existing.Version}: " +
                                    $"Field @{ordinal} in '{structName}' type changed from {existingField.Type} to {newField.Type}");
                            }
                        }
                    }
                }

                if (forward)
                {
                    foreach (var (ordinal, newField) in newByOrdinal)
                    {
                        if (existingByOrdinal.TryGetValue(ordinal, out var existingField))
                        {
                            if (!AreCapnProtoTypesCompatible(existingField.Type, newField.Type))
                            {
                                errors.Add($"Forward compatibility error with version {existing.Version}: " +
                                    $"Field @{ordinal} in '{structName}' type changed from {newField.Type} to {existingField.Type}");
                            }
                        }
                    }
                }
            }
        }

        return new CompatibilityResult(errors.Count == 0, errors.Count > 0 ? errors : null);
    }

    private static Dictionary<string, List<CapnProtoField>> ParseCapnProtoStructs(string schemaString)
    {
        var structs = new Dictionary<string, List<CapnProtoField>>(StringComparer.Ordinal);

        foreach (Match match in StructRegex().Matches(schemaString))
        {
            var structName = match.Groups[1].Value;
            var body = match.Groups[2].Value;
            var fields = new List<CapnProtoField>();

            foreach (Match fieldMatch in FieldRegex().Matches(body))
            {
                var name = fieldMatch.Groups[1].Value;
                var ordinal = int.Parse(fieldMatch.Groups[2].Value);
                var type = fieldMatch.Groups[3].Value.Trim();

                fields.Add(new CapnProtoField(ordinal, name, type));
            }

            structs[structName] = fields;
        }

        return structs;
    }

    private static bool AreCapnProtoTypesCompatible(string type1, string type2)
    {
        if (string.Equals(type1, type2, StringComparison.Ordinal))
        {
            return true;
        }

        // Cap'n Proto wire type compatibility groups
        var wireTypeGroups = new[]
        {
            new HashSet<string>(StringComparer.Ordinal) { "Int8", "Int16", "Int32", "Int64" },
            new HashSet<string>(StringComparer.Ordinal) { "UInt8", "UInt16", "UInt32", "UInt64" },
            new HashSet<string>(StringComparer.Ordinal) { "Float32", "Float64" },
            new HashSet<string>(StringComparer.Ordinal) { "Text", "Data" }
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

    [GeneratedRegex(@"(\w+)\s+@(\d+)\s*:\s*([\w.()]+)\s*;")]
    private static partial Regex FieldRegex();

    private sealed record CapnProtoField(int Ordinal, string Name, string Type);
}
