using System.Text.RegularExpressions;

namespace Kuestenlogik.Surgewave.Schema.Registry.Handlers;

/// <summary>
/// Schema type handler for Bond schemas.
/// Validates Bond IDL schema structure and checks field-level compatibility.
/// </summary>
public sealed partial class BondSchemaHandler : ISchemaTypeHandler
{
    /// <inheritdoc />
    public string TypeName => "BOND";

    /// <inheritdoc />
    public (bool IsValid, string? Error) Validate(string schemaString)
    {
        try
        {
            var structs = ParseBondStructs(schemaString);

            if (structs.Count == 0)
            {
                return (false, "Invalid Bond schema: no struct definitions found");
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
            return (false, $"Failed to parse Bond schema: {ex.Message}");
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
        var newStructs = ParseBondStructs(newSchemaString);

        foreach (var existing in existingSchemas)
        {
            var existingStructs = ParseBondStructs(existing.SchemaString);

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
                            if (!AreBondTypesCompatible(newField.Type, existingField.Type))
                            {
                                errors.Add($"Backward compatibility error with version {existing.Version}: " +
                                    $"Field {ordinal} in '{structName}' type changed from {existingField.Type} to {newField.Type}");
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
                            if (!AreBondTypesCompatible(existingField.Type, newField.Type))
                            {
                                errors.Add($"Forward compatibility error with version {existing.Version}: " +
                                    $"Field {ordinal} in '{structName}' type changed from {newField.Type} to {existingField.Type}");
                            }
                        }
                    }
                }
            }
        }

        return new CompatibilityResult(errors.Count == 0, errors.Count > 0 ? errors : null);
    }

    private static Dictionary<string, List<BondField>> ParseBondStructs(string schemaString)
    {
        var structs = new Dictionary<string, List<BondField>>(StringComparer.Ordinal);

        foreach (Match match in StructRegex().Matches(schemaString))
        {
            var structName = match.Groups[1].Value;
            var body = match.Groups[2].Value;
            var fields = new List<BondField>();

            foreach (Match fieldMatch in FieldRegex().Matches(body))
            {
                var ordinal = int.Parse(fieldMatch.Groups[1].Value);
                var modifier = fieldMatch.Groups[2].Value;
                var type = fieldMatch.Groups[3].Value.Trim();
                var name = fieldMatch.Groups[4].Value;

                fields.Add(new BondField(ordinal, name, type, modifier));
            }

            structs[structName] = fields;
        }

        return structs;
    }

    private static bool AreBondTypesCompatible(string type1, string type2)
    {
        if (string.Equals(type1, type2, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        // Bond wire type compatibility groups
        var wireTypeGroups = new[]
        {
            new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "int8", "int16", "int32", "int64" },
            new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "uint8", "uint16", "uint32", "uint64" },
            new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "float", "double" },
            new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "string", "wstring" }
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

    [GeneratedRegex(@"struct\s+(\w+)(?:\s*:\s*\w+)?\s*\{([^{}]*(?:\{[^{}]*\}[^{}]*)*)\}", RegexOptions.Singleline)]
    private static partial Regex StructRegex();

    [GeneratedRegex(@"(\d+)\s*:\s*(optional|required)?\s*([\w.<>\s,]+?)\s+(\w+)\s*;")]
    private static partial Regex FieldRegex();

    private sealed record BondField(int Ordinal, string Name, string Type, string Modifier);
}
