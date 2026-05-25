using System.Text;
using System.Text.Json;

namespace Kuestenlogik.Surgewave.Schema.Registry.Evolution;

/// <summary>
/// Compares two JSON Schema versions and produces a detailed change analysis.
/// Works entirely rule-based — no LLM required.
/// </summary>
public sealed class SchemaEvolutionAnalyzer
{
    private static readonly JsonDocumentOptions s_docOptions = new() { AllowTrailingCommas = true, CommentHandling = JsonCommentHandling.Skip };

    /// <summary>
    /// Compare two schema versions and return a detailed change descriptor.
    /// </summary>
    /// <param name="oldSchemaJson">The old schema JSON string.</param>
    /// <param name="newSchemaJson">The new schema JSON string.</param>
    /// <param name="subject">The subject name.</param>
    /// <param name="oldVersion">The old version number.</param>
    /// <param name="newVersion">The new version number.</param>
    /// <returns>A <see cref="SchemaChange"/> describing all detected changes.</returns>
    public SchemaChange AnalyzeChanges(string oldSchemaJson, string newSchemaJson, string subject, int oldVersion, int newVersion)
    {
        var oldProps = ExtractProperties(oldSchemaJson);
        var newProps = ExtractProperties(newSchemaJson);
        var oldRequired = ExtractRequired(oldSchemaJson);
        var newRequired = ExtractRequired(newSchemaJson);

        var fieldChanges = new List<FieldChange>();

        // Detect removed fields
        foreach (var (name, oldType) in oldProps)
        {
            if (!newProps.ContainsKey(name))
            {
                fieldChanges.Add(new FieldChange
                {
                    FieldName = name,
                    Type = FieldChangeType.Removed,
                    OldType = oldType,
                    NewType = null,
                    Breaking = BreakingLevel.Major
                });
            }
        }

        // Detect added fields
        foreach (var (name, newType) in newProps)
        {
            if (!oldProps.ContainsKey(name))
            {
                var isRequired = newRequired.Contains(name);
                var hasDefault = !isRequired; // If not required, treat as having an implicit default (null)

                fieldChanges.Add(new FieldChange
                {
                    FieldName = name,
                    Type = FieldChangeType.Added,
                    OldType = null,
                    NewType = newType,
                    HasDefault = hasDefault,
                    Breaking = isRequired ? BreakingLevel.Minor : BreakingLevel.None
                });
            }
        }

        // Detect type changes and nullability changes on fields present in both
        foreach (var (name, oldType) in oldProps)
        {
            if (!newProps.TryGetValue(name, out var newType))
            {
                continue;
            }

            var oldNullable = IsNullableType(oldType);
            var newNullable = IsNullableType(newType);
            var oldBaseType = GetBaseType(oldType);
            var newBaseType = GetBaseType(newType);

            if (!string.Equals(oldBaseType, newBaseType, StringComparison.Ordinal))
            {
                fieldChanges.Add(new FieldChange
                {
                    FieldName = name,
                    Type = FieldChangeType.TypeChanged,
                    OldType = oldType,
                    NewType = newType,
                    Breaking = BreakingLevel.Major
                });
            }
            else if (!oldNullable && newNullable)
            {
                fieldChanges.Add(new FieldChange
                {
                    FieldName = name,
                    Type = FieldChangeType.MadeNullable,
                    OldType = oldType,
                    NewType = newType,
                    Breaking = BreakingLevel.None
                });
            }
            else if (oldNullable && !newNullable)
            {
                fieldChanges.Add(new FieldChange
                {
                    FieldName = name,
                    Type = FieldChangeType.MadeRequired,
                    OldType = oldType,
                    NewType = newType,
                    Breaking = BreakingLevel.Major
                });
            }
            else
            {
                // Check required status changed
                var wasRequired = oldRequired.Contains(name);
                var nowRequired = newRequired.Contains(name);

                if (!wasRequired && nowRequired)
                {
                    fieldChanges.Add(new FieldChange
                    {
                        FieldName = name,
                        Type = FieldChangeType.MadeRequired,
                        OldType = oldType,
                        NewType = newType,
                        Breaking = BreakingLevel.Major
                    });
                }
                else if (wasRequired && !nowRequired)
                {
                    fieldChanges.Add(new FieldChange
                    {
                        FieldName = name,
                        Type = FieldChangeType.MadeNullable,
                        OldType = oldType,
                        NewType = newType,
                        Breaking = BreakingLevel.None
                    });
                }
            }
        }

        // Detect renames: heuristic — same base type, old removed + new added
        DetectRenames(fieldChanges);

        var changeType = ClassifyOverallChangeType(fieldChanges);
        var breaking = AssessBreakingLevel(fieldChanges);

        return new SchemaChange
        {
            SubjectName = subject,
            OldVersion = oldVersion,
            NewVersion = newVersion,
            ChangeType = changeType,
            FieldChanges = fieldChanges,
            Breaking = breaking
        };
    }

    /// <summary>
    /// Determine the overall breaking level from a list of field changes.
    /// </summary>
    public BreakingLevel AssessBreakingLevel(SchemaChange change)
    {
        return AssessBreakingLevel(change.FieldChanges);
    }

    /// <summary>
    /// Generate a complete impact report for a schema change.
    /// </summary>
    public SchemaImpactReport GenerateImpactReport(SchemaChange change)
    {
        var summary = GenerateSummary(change);
        var steps = GenerateMigrationSteps(change);
        var codeGen = new SchemaMigrationCodeGenerator();
        var className = DeriveClassName(change.SubjectName);
        var generatedCode = codeGen.GenerateMigrationCode(change, className);

        return new SchemaImpactReport
        {
            Change = change,
            Summary = summary,
            AffectedConsumers = [], // Populated externally by the service
            MigrationSteps = steps,
            GeneratedCode = generatedCode
        };
    }

    private static BreakingLevel AssessBreakingLevel(List<FieldChange> fieldChanges)
    {
        var maxLevel = BreakingLevel.None;

        foreach (var fc in fieldChanges)
        {
            if (fc.Breaking > maxLevel)
            {
                maxLevel = fc.Breaking;
            }

            if (maxLevel == BreakingLevel.Major)
            {
                break;
            }
        }

        return maxLevel;
    }

    private static SchemaChangeType ClassifyOverallChangeType(List<FieldChange> fieldChanges)
    {
        if (fieldChanges.Count == 0)
        {
            return SchemaChangeType.FieldAdded; // No changes (identity)
        }

        var types = new HashSet<FieldChangeType>();
        foreach (var fc in fieldChanges)
        {
            types.Add(fc.Type);
        }

        if (types.Count == 1)
        {
            return types.First() switch
            {
                FieldChangeType.Added => SchemaChangeType.FieldAdded,
                FieldChangeType.Removed => SchemaChangeType.FieldRemoved,
                FieldChangeType.TypeChanged => SchemaChangeType.FieldTypeChanged,
                FieldChangeType.Renamed => SchemaChangeType.FieldRenamed,
                _ => SchemaChangeType.Multiple
            };
        }

        return SchemaChangeType.Multiple;
    }

    private static void DetectRenames(List<FieldChange> fieldChanges)
    {
        var removed = fieldChanges.Where(fc => fc.Type == FieldChangeType.Removed).ToList();
        var added = fieldChanges.Where(fc => fc.Type == FieldChangeType.Added).ToList();

        foreach (var rem in removed)
        {
            // Find an added field with the same base type
            var match = added.FirstOrDefault(a =>
                string.Equals(GetBaseType(a.NewType ?? ""), GetBaseType(rem.OldType ?? ""), StringComparison.Ordinal));

            if (match is null)
            {
                continue;
            }

            // Replace the pair with a rename
            fieldChanges.Remove(rem);
            fieldChanges.Remove(match);
            added.Remove(match);

            fieldChanges.Add(new FieldChange
            {
                FieldName = match.FieldName,
                Type = FieldChangeType.Renamed,
                OldType = rem.OldType,
                NewType = match.NewType,
                OldFieldName = rem.FieldName,
                Breaking = BreakingLevel.Major
            });
        }
    }

    private static string GenerateSummary(SchemaChange change)
    {
        var sb = new StringBuilder();
        sb.Append($"Schema '{change.SubjectName}' evolved from v{change.OldVersion} to v{change.NewVersion}: ");

        var parts = new List<string>();
        var addedCount = change.FieldChanges.Count(fc => fc.Type == FieldChangeType.Added);
        var removedCount = change.FieldChanges.Count(fc => fc.Type == FieldChangeType.Removed);
        var typeChangedCount = change.FieldChanges.Count(fc => fc.Type == FieldChangeType.TypeChanged);
        var renamedCount = change.FieldChanges.Count(fc => fc.Type == FieldChangeType.Renamed);
        var nullableCount = change.FieldChanges.Count(fc => fc.Type == FieldChangeType.MadeNullable);
        var requiredCount = change.FieldChanges.Count(fc => fc.Type == FieldChangeType.MadeRequired);

        if (addedCount > 0) parts.Add($"{addedCount} field(s) added");
        if (removedCount > 0) parts.Add($"{removedCount} field(s) removed");
        if (typeChangedCount > 0) parts.Add($"{typeChangedCount} field(s) type changed");
        if (renamedCount > 0) parts.Add($"{renamedCount} field(s) renamed");
        if (nullableCount > 0) parts.Add($"{nullableCount} field(s) made nullable");
        if (requiredCount > 0) parts.Add($"{requiredCount} field(s) made required");

        sb.Append(parts.Count > 0 ? string.Join(", ", parts) : "no field changes");

        var breakingLabel = change.Breaking switch
        {
            BreakingLevel.None => " (non-breaking)",
            BreakingLevel.Minor => " (minor)",
            BreakingLevel.Major => " (BREAKING)",
            _ => ""
        };
        sb.Append(breakingLabel);

        return sb.ToString();
    }

    private static List<MigrationStep> GenerateMigrationSteps(SchemaChange change)
    {
        var steps = new List<MigrationStep>();
        var order = 1;

        foreach (var fc in change.FieldChanges)
        {
            switch (fc.Type)
            {
                case FieldChangeType.Added:
                    steps.Add(new MigrationStep
                    {
                        Order = order++,
                        Description = $"Add property '{fc.FieldName}' ({fc.NewType}) to your model class.",
                        Action = MigrationAction.UpdateModel,
                        CodeSnippet = fc.HasDefault
                            ? $"public {MapJsonTypeToCSharp(fc.NewType)}? {ToPascalCase(fc.FieldName)} {{ get; set; }}"
                            : $"public {MapJsonTypeToCSharp(fc.NewType)} {ToPascalCase(fc.FieldName)} {{ get; set; }} = default!;"
                    });
                    break;

                case FieldChangeType.Removed:
                    steps.Add(new MigrationStep
                    {
                        Order = order++,
                        Description = $"Remove or mark property '{fc.FieldName}' as obsolete — it no longer exists in the schema.",
                        Action = MigrationAction.UpdateModel,
                        CodeSnippet = $"[Obsolete(\"Removed in v{change.NewVersion}\")] public {MapJsonTypeToCSharp(fc.OldType)} {ToPascalCase(fc.FieldName)} {{ get; set; }}"
                    });
                    break;

                case FieldChangeType.TypeChanged:
                    steps.Add(new MigrationStep
                    {
                        Order = order++,
                        Description = $"Update property '{fc.FieldName}' type from {fc.OldType} to {fc.NewType}.",
                        Action = MigrationAction.UpdateDeserializer,
                        CodeSnippet = $"public {MapJsonTypeToCSharp(fc.NewType)} {ToPascalCase(fc.FieldName)} {{ get; set; }}"
                    });
                    break;

                case FieldChangeType.Renamed:
                    steps.Add(new MigrationStep
                    {
                        Order = order++,
                        Description = $"Rename property '{fc.OldFieldName}' to '{fc.FieldName}'.",
                        Action = MigrationAction.UpdateModel,
                        CodeSnippet = $"[JsonPropertyName(\"{fc.FieldName}\")] public {MapJsonTypeToCSharp(fc.NewType)} {ToPascalCase(fc.FieldName)} {{ get; set; }}"
                    });
                    break;

                case FieldChangeType.MadeNullable:
                    steps.Add(new MigrationStep
                    {
                        Order = order++,
                        Description = $"Property '{fc.FieldName}' is now nullable — add null checks where used.",
                        Action = MigrationAction.HandleNull,
                        CodeSnippet = $"public {MapJsonTypeToCSharp(fc.NewType)}? {ToPascalCase(fc.FieldName)} {{ get; set; }}"
                    });
                    break;

                case FieldChangeType.MadeRequired:
                    steps.Add(new MigrationStep
                    {
                        Order = order++,
                        Description = $"Property '{fc.FieldName}' is now required — ensure it is always provided.",
                        Action = MigrationAction.UpdateModel,
                        CodeSnippet = $"public required {MapJsonTypeToCSharp(fc.NewType)} {ToPascalCase(fc.FieldName)} {{ get; set; }}"
                    });
                    break;
            }
        }

        if (steps.Count == 0)
        {
            steps.Add(new MigrationStep
            {
                Order = 1,
                Description = "No consumer code changes required.",
                Action = MigrationAction.NoActionNeeded
            });
        }

        return steps;
    }

    /// <summary>
    /// Extract property names and their JSON types from a JSON Schema string.
    /// </summary>
    public static Dictionary<string, string> ExtractProperties(string schemaJson)
    {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);

        try
        {
            using var doc = JsonDocument.Parse(schemaJson, s_docOptions);
            var root = doc.RootElement;

            if (root.TryGetProperty("properties", out var props) && props.ValueKind == JsonValueKind.Object)
            {
                foreach (var prop in props.EnumerateObject())
                {
                    var type = ExtractTypeFromProperty(prop.Value);
                    result[prop.Name] = type;
                }
            }
        }
        catch (JsonException)
        {
            // Invalid JSON — return empty
        }

        return result;
    }

    /// <summary>
    /// Extract the set of required field names from a JSON Schema string.
    /// </summary>
    public static HashSet<string> ExtractRequired(string schemaJson)
    {
        var result = new HashSet<string>(StringComparer.Ordinal);

        try
        {
            using var doc = JsonDocument.Parse(schemaJson, s_docOptions);
            var root = doc.RootElement;

            if (root.TryGetProperty("required", out var required) && required.ValueKind == JsonValueKind.Array)
            {
                foreach (var item in required.EnumerateArray())
                {
                    if (item.ValueKind == JsonValueKind.String)
                    {
                        result.Add(item.GetString()!);
                    }
                }
            }
        }
        catch (JsonException)
        {
            // Invalid JSON — return empty
        }

        return result;
    }

    private static string ExtractTypeFromProperty(JsonElement propElement)
    {
        if (propElement.TryGetProperty("type", out var typeEl))
        {
            if (typeEl.ValueKind == JsonValueKind.String)
            {
                return typeEl.GetString() ?? "string";
            }

            if (typeEl.ValueKind == JsonValueKind.Array)
            {
                // e.g., ["string", "null"]
                var types = new List<string>();
                foreach (var t in typeEl.EnumerateArray())
                {
                    if (t.ValueKind == JsonValueKind.String)
                    {
                        types.Add(t.GetString()!);
                    }
                }
                return string.Join("|", types);
            }
        }

        return "string";
    }

    private static bool IsNullableType(string type)
    {
        return type.Contains("null", StringComparison.Ordinal);
    }

    private static string GetBaseType(string type) => JsonSchemaTypeHelper.GetBaseType(type);

    /// <summary>
    /// Map a JSON Schema type string to a C# type name.
    /// </summary>
    public static string MapJsonTypeToCSharp(string? jsonType)
    {
        if (jsonType is null)
        {
            return "object";
        }

        var baseType = GetBaseType(jsonType);

        return baseType switch
        {
            "string" => "string",
            "integer" => "int",
            "number" => "double",
            "boolean" => "bool",
            "array" => "List<object>",
            "object" => "object",
            _ => "object"
        };
    }

    /// <summary>
    /// Convert a string (camelCase, snake_case, kebab-case) to PascalCase.
    /// </summary>
    public static string ToPascalCase(string name)
    {
        if (string.IsNullOrEmpty(name))
        {
            return name;
        }

        // Handle camelCase and snake_case
        var parts = name.Split('_', '-');
        var sb = new StringBuilder();

        foreach (var part in parts)
        {
            if (part.Length > 0)
            {
                sb.Append(char.ToUpperInvariant(part[0]));
                if (part.Length > 1)
                {
                    sb.Append(part[1..]);
                }
            }
        }

        return sb.ToString();
    }

    /// <summary>
    /// Derive a C# class name from a schema subject name (e.g., "orders-value" to "OrdersValue").
    /// </summary>
    public static string DeriveClassName(string subjectName)
    {
        // Convert "orders-value" → "OrdersValue", "user-events" → "UserEvents"
        var parts = subjectName.Split('-', '_', '.');
        var sb = new StringBuilder();

        foreach (var part in parts)
        {
            if (part.Length > 0)
            {
                sb.Append(char.ToUpperInvariant(part[0]));
                if (part.Length > 1)
                {
                    sb.Append(part[1..]);
                }
            }
        }

        return sb.ToString();
    }
}
