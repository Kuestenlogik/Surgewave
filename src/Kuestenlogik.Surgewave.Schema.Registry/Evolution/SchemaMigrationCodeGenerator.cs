using System.Text;

namespace Kuestenlogik.Surgewave.Schema.Registry.Evolution;

/// <summary>
/// Generates C# model classes and migration code from JSON Schemas and schema changes.
/// Produces compilable code snippets that consumers can adopt.
/// </summary>
public sealed class SchemaMigrationCodeGenerator
{
    /// <summary>
    /// Generate a C# model class from a JSON Schema string.
    /// </summary>
    /// <param name="schemaJson">The JSON Schema definition.</param>
    /// <param name="className">The desired class name.</param>
    /// <param name="namespaceName">The desired namespace.</param>
    /// <returns>A compilable C# class definition.</returns>
    public string GenerateModelClass(string schemaJson, string className, string namespaceName = "Surgewave.Models")
    {
        var properties = SchemaEvolutionAnalyzer.ExtractProperties(schemaJson);
        var required = SchemaEvolutionAnalyzer.ExtractRequired(schemaJson);

        var sb = new StringBuilder();
        sb.AppendLine("using System.Text.Json.Serialization;");
        sb.AppendLine();
        sb.AppendLine($"namespace {namespaceName};");
        sb.AppendLine();
        sb.AppendLine($"public class {className}");
        sb.AppendLine("{");

        foreach (var (name, type) in properties)
        {
            var csharpType = SchemaEvolutionAnalyzer.MapJsonTypeToCSharp(type);
            var propName = SchemaEvolutionAnalyzer.ToPascalCase(name);
            var isRequired = required.Contains(name);
            var isNullable = type.Contains("null", StringComparison.Ordinal);

            // Add JsonPropertyName if the JSON name differs from the C# name
            if (!string.Equals(name, propName, StringComparison.Ordinal))
            {
                sb.AppendLine($"    [JsonPropertyName(\"{name}\")]");
            }

            if (isRequired && !isNullable)
            {
                var defaultVal = GetDefaultForType(csharpType);
                sb.AppendLine($"    public {csharpType} {propName} {{ get; set; }} = {defaultVal};");
            }
            else
            {
                sb.AppendLine($"    public {csharpType}? {propName} {{ get; set; }}");
            }
        }

        sb.AppendLine("}");

        return sb.ToString();
    }

    /// <summary>
    /// Generate migration code for a schema change: produces the new model class and consumer update comments.
    /// </summary>
    /// <param name="change">The detected schema change.</param>
    /// <param name="className">The base class name (without version suffix).</param>
    /// <returns>A C# code string with the migration.</returns>
    public string GenerateMigrationCode(SchemaChange change, string className)
    {
        var sb = new StringBuilder();

        sb.AppendLine($"// Migration: {className} v{change.OldVersion} -> v{change.NewVersion}");
        sb.AppendLine($"// Breaking level: {change.Breaking}");
        sb.AppendLine($"// Changes:");

        foreach (var fc in change.FieldChanges)
        {
            var desc = fc.Type switch
            {
                FieldChangeType.Added => $"  Added '{fc.FieldName}' ({fc.NewType}{(fc.HasDefault ? ", optional" : "")})",
                FieldChangeType.Removed => $"  Removed '{fc.FieldName}' ({fc.OldType})",
                FieldChangeType.TypeChanged => $"  Changed '{fc.FieldName}' type: {fc.OldType} -> {fc.NewType}",
                FieldChangeType.Renamed => $"  Renamed '{fc.OldFieldName}' -> '{fc.FieldName}'",
                FieldChangeType.MadeNullable => $"  Made '{fc.FieldName}' nullable",
                FieldChangeType.MadeRequired => $"  Made '{fc.FieldName}' required",
                _ => $"  Changed '{fc.FieldName}'"
            };
            sb.AppendLine($"// {desc}");
        }

        sb.AppendLine();

        // Generate the updated model class
        var newClassName = $"{className}V{change.NewVersion}";
        sb.AppendLine($"public class {newClassName}");
        sb.AppendLine("{");

        // Build a property set for the new version
        foreach (var fc in change.FieldChanges)
        {
            var propName = SchemaEvolutionAnalyzer.ToPascalCase(fc.FieldName);

            switch (fc.Type)
            {
                case FieldChangeType.Added:
                    if (fc.HasDefault)
                    {
                        sb.AppendLine($"    public {SchemaEvolutionAnalyzer.MapJsonTypeToCSharp(fc.NewType)}? {propName} {{ get; set; }} // NEW in v{change.NewVersion} (optional)");
                    }
                    else
                    {
                        sb.AppendLine($"    public {SchemaEvolutionAnalyzer.MapJsonTypeToCSharp(fc.NewType)} {propName} {{ get; set; }} = {GetDefaultForType(SchemaEvolutionAnalyzer.MapJsonTypeToCSharp(fc.NewType))}; // NEW in v{change.NewVersion}");
                    }
                    break;

                case FieldChangeType.Removed:
                    sb.AppendLine($"    // [Obsolete(\"Removed in v{change.NewVersion}\")] public {SchemaEvolutionAnalyzer.MapJsonTypeToCSharp(fc.OldType)} {propName} {{ get; set; }}");
                    break;

                case FieldChangeType.TypeChanged:
                    sb.AppendLine($"    public {SchemaEvolutionAnalyzer.MapJsonTypeToCSharp(fc.NewType)} {propName} {{ get; set; }} = {GetDefaultForType(SchemaEvolutionAnalyzer.MapJsonTypeToCSharp(fc.NewType))}; // Changed from {fc.OldType}");
                    break;

                case FieldChangeType.Renamed:
                    sb.AppendLine($"    [JsonPropertyName(\"{fc.FieldName}\")] public {SchemaEvolutionAnalyzer.MapJsonTypeToCSharp(fc.NewType)} {propName} {{ get; set; }} = {GetDefaultForType(SchemaEvolutionAnalyzer.MapJsonTypeToCSharp(fc.NewType))}; // Renamed from '{fc.OldFieldName}'");
                    break;

                case FieldChangeType.MadeNullable:
                    sb.AppendLine($"    public {SchemaEvolutionAnalyzer.MapJsonTypeToCSharp(fc.NewType)}? {propName} {{ get; set; }} // Now nullable");
                    break;

                case FieldChangeType.MadeRequired:
                    sb.AppendLine($"    public required {SchemaEvolutionAnalyzer.MapJsonTypeToCSharp(fc.NewType)} {propName} {{ get; set; }}");
                    break;
            }
        }

        sb.AppendLine("}");
        sb.AppendLine();

        // Generate consumer migration comment block
        sb.AppendLine("// Consumer migration:");
        var oldClassName = $"{className}V{change.OldVersion}";
        sb.AppendLine($"// Before: var obj = JsonSerializer.Deserialize<{oldClassName}>(msg.Value);");
        sb.AppendLine($"// After:  var obj = JsonSerializer.Deserialize<{newClassName}>(msg.Value);");

        foreach (var fc in change.FieldChanges)
        {
            var propName = SchemaEvolutionAnalyzer.ToPascalCase(fc.FieldName);

            switch (fc.Type)
            {
                case FieldChangeType.Added when fc.HasDefault:
                    sb.AppendLine($"//         // {propName} will be null for v{change.OldVersion} messages (backward compatible)");
                    break;
                case FieldChangeType.Removed:
                    sb.AppendLine($"//         // {propName} has been removed — remove references from consumer logic");
                    break;
                case FieldChangeType.TypeChanged:
                    sb.AppendLine($"//         // {propName} type changed from {fc.OldType} to {fc.NewType} — update conversion logic");
                    break;
                case FieldChangeType.MadeNullable:
                    sb.AppendLine($"//         // {propName} is now nullable — add null checks: if (obj.{propName} is not null) {{ ... }}");
                    break;
            }
        }

        return sb.ToString();
    }

    /// <summary>
    /// Generate consumer update code showing before/after patterns.
    /// </summary>
    public string GenerateConsumerUpdateCode(SchemaChange change, string className)
    {
        var sb = new StringBuilder();
        var newClassName = $"{className}V{change.NewVersion}";

        sb.AppendLine("// === Consumer Update Guide ===");
        sb.AppendLine();
        sb.AppendLine("// 1. Update your model class (see generated model above)");
        sb.AppendLine();
        sb.AppendLine("// 2. Update deserialization:");
        sb.AppendLine($"var message = JsonSerializer.Deserialize<{newClassName}>(record.Value);");
        sb.AppendLine();

        // Generate specific handling code for each change
        foreach (var fc in change.FieldChanges)
        {
            var propName = SchemaEvolutionAnalyzer.ToPascalCase(fc.FieldName);

            switch (fc.Type)
            {
                case FieldChangeType.Added when fc.HasDefault:
                    sb.AppendLine($"// 3. Handle optional new field '{fc.FieldName}':");
                    sb.AppendLine($"if (message.{propName} is not null)");
                    sb.AppendLine("{");
                    sb.AppendLine($"    // Process {propName}");
                    sb.AppendLine("}");
                    sb.AppendLine();
                    break;

                case FieldChangeType.TypeChanged:
                    sb.AppendLine($"// 3. Handle type change for '{fc.FieldName}' ({fc.OldType} -> {fc.NewType}):");
                    if (fc.OldType == "integer" && fc.NewType == "string")
                    {
                        sb.AppendLine($"// Old: int value = message.{propName};");
                        sb.AppendLine($"// New: string value = message.{propName};");
                        sb.AppendLine($"// If you need the old int: int.TryParse(message.{propName}, out var intValue);");
                    }
                    else if (fc.OldType == "string" && fc.NewType == "integer")
                    {
                        sb.AppendLine($"// Old: string value = message.{propName};");
                        sb.AppendLine($"// New: int value = message.{propName};");
                    }
                    sb.AppendLine();
                    break;

                case FieldChangeType.MadeNullable:
                    sb.AppendLine($"// 3. Add null check for '{fc.FieldName}' (now nullable):");
                    sb.AppendLine($"if (message.{propName} is null)");
                    sb.AppendLine("{");
                    sb.AppendLine($"    // Handle missing {propName}");
                    sb.AppendLine("}");
                    sb.AppendLine();
                    break;

                case FieldChangeType.Removed:
                    sb.AppendLine($"// 3. Remove usage of '{fc.FieldName}' — no longer in schema.");
                    sb.AppendLine();
                    break;
            }
        }

        return sb.ToString();
    }

    private static string GetDefaultForType(string csharpType)
    {
        return csharpType switch
        {
            "string" => "\"\"",
            "int" => "0",
            "double" => "0.0",
            "bool" => "false",
            "List<object>" => "[]",
            _ => "default!"
        };
    }
}
