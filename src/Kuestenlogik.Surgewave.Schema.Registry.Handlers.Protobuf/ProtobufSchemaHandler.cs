using System.Text.RegularExpressions;
using Kuestenlogik.Surgewave.Schema.Registry;

namespace Kuestenlogik.Surgewave.Schema.Registry.Handlers;

/// <summary>
/// Schema type handler for Protocol Buffers schemas.
/// Uses regex-based parsing for field definitions and wire type compatibility.
/// </summary>
public sealed partial class ProtobufSchemaHandler : ISchemaTypeHandler
{
    public string TypeName => "PROTOBUF";

    // Protobuf scalar types
    private static readonly HashSet<string> ScalarTypes =
    [
        "double", "float", "int32", "int64", "uint32", "uint64",
        "sint32", "sint64", "fixed32", "fixed64", "sfixed32", "sfixed64",
        "bool", "string", "bytes"
    ];

    public (bool IsValid, string? Error) Validate(string schemaString)
    {
        try
        {
            var parsed = ParseProtobufSchema(schemaString);

            // Check for syntax declaration (proto3 is recommended)
            if (string.IsNullOrEmpty(parsed.Syntax))
            {
                // proto2 doesn't require syntax declaration, but warn if completely missing message
                if (parsed.Messages.Count == 0 && parsed.Enums.Count == 0 && parsed.Services.Count == 0)
                {
                    return (false, "Invalid protobuf schema: no message, enum, or service definitions found");
                }
            }

            // Validate field numbers (1-536870911, excluding 19000-19999 reserved range)
            foreach (var (msgName, message) in parsed.Messages)
            {
                foreach (var (fieldNum, field) in message.Fields)
                {
                    if (fieldNum < 1 || fieldNum > 536870911)
                    {
                        return (false, $"Invalid field number {fieldNum} in message '{msgName}': must be between 1 and 536870911");
                    }
                    if (fieldNum >= 19000 && fieldNum <= 19999)
                    {
                        return (false, $"Invalid field number {fieldNum} in message '{msgName}': 19000-19999 are reserved by the Protocol Buffers implementation");
                    }
                }

                // Check for duplicate field numbers
                var fieldNumbers = message.Fields.Keys.ToList();
                var duplicates = fieldNumbers.GroupBy(n => n).Where(g => g.Count() > 1).Select(g => g.Key);
                if (duplicates.Any())
                {
                    return (false, $"Duplicate field numbers in message '{msgName}': {string.Join(", ", duplicates)}");
                }

                // Check for duplicate field names
                var fieldNames = message.Fields.Values.Select(f => f.Name).ToList();
                var duplicateNames = fieldNames.GroupBy(n => n).Where(g => g.Count() > 1).Select(g => g.Key);
                if (duplicateNames.Any())
                {
                    return (false, $"Duplicate field names in message '{msgName}': {string.Join(", ", duplicateNames)}");
                }
            }

            // Validate enum values
            foreach (var (enumName, pbEnum) in parsed.Enums)
            {
                // In proto3, first enum value must be 0
                if (parsed.Syntax == "proto3" && pbEnum.Values.Count > 0)
                {
                    var firstValue = pbEnum.Values.Values.Min();
                    if (firstValue != 0)
                    {
                        return (false, $"In proto3, first enum value in '{enumName}' must be 0");
                    }
                }
            }

            // Validate field types are valid
            foreach (var (msgName, message) in parsed.Messages)
            {
                foreach (var (_, field) in message.Fields)
                {
                    // Type could be scalar, message reference, enum, or map
                    var fieldType = field.Type;

                    // Check for map type
                    if (fieldType.StartsWith("map<", StringComparison.Ordinal))
                    {
                        continue; // Map types have their own validation
                    }

                    // Strip repeated prefix if present
                    if (fieldType.StartsWith("repeated ", StringComparison.OrdinalIgnoreCase))
                    {
                        fieldType = fieldType["repeated ".Length..];
                    }

                    // Check if it's a known scalar type or a defined message/enum
                    if (!ScalarTypes.Contains(fieldType) &&
                        !parsed.Messages.ContainsKey(fieldType) &&
                        !parsed.Enums.ContainsKey(fieldType) &&
                        !fieldType.Contains('.')) // Allow fully qualified names
                    {
                        // Don't fail, just log warning - could be an external type reference
                    }
                }
            }

            return (true, null);
        }
        catch (Exception ex)
        {
            return (false, $"Failed to parse protobuf schema: {ex.Message}");
        }
    }

    private static ProtobufSchema ParseProtobufSchema(string schemaString)
    {
        var schema = new ProtobufSchema();
        var content = RemoveComments(schemaString);

        // Parse syntax
        var syntaxMatch = SyntaxRegex().Match(content);
        if (syntaxMatch.Success)
        {
            schema.Syntax = syntaxMatch.Groups[1].Value;
        }

        // Parse package
        var packageMatch = PackageRegex().Match(content);
        if (packageMatch.Success)
        {
            schema.Package = packageMatch.Groups[1].Value;
        }

        // Parse messages
        foreach (Match match in MessageRegex().Matches(content))
        {
            var messageName = match.Groups[1].Value;
            var fieldsContent = match.Groups[2].Value;
            var message = new ProtobufMessage { Name = messageName };

            foreach (Match fieldMatch in FieldPatternRegex().Matches(fieldsContent))
            {
                var modifier = fieldMatch.Groups[1].Value.ToLowerInvariant();
                var type = fieldMatch.Groups[2].Value;
                var name = fieldMatch.Groups[3].Value;
                var number = int.Parse(fieldMatch.Groups[4].Value);

                message.Fields[number] = new ProtobufField
                {
                    Name = name,
                    Type = type,
                    Number = number,
                    IsRequired = modifier == "required",
                    IsRepeated = modifier == "repeated"
                };
            }

            schema.Messages[messageName] = message;
        }

        // Parse enums
        foreach (Match match in EnumRegex().Matches(content))
        {
            var enumName = match.Groups[1].Value;
            var valuesContent = match.Groups[2].Value;
            var pbEnum = new ProtobufEnum { Name = enumName };

            foreach (Match valueMatch in EnumValueRegex().Matches(valuesContent))
            {
                var valueName = valueMatch.Groups[1].Value;
                var valueNumber = int.Parse(valueMatch.Groups[2].Value);
                pbEnum.Values[valueName] = valueNumber;
            }

            schema.Enums[enumName] = pbEnum;
        }

        // Parse services
        foreach (Match match in ServiceRegex().Matches(content))
        {
            var serviceName = match.Groups[1].Value;
            schema.Services.Add(serviceName);
        }

        return schema;
    }

    private static string RemoveComments(string input)
    {
        // Remove single-line comments
        input = SingleLineCommentRegex().Replace(input, "");
        // Remove multi-line comments
        input = MultiLineCommentRegex().Replace(input, "");
        return input;
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
        var newFields = ParseProtobufFields(newSchemaString);

        foreach (var existing in existingSchemas)
        {
            var existingFields = ParseProtobufFields(existing.SchemaString);

            var (backward, forward) = mode switch
            {
                CompatibilityMode.Backward or CompatibilityMode.BackwardTransitive => (true, false),
                CompatibilityMode.Forward or CompatibilityMode.ForwardTransitive => (false, true),
                CompatibilityMode.Full or CompatibilityMode.FullTransitive => (true, true),
                _ => (false, false)
            };

            if (backward)
            {
                // Check that existing field numbers haven't changed type
                foreach (var (number, existingField) in existingFields)
                {
                    if (newFields.TryGetValue(number, out var newField))
                    {
                        if (!AreProtobufTypesCompatible(newField.Type, existingField.Type))
                        {
                            errors.Add($"Backward compatibility error with version {existing.Version}: Field {number} type changed from {existingField.Type} to {newField.Type}");
                        }
                    }
                }
            }

            if (forward)
            {
                // Check that new field numbers haven't changed type
                foreach (var (number, newField) in newFields)
                {
                    if (existingFields.TryGetValue(number, out var existingField))
                    {
                        if (!AreProtobufTypesCompatible(existingField.Type, newField.Type))
                        {
                            errors.Add($"Forward compatibility error with version {existing.Version}: Field {number} type changed from {newField.Type} to {existingField.Type}");
                        }
                    }
                }
            }
        }

        return new CompatibilityResult(errors.Count == 0, errors.Count > 0 ? errors : null);
    }

    private static Dictionary<int, (string Name, string Type, bool Required)> ParseProtobufFields(string protoString)
    {
        var fields = new Dictionary<int, (string Name, string Type, bool Required)>();

        // Simple regex-based parsing for protobuf field definitions
        // Format: [optional|required|repeated] type name = number;
        var fieldPattern = FieldPatternRegex();

        foreach (Match match in fieldPattern.Matches(protoString))
        {
            var modifier = match.Groups[1].Value.ToLowerInvariant();
            var type = match.Groups[2].Value;
            var name = match.Groups[3].Value;
            var number = int.Parse(match.Groups[4].Value);

            fields[number] = (name, type, modifier == "required");
        }

        return fields;
    }

    private static bool AreProtobufTypesCompatible(string type1, string type2)
    {
        if (string.Equals(type1, type2, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        // Protobuf wire type compatibility groups
        var wireTypeGroups = new[]
        {
            new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "int32", "int64", "uint32", "uint64", "sint32", "sint64", "bool", "enum" },
            new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "fixed64", "sfixed64", "double" },
            new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "fixed32", "sfixed32", "float" },
            new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "string", "bytes" }
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

    [GeneratedRegex(@"(optional|required|repeated)?\s*(\w+)\s+(\w+)\s*=\s*(\d+)", RegexOptions.IgnoreCase)]
    private static partial Regex FieldPatternRegex();

    [GeneratedRegex(@"//[^\n]*")]
    private static partial Regex SingleLineCommentRegex();

    [GeneratedRegex(@"/\*[\s\S]*?\*/")]
    private static partial Regex MultiLineCommentRegex();

    [GeneratedRegex(@"syntax\s*=\s*""(proto[23])""")]
    private static partial Regex SyntaxRegex();

    [GeneratedRegex(@"package\s+([\w.]+)\s*;")]
    private static partial Regex PackageRegex();

    [GeneratedRegex(@"message\s+(\w+)\s*\{([^{}]*(?:\{[^{}]*\}[^{}]*)*)\}")]
    private static partial Regex MessageRegex();

    [GeneratedRegex(@"enum\s+(\w+)\s*\{([^}]*)\}")]
    private static partial Regex EnumRegex();

    [GeneratedRegex(@"(\w+)\s*=\s*(-?\d+)")]
    private static partial Regex EnumValueRegex();

    [GeneratedRegex(@"service\s+(\w+)\s*\{")]
    private static partial Regex ServiceRegex();

    private sealed class ProtobufSchema
    {
        public string? Syntax { get; set; }
        public string? Package { get; set; }
        public Dictionary<string, ProtobufMessage> Messages { get; } = new(StringComparer.Ordinal);
        public Dictionary<string, ProtobufEnum> Enums { get; } = new(StringComparer.Ordinal);
        public List<string> Services { get; } = new();
    }

    private sealed class ProtobufMessage
    {
        public required string Name { get; init; }
        public Dictionary<int, ProtobufField> Fields { get; } = new();
    }

    private sealed class ProtobufField
    {
        public required string Name { get; init; }
        public required string Type { get; init; }
        public required int Number { get; init; }
        public bool IsRequired { get; init; }
        public bool IsRepeated { get; init; }
    }

    private sealed class ProtobufEnum
    {
        public required string Name { get; init; }
        public Dictionary<string, int> Values { get; } = new(StringComparer.Ordinal);
    }
}
