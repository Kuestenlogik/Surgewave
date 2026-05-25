using System.Text.RegularExpressions;
using Kuestenlogik.Surgewave.Schema.Registry;

namespace Kuestenlogik.Surgewave.Schema.Registry.Handlers;

/// <summary>
/// Schema type handler for FlatBuffers IDL (.fbs) schemas.
/// Parses FlatBuffers IDL syntax and performs compatibility checking.
/// </summary>
public sealed partial class FlatBuffersSchemaHandler : ISchemaTypeHandler
{
    public string TypeName => "FLATBUFFERS";

    // FlatBuffers scalar types
    private static readonly HashSet<string> ScalarTypes =
    [
        "bool", "byte", "ubyte", "short", "ushort", "int", "uint",
        "float", "long", "ulong", "double", "int8", "uint8", "int16",
        "uint16", "int32", "uint32", "int64", "uint64", "float32", "float64",
        "string"
    ];

    public (bool IsValid, string? Error) Validate(string schemaString)
    {
        try
        {
            var parsed = ParseSchema(schemaString);
            if (parsed.Tables.Count == 0 && parsed.Structs.Count == 0 && parsed.Enums.Count == 0)
            {
                return (false, "FlatBuffers schema must contain at least one table, struct, or enum definition");
            }

            // Validate field types in tables
            foreach (var (tableName, table) in parsed.Tables)
            {
                foreach (var (fieldName, field) in table.Fields)
                {
                    if (!IsValidFieldType(field.Type, parsed))
                    {
                        return (false, $"Invalid field type '{field.Type}' for field '{tableName}.{fieldName}'");
                    }
                }
            }

            // Validate field types in structs (structs can only contain scalar types or other structs)
            foreach (var (structName, fbsStruct) in parsed.Structs)
            {
                foreach (var (fieldName, fieldType) in fbsStruct.Fields)
                {
                    if (!IsValidStructFieldType(fieldType, parsed))
                    {
                        return (false, $"Invalid struct field type '{fieldType}' for field '{structName}.{fieldName}' (structs can only contain scalars or other structs)");
                    }
                }
            }

            return (true, null);
        }
        catch (Exception ex)
        {
            return (false, $"Invalid FlatBuffers schema: {ex.Message}");
        }
    }

    private static bool IsValidFieldType(string fieldType, FbsSchema schema)
    {
        // Check scalar types
        if (ScalarTypes.Contains(fieldType))
            return true;

        // Check vector types [type]
        if (fieldType.StartsWith('[') && fieldType.EndsWith(']'))
        {
            var innerType = fieldType[1..^1];
            return IsValidFieldType(innerType, schema);
        }

        // Check if it's a defined table, struct, enum, or union
        if (schema.Tables.ContainsKey(fieldType) ||
            schema.Structs.ContainsKey(fieldType) ||
            schema.Enums.ContainsKey(fieldType) ||
            schema.Unions.ContainsKey(fieldType))
            return true;

        return false;
    }

    private static bool IsValidStructFieldType(string fieldType, FbsSchema schema)
    {
        // Structs can only contain scalar types (except string) or other structs
        if (ScalarTypes.Contains(fieldType) && fieldType != "string")
            return true;

        // Check if it's a defined struct
        if (schema.Structs.ContainsKey(fieldType))
            return true;

        return false;
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
            var newSchema = ParseSchema(newSchemaString);

            foreach (var existing in existingSchemas)
            {
                var existingSchema = ParseSchema(existing.SchemaString);

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
            errors.Add($"Error parsing FlatBuffers schema: {ex.Message}");
        }

        return new CompatibilityResult(errors.Count == 0, errors.Count > 0 ? errors : null);
    }

    private static List<string> CheckBackwardCompatibility(FbsSchema readerSchema, FbsSchema writerSchema)
    {
        var errors = new List<string>();

        // Check tables - reader should be able to read writer's data
        foreach (var (tableName, readerTable) in readerSchema.Tables)
        {
            if (!writerSchema.Tables.TryGetValue(tableName, out var writerTable))
            {
                // Table doesn't exist in writer - not necessarily an error for FlatBuffers
                // since fields are optional by default, but warn if it's the root type
                if (readerSchema.RootType == tableName)
                {
                    errors.Add($"Root type table '{tableName}' not found in writer schema");
                }
                continue;
            }

            // Check that required fields in reader exist in writer
            foreach (var (fieldName, readerField) in readerTable.Fields)
            {
                if (readerField.IsRequired)
                {
                    if (!writerTable.Fields.TryGetValue(fieldName, out var writerField))
                    {
                        errors.Add($"Required field '{tableName}.{fieldName}' not found in writer schema");
                    }
                    else if (!AreTypesCompatible(readerField.Type, writerField.Type))
                    {
                        errors.Add($"Field '{tableName}.{fieldName}' type mismatch: reader expects {readerField.Type}, writer has {writerField.Type}");
                    }
                }
            }

            // Check field ID conflicts (if using explicit IDs)
            var readerFieldIds = readerTable.Fields.Values.Where(f => f.Id.HasValue).ToDictionary(f => f.Id!.Value, f => f);
            var writerFieldIds = writerTable.Fields.Values.Where(f => f.Id.HasValue).ToDictionary(f => f.Id!.Value, f => f);

            foreach (var (id, readerField) in readerFieldIds)
            {
                if (writerFieldIds.TryGetValue(id, out var writerField))
                {
                    if (!AreTypesCompatible(readerField.Type, writerField.Type))
                    {
                        errors.Add($"Field ID {id} in table '{tableName}' has incompatible types: reader has {readerField.Type}, writer has {writerField.Type}");
                    }
                }
            }
        }

        // Check enums - values should not be removed or changed
        foreach (var (enumName, readerEnum) in readerSchema.Enums)
        {
            if (!writerSchema.Enums.TryGetValue(enumName, out var writerEnum))
            {
                continue; // Enum not in writer, will fail if used
            }

            foreach (var (valueName, readerValue) in readerEnum.Values)
            {
                if (writerEnum.Values.TryGetValue(valueName, out var writerValue))
                {
                    if (readerValue != writerValue)
                    {
                        errors.Add($"Enum '{enumName}.{valueName}' value changed from {writerValue} to {readerValue}");
                    }
                }
            }
        }

        return errors;
    }

    private static bool AreTypesCompatible(string readerType, string writerType)
    {
        // Exact match
        if (readerType == writerType) return true;

        // Handle vector types [type]
        if (readerType.StartsWith('[') && writerType.StartsWith('['))
        {
            var readerInner = readerType[1..^1];
            var writerInner = writerType[1..^1];
            return AreTypesCompatible(readerInner, writerInner);
        }

        // Numeric type widening (safe promotions)
        var wideningPairs = new HashSet<(string, string)>
        {
            ("byte", "short"), ("byte", "int"), ("byte", "long"),
            ("ubyte", "ushort"), ("ubyte", "uint"), ("ubyte", "ulong"),
            ("short", "int"), ("short", "long"),
            ("ushort", "uint"), ("ushort", "ulong"),
            ("int", "long"),
            ("uint", "ulong"),
            ("float", "double"),
            ("int8", "int16"), ("int8", "int32"), ("int8", "int64"),
            ("uint8", "uint16"), ("uint8", "uint32"), ("uint8", "uint64"),
            ("int16", "int32"), ("int16", "int64"),
            ("uint16", "uint32"), ("uint16", "uint64"),
            ("int32", "int64"),
            ("uint32", "uint64"),
            ("float32", "float64")
        };

        return wideningPairs.Contains((writerType, readerType));
    }

    private static FbsSchema ParseSchema(string schemaString)
    {
        var schema = new FbsSchema();
        var content = RemoveComments(schemaString);

        // Parse namespace
        var namespaceMatch = NamespaceRegex().Match(content);
        if (namespaceMatch.Success)
        {
            schema.Namespace = namespaceMatch.Groups[1].Value;
        }

        // Parse root_type
        var rootTypeMatch = RootTypeRegex().Match(content);
        if (rootTypeMatch.Success)
        {
            schema.RootType = rootTypeMatch.Groups[1].Value;
        }

        // Parse tables
        foreach (Match match in TableRegex().Matches(content))
        {
            var tableName = match.Groups[1].Value;
            var fieldsContent = match.Groups[2].Value;
            var table = new FbsTable { Name = tableName };

            foreach (Match fieldMatch in FieldRegex().Matches(fieldsContent))
            {
                var fieldName = fieldMatch.Groups[1].Value;
                var fieldType = fieldMatch.Groups[2].Value;
                var metadata = fieldMatch.Groups[3].Value;

                var field = new FbsField
                {
                    Name = fieldName,
                    Type = fieldType,
                    IsRequired = metadata.Contains("required"),
                    IsDeprecated = metadata.Contains("deprecated")
                };

                // Parse field ID if present
                var idMatch = FieldIdRegex().Match(metadata);
                if (idMatch.Success)
                {
                    field.Id = int.Parse(idMatch.Groups[1].Value);
                }

                // Parse default value if present
                var defaultMatch = DefaultValueRegex().Match(fieldMatch.Value);
                if (defaultMatch.Success)
                {
                    field.DefaultValue = defaultMatch.Groups[1].Value;
                }

                table.Fields[fieldName] = field;
            }

            schema.Tables[tableName] = table;
        }

        // Parse structs
        foreach (Match match in StructRegex().Matches(content))
        {
            var structName = match.Groups[1].Value;
            var fieldsContent = match.Groups[2].Value;
            var fbsStruct = new FbsStruct { Name = structName };

            foreach (Match fieldMatch in FieldRegex().Matches(fieldsContent))
            {
                var fieldName = fieldMatch.Groups[1].Value;
                var fieldType = fieldMatch.Groups[2].Value;
                fbsStruct.Fields[fieldName] = fieldType;
            }

            schema.Structs[structName] = fbsStruct;
        }

        // Parse enums
        foreach (Match match in EnumRegex().Matches(content))
        {
            var enumName = match.Groups[1].Value;
            var baseType = match.Groups[2].Value;
            var valuesContent = match.Groups[3].Value;
            var fbsEnum = new FbsEnum { Name = enumName, BaseType = baseType };

            int currentValue = 0;
            foreach (Match valueMatch in EnumValueRegex().Matches(valuesContent))
            {
                var valueName = valueMatch.Groups[1].Value;
                if (valueMatch.Groups[2].Success)
                {
                    currentValue = int.Parse(valueMatch.Groups[2].Value);
                }
                fbsEnum.Values[valueName] = currentValue;
                currentValue++;
            }

            schema.Enums[enumName] = fbsEnum;
        }

        // Parse unions
        foreach (Match match in UnionRegex().Matches(content))
        {
            var unionName = match.Groups[1].Value;
            var typesContent = match.Groups[2].Value;
            var types = typesContent.Split(',').Select(t => t.Trim()).Where(t => !string.IsNullOrEmpty(t)).ToList();
            schema.Unions[unionName] = types;
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

    // Regex patterns for parsing FlatBuffers IDL
    [GeneratedRegex(@"//[^\n]*")]
    private static partial Regex SingleLineCommentRegex();

    [GeneratedRegex(@"/\*[\s\S]*?\*/")]
    private static partial Regex MultiLineCommentRegex();

    [GeneratedRegex(@"namespace\s+([\w.]+)\s*;")]
    private static partial Regex NamespaceRegex();

    [GeneratedRegex(@"root_type\s+(\w+)\s*;")]
    private static partial Regex RootTypeRegex();

    [GeneratedRegex(@"table\s+(\w+)\s*\{([^}]*)\}")]
    private static partial Regex TableRegex();

    [GeneratedRegex(@"struct\s+(\w+)\s*\{([^}]*)\}")]
    private static partial Regex StructRegex();

    [GeneratedRegex(@"enum\s+(\w+)\s*:\s*(\w+)\s*\{([^}]*)\}")]
    private static partial Regex EnumRegex();

    [GeneratedRegex(@"union\s+(\w+)\s*\{([^}]*)\}")]
    private static partial Regex UnionRegex();

    [GeneratedRegex(@"(\w+)\s*:\s*([\w\[\].<>]+)(?:\s*=\s*[^;]+)?([^;]*);")]
    private static partial Regex FieldRegex();

    [GeneratedRegex(@"id:\s*(\d+)")]
    private static partial Regex FieldIdRegex();

    [GeneratedRegex(@"=\s*([^;(]+)")]
    private static partial Regex DefaultValueRegex();

    [GeneratedRegex(@"(\w+)(?:\s*=\s*(\d+))?")]
    private static partial Regex EnumValueRegex();

    private sealed class FbsSchema
    {
        public string? Namespace { get; set; }
        public string? RootType { get; set; }
        public Dictionary<string, FbsTable> Tables { get; } = new(StringComparer.Ordinal);
        public Dictionary<string, FbsStruct> Structs { get; } = new(StringComparer.Ordinal);
        public Dictionary<string, FbsEnum> Enums { get; } = new(StringComparer.Ordinal);
        public Dictionary<string, List<string>> Unions { get; } = new(StringComparer.Ordinal);
    }

    private sealed class FbsTable
    {
        public required string Name { get; init; }
        public Dictionary<string, FbsField> Fields { get; } = new(StringComparer.Ordinal);
    }

    private sealed class FbsStruct
    {
        public required string Name { get; init; }
        public Dictionary<string, string> Fields { get; } = new(StringComparer.Ordinal);
    }

    private sealed class FbsField
    {
        public required string Name { get; init; }
        public required string Type { get; init; }
        public int? Id { get; set; }
        public string? DefaultValue { get; set; }
        public bool IsRequired { get; set; }
        public bool IsDeprecated { get; set; }
    }

    private sealed class FbsEnum
    {
        public required string Name { get; init; }
        public required string BaseType { get; init; }
        public Dictionary<string, int> Values { get; } = new(StringComparer.Ordinal);
    }
}
