using System.Text;
using System.Text.Json;
using Kuestenlogik.Surgewave.Schema.Registry.Evolution;

namespace Kuestenlogik.Surgewave.Schema.Registry.Migration;

/// <summary>
/// Transforms JSON messages between schema versions for zero-downtime schema migration.
/// Handles field additions, removals, type changes, renames, and nested objects recursively.
/// </summary>
public sealed class SchemaMigrator
{
    private static readonly JsonDocumentOptions s_docOptions = new()
    {
        AllowTrailingCommas = true,
        CommentHandling = JsonCommentHandling.Skip
    };

    private static readonly JsonWriterOptions s_writerOptions = new()
    {
        Indented = false
    };

    private readonly SchemaEvolutionAnalyzer _analyzer = new();

    /// <summary>
    /// Migrate a JSON message from one schema to another.
    /// </summary>
    /// <param name="message">The raw JSON message bytes.</param>
    /// <param name="fromSchemaJson">The source JSON Schema.</param>
    /// <param name="toSchemaJson">The target JSON Schema.</param>
    /// <param name="config">Migration configuration.</param>
    /// <returns>The migrated message bytes.</returns>
    public byte[] Migrate(byte[] message, string fromSchemaJson, string toSchemaJson, SchemaMigrationConfig config)
    {
        var fromProps = SchemaEvolutionAnalyzer.ExtractProperties(fromSchemaJson);
        var toProps = SchemaEvolutionAnalyzer.ExtractProperties(toSchemaJson);
        var toRequired = SchemaEvolutionAnalyzer.ExtractRequired(toSchemaJson);

        using var sourceDoc = JsonDocument.Parse(message, s_docOptions);
        var sourceRoot = sourceDoc.RootElement;

        if (sourceRoot.ValueKind != JsonValueKind.Object)
        {
            // Not an object — return as-is
            return message;
        }

        using var ms = new MemoryStream();
        using var writer = new Utf8JsonWriter(ms, s_writerOptions);

        MigrateObject(writer, sourceRoot, fromProps, toProps, toRequired, config);

        writer.Flush();
        return ms.ToArray();
    }

    /// <summary>
    /// Check if migration is needed between two schema versions.
    /// </summary>
    /// <param name="messageSchemaVersion">The schema version of the message.</param>
    /// <param name="consumerSchemaVersion">The schema version the consumer expects.</param>
    /// <returns>True if the versions differ and migration is needed.</returns>
    public bool NeedsMigration(string messageSchemaVersion, string consumerSchemaVersion)
    {
        return !string.Equals(messageSchemaVersion, consumerSchemaVersion, StringComparison.Ordinal);
    }

    /// <summary>
    /// Get the migration path between two schema versions for a subject.
    /// Computes the transforms needed for each step in the path.
    /// </summary>
    /// <param name="subject">The schema subject.</param>
    /// <param name="fromVersion">The starting version.</param>
    /// <param name="toVersion">The target version.</param>
    /// <param name="schemasByVersion">A dictionary of version number to JSON Schema string.</param>
    /// <returns>The migration path with all steps and transforms.</returns>
    public MigrationPath GetMigrationPath(
        string subject,
        int fromVersion,
        int toVersion,
        IReadOnlyDictionary<int, string> schemasByVersion)
    {
        var steps = new List<SchemaMigrationStep>();

        if (fromVersion == toVersion)
        {
            return new MigrationPath
            {
                Subject = subject,
                FromVersion = fromVersion,
                ToVersion = toVersion,
                Steps = steps
            };
        }

        var direction = toVersion > fromVersion ? 1 : -1;
        var current = fromVersion;

        while (current != toVersion)
        {
            var next = current + direction;

            if (!schemasByVersion.TryGetValue(current, out var currentSchema) ||
                !schemasByVersion.TryGetValue(next, out var nextSchema))
            {
                // Missing intermediate version — try direct jump
                if (schemasByVersion.TryGetValue(fromVersion, out var directFrom) &&
                    schemasByVersion.TryGetValue(toVersion, out var directTo))
                {
                    var directTransforms = ComputeTransforms(directFrom, directTo);
                    steps.Clear();
                    steps.Add(new SchemaMigrationStep
                    {
                        FromVersion = fromVersion,
                        ToVersion = toVersion,
                        Transforms = directTransforms
                    });
                    break;
                }

                break;
            }

            var transforms = ComputeTransforms(currentSchema, nextSchema);
            steps.Add(new SchemaMigrationStep
            {
                FromVersion = current,
                ToVersion = next,
                Transforms = transforms
            });

            current = next;
        }

        return new MigrationPath
        {
            Subject = subject,
            FromVersion = fromVersion,
            ToVersion = toVersion,
            Steps = steps
        };
    }

    /// <summary>
    /// Build a compiled migrator function for the given schemas and config.
    /// The returned function can be cached and reused for high performance.
    /// </summary>
    /// <param name="fromSchemaJson">Source JSON Schema.</param>
    /// <param name="toSchemaJson">Target JSON Schema.</param>
    /// <param name="config">Migration configuration.</param>
    /// <returns>A function that migrates byte[] messages.</returns>
    public Func<byte[], byte[]> BuildMigrator(string fromSchemaJson, string toSchemaJson, SchemaMigrationConfig config)
    {
        // Pre-compute the property maps so they are not re-parsed per message
        var fromProps = SchemaEvolutionAnalyzer.ExtractProperties(fromSchemaJson);
        var toProps = SchemaEvolutionAnalyzer.ExtractProperties(toSchemaJson);
        var toRequired = SchemaEvolutionAnalyzer.ExtractRequired(toSchemaJson);

        return message =>
        {
            using var sourceDoc = JsonDocument.Parse(message, s_docOptions);
            var sourceRoot = sourceDoc.RootElement;

            if (sourceRoot.ValueKind != JsonValueKind.Object)
            {
                return message;
            }

            using var ms = new MemoryStream();
            using var writer = new Utf8JsonWriter(ms, s_writerOptions);

            MigrateObject(writer, sourceRoot, fromProps, toProps, toRequired, config);

            writer.Flush();
            return ms.ToArray();
        };
    }

    private List<FieldTransform> ComputeTransforms(string fromSchemaJson, string toSchemaJson)
    {
        var change = _analyzer.AnalyzeChanges(fromSchemaJson, toSchemaJson, "migration", 0, 1);
        var transforms = new List<FieldTransform>();

        foreach (var fc in change.FieldChanges)
        {
            var transform = fc.Type switch
            {
                FieldChangeType.Added => new FieldTransform
                {
                    FieldPath = fc.FieldName,
                    Type = FieldTransformType.AddWithDefault,
                    TargetType = fc.NewType,
                    DefaultValue = fc.DefaultValue ?? GetDefaultJsonValue(fc.NewType)
                },
                FieldChangeType.Removed => new FieldTransform
                {
                    FieldPath = fc.FieldName,
                    Type = FieldTransformType.Remove,
                    SourceType = fc.OldType
                },
                FieldChangeType.TypeChanged => new FieldTransform
                {
                    FieldPath = fc.FieldName,
                    Type = FieldTransformType.ChangeType,
                    SourceType = fc.OldType,
                    TargetType = fc.NewType
                },
                FieldChangeType.Renamed => new FieldTransform
                {
                    FieldPath = fc.FieldName,
                    Type = FieldTransformType.Rename,
                    OldFieldName = fc.OldFieldName,
                    SourceType = fc.OldType,
                    TargetType = fc.NewType
                },
                FieldChangeType.MadeNullable => new FieldTransform
                {
                    FieldPath = fc.FieldName,
                    Type = FieldTransformType.MakeNullable,
                    SourceType = fc.OldType,
                    TargetType = fc.NewType
                },
                FieldChangeType.MadeRequired => new FieldTransform
                {
                    FieldPath = fc.FieldName,
                    Type = FieldTransformType.MakeRequired,
                    SourceType = fc.OldType,
                    TargetType = fc.NewType
                },
                _ => null
            };

            if (transform is not null)
            {
                transforms.Add(transform);
            }
        }

        return transforms;
    }

    private static void MigrateObject(
        Utf8JsonWriter writer,
        JsonElement source,
        Dictionary<string, string> fromProps,
        Dictionary<string, string> toProps,
        HashSet<string> toRequired,
        SchemaMigrationConfig config)
    {
        writer.WriteStartObject();

        // Track which target fields we have written
        var writtenFields = new HashSet<string>(StringComparer.Ordinal);

        // Write fields from the source that exist in the target
        foreach (var prop in source.EnumerateObject())
        {
            if (toProps.TryGetValue(prop.Name, out var targetType))
            {
                // Field exists in both schemas
                var sourceType = fromProps.TryGetValue(prop.Name, out var st) ? st : "string";

                if (!string.Equals(GetBaseType(sourceType), GetBaseType(targetType), StringComparison.Ordinal))
                {
                    // Type mismatch — apply strategy
                    WriteCoercedField(writer, prop.Name, prop.Value, sourceType, targetType, config);
                }
                else
                {
                    // Same type — copy directly
                    writer.WritePropertyName(prop.Name);
                    prop.Value.WriteTo(writer);
                }

                writtenFields.Add(prop.Name);
            }
            else
            {
                // Field exists in source but not in target
                switch (config.ExtraFieldStrategy)
                {
                    case ExtraFieldStrategy.Include:
                        writer.WritePropertyName(prop.Name);
                        prop.Value.WriteTo(writer);
                        break;

                    case ExtraFieldStrategy.Fail:
                        throw new SchemaMigrationException(
                            $"Extra field '{prop.Name}' found in source but not in target schema");

                    case ExtraFieldStrategy.Ignore:
                    default:
                        // Skip it
                        break;
                }
            }
        }

        // Add fields that exist in the target but not in the source
        foreach (var (fieldName, targetType) in toProps)
        {
            if (writtenFields.Contains(fieldName))
            {
                continue;
            }

            var isRequired = toRequired.Contains(fieldName);

            switch (config.MissingFieldStrategy)
            {
                case MissingFieldStrategy.UseDefault:
                    writer.WritePropertyName(fieldName);
                    WriteDefaultValue(writer, targetType);
                    break;

                case MissingFieldStrategy.UseNull:
                    writer.WritePropertyName(fieldName);
                    writer.WriteNullValue();
                    break;

                case MissingFieldStrategy.Fail:
                    if (isRequired)
                    {
                        throw new SchemaMigrationException(
                            $"Required field '{fieldName}' is missing from source message");
                    }
                    // Non-required missing fields get null
                    writer.WritePropertyName(fieldName);
                    writer.WriteNullValue();
                    break;
            }
        }

        writer.WriteEndObject();
    }

    private static void WriteCoercedField(
        Utf8JsonWriter writer,
        string fieldName,
        JsonElement value,
        string fromType,
        string toType,
        SchemaMigrationConfig config)
    {
        switch (config.TypeMismatchStrategy)
        {
            case TypeMismatchStrategy.Coerce:
                if (TypeCoercer.CanCoerce(fromType, toType))
                {
                    var coerced = TypeCoercer.Coerce(value, fromType, toType);
                    writer.WritePropertyName(fieldName);
                    WriteCoercedValue(writer, coerced, toType);
                }
                else
                {
                    // Cannot coerce — use default
                    writer.WritePropertyName(fieldName);
                    WriteDefaultValue(writer, toType);
                }
                break;

            case TypeMismatchStrategy.UseDefault:
                writer.WritePropertyName(fieldName);
                WriteDefaultValue(writer, toType);
                break;

            case TypeMismatchStrategy.Fail:
                throw new SchemaMigrationException(
                    $"Type mismatch for field '{fieldName}': source is '{fromType}', target is '{toType}'");
        }
    }

    private static void WriteCoercedValue(Utf8JsonWriter writer, object? value, string targetType)
    {
        if (value is null)
        {
            writer.WriteNullValue();
            return;
        }

        if (value is JsonElement element)
        {
            element.WriteTo(writer);
            return;
        }

        switch (value)
        {
            case string s:
                writer.WriteStringValue(s);
                break;
            case long l:
                writer.WriteNumberValue(l);
                break;
            case int i:
                writer.WriteNumberValue(i);
                break;
            case double d:
                writer.WriteNumberValue(d);
                break;
            case bool b:
                writer.WriteBooleanValue(b);
                break;
            default:
                writer.WriteStringValue(value.ToString());
                break;
        }
    }

    private static void WriteDefaultValue(Utf8JsonWriter writer, string jsonType)
    {
        var baseType = GetBaseType(jsonType);
        var isNullable = jsonType.Contains("null", StringComparison.Ordinal);

        if (isNullable)
        {
            writer.WriteNullValue();
            return;
        }

        switch (baseType)
        {
            case "string":
                writer.WriteStringValue("");
                break;
            case "integer":
                writer.WriteNumberValue(0);
                break;
            case "number":
                writer.WriteNumberValue(0.0);
                break;
            case "boolean":
                writer.WriteBooleanValue(false);
                break;
            case "array":
                writer.WriteStartArray();
                writer.WriteEndArray();
                break;
            case "object":
                writer.WriteStartObject();
                writer.WriteEndObject();
                break;
            default:
                writer.WriteNullValue();
                break;
        }
    }

    private static string GetBaseType(string type) => JsonSchemaTypeHelper.GetBaseType(type);

    private static string? GetDefaultJsonValue(string? jsonType)
    {
        if (jsonType is null)
        {
            return null;
        }

        var baseType = GetBaseType(jsonType);
        return baseType switch
        {
            "string" => "\"\"",
            "integer" => "0",
            "number" => "0.0",
            "boolean" => "false",
            "array" => "[]",
            "object" => "{}",
            _ => "null"
        };
    }
}
