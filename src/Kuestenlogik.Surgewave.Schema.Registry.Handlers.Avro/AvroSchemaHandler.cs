using Chr.Avro.Abstract;
using Chr.Avro.Representation;
using Kuestenlogik.Surgewave.Schema.Registry;

namespace Kuestenlogik.Surgewave.Schema.Registry.Handlers;

/// <summary>
/// Schema type handler for Apache Avro schemas.
/// Uses Chr.Avro for parsing and compatibility checking.
/// </summary>
public sealed class AvroSchemaHandler : ISchemaTypeHandler
{
    private readonly JsonSchemaReader _avroReader = new();

    public string TypeName => "AVRO";

    public (bool IsValid, string? Error) Validate(string schemaString)
    {
        try
        {
            _avroReader.Read(schemaString);
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

        try
        {
            var newSchema = _avroReader.Read(newSchemaString);
            var errors = new List<string>();

            foreach (var existing in existingSchemas)
            {
                var existingSchema = _avroReader.Read(existing.SchemaString);

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

            return new CompatibilityResult(errors.Count == 0, errors.Count > 0 ? errors : null);
        }
        catch (Exception ex)
        {
            return new CompatibilityResult(false, [$"Error parsing Avro schema: {ex.Message}"]);
        }
    }

    private static List<string> CheckBackwardCompatibility(Chr.Avro.Abstract.Schema readerSchema, Chr.Avro.Abstract.Schema writerSchema)
    {
        var errors = new List<string>();
        CanRead(readerSchema, writerSchema, errors);
        return errors;
    }

    private static bool CanRead(Chr.Avro.Abstract.Schema readerSchema, Chr.Avro.Abstract.Schema writerSchema, List<string> errors)
    {
        if (readerSchema.GetType() == writerSchema.GetType())
        {
            switch (readerSchema)
            {
                case RecordSchema readerRecord when writerSchema is RecordSchema writerRecord:
                    return CanReadRecord(readerRecord, writerRecord, errors);

                case EnumSchema readerEnum when writerSchema is EnumSchema writerEnum:
                    return CanReadEnum(readerEnum, writerEnum, errors);

                case ArraySchema readerArray when writerSchema is ArraySchema writerArray:
                    return CanRead(readerArray.Item, writerArray.Item, errors);

                case MapSchema readerMap when writerSchema is MapSchema writerMap:
                    return CanRead(readerMap.Value, writerMap.Value, errors);

                case UnionSchema readerUnion when writerSchema is UnionSchema writerUnion:
                    return CanReadUnion(readerUnion, writerUnion, errors);

                case FixedSchema readerFixed when writerSchema is FixedSchema writerFixed:
                    if (readerFixed.Size != writerFixed.Size)
                    {
                        errors.Add($"Fixed size mismatch: reader expects {readerFixed.Size}, writer has {writerFixed.Size}");
                        return false;
                    }
                    return true;

                default:
                    return true;
            }
        }

        if (IsPromotable(writerSchema, readerSchema))
        {
            return true;
        }

        if (writerSchema is UnionSchema writerUnion2)
        {
            foreach (var writerType in writerUnion2.Schemas)
            {
                if (!CanRead(readerSchema, writerType, []))
                {
                    errors.Add($"Reader cannot read union type {writerType}");
                    return false;
                }
            }
            return true;
        }

        if (readerSchema is UnionSchema readerUnion2)
        {
            foreach (var readerType in readerUnion2.Schemas)
            {
                if (CanRead(readerType, writerSchema, []))
                {
                    return true;
                }
            }
            errors.Add($"No matching type in reader union for writer type {writerSchema}");
            return false;
        }

        errors.Add($"Incompatible types: reader expects {readerSchema.GetType().Name}, writer has {writerSchema.GetType().Name}");
        return false;
    }

    private static bool CanReadRecord(RecordSchema readerSchema, RecordSchema writerSchema, List<string> errors)
    {
        foreach (var readerField in readerSchema.Fields)
        {
            var writerField = writerSchema.Fields.FirstOrDefault(f => f.Name == readerField.Name);

            if (writerField == null)
            {
                var hasDefault = HasDefaultValue(readerField);
                if (!hasDefault)
                {
                    errors.Add($"Reader field '{readerField.Name}' has no default value and is missing from writer");
                    return false;
                }
            }
            else
            {
                if (!CanRead(readerField.Type, writerField.Type, errors))
                {
                    errors.Add($"Field '{readerField.Name}' types are incompatible");
                    return false;
                }
            }
        }

        return true;
    }

    private static bool HasDefaultValue(RecordField field)
    {
        if (field.Default != null)
        {
            return true;
        }

        if (field.Type is UnionSchema union)
        {
            if (union.Schemas.Count > 0 && union.Schemas.First() is NullSchema)
            {
                return true;
            }
        }

        return false;
    }

    private static bool CanReadEnum(EnumSchema readerSchema, EnumSchema writerSchema, List<string> errors)
    {
        foreach (var symbol in writerSchema.Symbols)
        {
            if (!readerSchema.Symbols.Contains(symbol))
            {
                if (string.IsNullOrEmpty(readerSchema.Default))
                {
                    errors.Add($"Writer enum symbol '{symbol}' not in reader and no default defined");
                    return false;
                }
            }
        }

        return true;
    }

    private static bool CanReadUnion(UnionSchema readerSchema, UnionSchema writerSchema, List<string> errors)
    {
        foreach (var writerType in writerSchema.Schemas)
        {
            var matched = false;
            foreach (var readerType in readerSchema.Schemas)
            {
                if (CanRead(readerType, writerType, []))
                {
                    matched = true;
                    break;
                }
            }

            if (!matched)
            {
                errors.Add($"Writer union type {writerType} has no matching reader type");
                return false;
            }
        }

        return true;
    }

    private static bool IsPromotable(Chr.Avro.Abstract.Schema from, Chr.Avro.Abstract.Schema to)
    {
        return (from, to) switch
        {
            (IntSchema, LongSchema) => true,
            (IntSchema, FloatSchema) => true,
            (IntSchema, DoubleSchema) => true,
            (LongSchema, FloatSchema) => true,
            (LongSchema, DoubleSchema) => true,
            (FloatSchema, DoubleSchema) => true,
            (StringSchema, BytesSchema) => true,
            (BytesSchema, StringSchema) => true,
            _ => false
        };
    }
}
