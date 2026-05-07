using Kuestenlogik.Surgewave.Client.Native.Operations.Schema;

namespace Kuestenlogik.Surgewave.Client.Tests.Serialization;

/// <summary>
/// Mock implementation of ISchemaRegistryOperations for testing.
/// </summary>
public sealed class MockSchemaRegistry : ISchemaRegistryOperations
{
    private readonly Dictionary<string, (int Id, string Schema)> _subjectSchemas = new();
    private readonly Dictionary<int, SchemaInfo> _schemasById = new();
    private int _nextSchemaId = 1;

    public Task<SchemaRegistrationResult> RegisterSchemaAsync(
        string subject,
        string schemaString,
        string schemaType,
        CancellationToken cancellationToken = default)
    {
        if (_subjectSchemas.TryGetValue(subject, out var existing))
        {
            return Task.FromResult(new SchemaRegistrationResult(existing.Id, 1));
        }

        var schemaId = _nextSchemaId++;
        _subjectSchemas[subject] = (schemaId, schemaString);
        _schemasById[schemaId] = new SchemaInfo
        {
            Id = schemaId,
            Subject = subject,
            Version = 1,
            SchemaType = schemaType,
            SchemaString = schemaString
        };

        return Task.FromResult(new SchemaRegistrationResult(schemaId, 1));
    }

    public Task<SchemaInfo?> GetSchemaByIdAsync(int schemaId, CancellationToken cancellationToken = default)
    {
        _schemasById.TryGetValue(schemaId, out var schema);
        return Task.FromResult(schema);
    }

    public Task<SchemaInfo?> GetSchemaByVersionAsync(
        string subject,
        int version,
        CancellationToken cancellationToken = default)
    {
        if (_subjectSchemas.TryGetValue(subject, out var existing))
        {
            return Task.FromResult<SchemaInfo?>(new SchemaInfo
            {
                Id = existing.Id,
                Subject = subject,
                Version = version,
                SchemaType = "AVRO",
                SchemaString = existing.Schema
            });
        }
        return Task.FromResult<SchemaInfo?>(null);
    }

    public Task<IReadOnlyList<int>> GetSubjectVersionsAsync(
        string subject,
        CancellationToken cancellationToken = default)
    {
        if (_subjectSchemas.ContainsKey(subject))
        {
            return Task.FromResult<IReadOnlyList<int>>([1]);
        }
        return Task.FromResult<IReadOnlyList<int>>([]);
    }

    /// <summary>
    /// Pre-register a schema for testing deserialization.
    /// </summary>
    public void PreRegisterSchema(int schemaId, string subject, string schemaString, string schemaType = "AVRO")
    {
        _schemasById[schemaId] = new SchemaInfo
        {
            Id = schemaId,
            Subject = subject,
            Version = 1,
            SchemaType = schemaType,
            SchemaString = schemaString
        };
        _subjectSchemas[subject] = (schemaId, schemaString);
        if (schemaId >= _nextSchemaId)
        {
            _nextSchemaId = schemaId + 1;
        }
    }
}
