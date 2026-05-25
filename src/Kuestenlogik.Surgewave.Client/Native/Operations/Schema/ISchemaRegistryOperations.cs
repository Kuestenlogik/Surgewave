namespace Kuestenlogik.Surgewave.Client.Native.Operations.Schema;

/// <summary>
/// Interface for schema registry operations.
/// </summary>
public interface ISchemaRegistryOperations
{
    /// <summary>
    /// Register a new schema.
    /// </summary>
    Task<SchemaRegistrationResult> RegisterSchemaAsync(string subject, string schemaString, string schemaType, CancellationToken cancellationToken = default);

    /// <summary>
    /// Get schema by global ID.
    /// </summary>
    Task<SchemaInfo?> GetSchemaByIdAsync(int schemaId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Get schema by subject and version.
    /// </summary>
    Task<SchemaInfo?> GetSchemaByVersionAsync(string subject, int version, CancellationToken cancellationToken = default);

    /// <summary>
    /// Get versions for a subject.
    /// </summary>
    Task<IReadOnlyList<int>> GetSubjectVersionsAsync(string subject, CancellationToken cancellationToken = default);
}
