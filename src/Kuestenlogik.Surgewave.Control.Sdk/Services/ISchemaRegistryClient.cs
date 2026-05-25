using Kuestenlogik.Surgewave.Control.Models;

namespace Kuestenlogik.Surgewave.Control.Services;

/// <summary>
/// Client for Schema Registry REST API.
/// </summary>
public interface ISchemaRegistryClient
{
    /// <summary>
    /// Get all subjects.
    /// </summary>
    Task<IReadOnlyList<string>> GetSubjectsAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Get all versions for a subject.
    /// </summary>
    Task<IReadOnlyList<int>> GetVersionsAsync(string subject, CancellationToken cancellationToken = default);

    /// <summary>
    /// Get schema by subject and version.
    /// </summary>
    Task<SchemaModel?> GetSchemaAsync(string subject, int version, CancellationToken cancellationToken = default);

    /// <summary>
    /// Get latest schema for a subject.
    /// </summary>
    Task<SchemaModel?> GetLatestSchemaAsync(string subject, CancellationToken cancellationToken = default);

    /// <summary>
    /// Register a new schema.
    /// </summary>
    Task<int?> RegisterSchemaAsync(string subject, RegisterSchemaRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Delete a subject.
    /// </summary>
    Task<IReadOnlyList<int>?> DeleteSubjectAsync(string subject, bool permanent = false, CancellationToken cancellationToken = default);

    /// <summary>
    /// Delete a specific version.
    /// </summary>
    Task<int?> DeleteVersionAsync(string subject, int version, bool permanent = false, CancellationToken cancellationToken = default);

    /// <summary>
    /// Check compatibility of a schema against a subject.
    /// </summary>
    Task<CompatibilityCheckResult?> CheckCompatibilityAsync(string subject, RegisterSchemaRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Get global compatibility level.
    /// </summary>
    Task<string?> GetGlobalCompatibilityAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Set global compatibility level.
    /// </summary>
    Task<bool> SetGlobalCompatibilityAsync(string compatibility, CancellationToken cancellationToken = default);

    /// <summary>
    /// Get subject-level compatibility.
    /// </summary>
    Task<string?> GetSubjectCompatibilityAsync(string subject, CancellationToken cancellationToken = default);

    /// <summary>
    /// Set subject-level compatibility.
    /// </summary>
    Task<bool> SetSubjectCompatibilityAsync(string subject, string compatibility, CancellationToken cancellationToken = default);

    /// <summary>
    /// Get supported schema types.
    /// </summary>
    Task<IReadOnlyList<string>> GetSchemaTypesAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Get schema by its global ID.
    /// </summary>
    Task<SchemaByIdModel?> GetSchemaByIdAsync(int id, CancellationToken cancellationToken = default);

    /// <summary>
    /// Get subject-version pairs for a schema ID.
    /// </summary>
    Task<IReadOnlyList<SubjectVersionModel>> GetSchemaVersionsByIdAsync(int id, CancellationToken cancellationToken = default);

    /// <summary>
    /// Infer JSON Schema from topic messages.
    /// </summary>
    Task<InferredSchemaModel?> InferSchemaAsync(string topic, int? sampleSize = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Infer schema and register it in the registry.
    /// </summary>
    Task<int?> InferAndRegisterSchemaAsync(string topic, CancellationToken cancellationToken = default);

    /// <summary>
    /// List all auto-inferred schemas.
    /// </summary>
    Task<IReadOnlyList<InferredSchemaSummaryModel>> GetInferredSchemasAsync(CancellationToken cancellationToken = default);
}
