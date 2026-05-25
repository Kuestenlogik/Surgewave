using Kuestenlogik.Surgewave.Client.Native.Commands;
using Kuestenlogik.Surgewave.Client.Native.Commands.Schema;
using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Client.Native.Operations.Schema;

/// <summary>
/// Schema registry operations for Surgewave native client.
/// </summary>
public sealed class SurgewaveSchemaRegistryOperations : ISchemaRegistryOperations
{
    private readonly SurgewaveNativeClient _client;
    private readonly CommandExecutor _executor;

    internal SurgewaveSchemaRegistryOperations(SurgewaveNativeClient client)
    {
        _client = client;
        _executor = new CommandExecutor(client);
    }

    /// <summary>
    /// List all schema subjects.
    /// </summary>
    public Task<IReadOnlyList<string>> ListAsync(CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new ListSubjectsCommand(), cancellationToken);

    /// <summary>
    /// Delete a subject.
    /// </summary>
    public Task<IReadOnlyList<int>> DeleteSubjectAsync(string subject, bool permanent, CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new DeleteSubjectCommand(subject, permanent), cancellationToken);

    /// <summary>
    /// Register a new schema.
    /// </summary>
    public Task<SchemaRegistrationResult> RegisterSchemaAsync(string subject, string schemaString, string schemaType, CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new RegisterSchemaCommand(subject, schemaString, schemaType), cancellationToken);

    /// <summary>
    /// Get schema by global ID.
    /// </summary>
    public Task<SchemaInfo?> GetSchemaByIdAsync(int schemaId, CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new GetSchemaByIdCommand(schemaId), cancellationToken);

    /// <summary>
    /// Get schema by subject and version.
    /// </summary>
    public Task<SchemaInfo?> GetSchemaByVersionAsync(string subject, int version, CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new GetSchemaByVersionCommand(subject, version), cancellationToken);

    /// <summary>
    /// Get versions for a subject.
    /// </summary>
    public Task<IReadOnlyList<int>> GetSubjectVersionsAsync(string subject, CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new GetSubjectVersionsCommand(subject), cancellationToken);

    /// <summary>
    /// Check schema compatibility.
    /// </summary>
    public Task<CompatibilityCheckResult> CheckCompatibilityAsync(string subject, string schemaString, string schemaType, int? version, CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new CheckCompatibilityCommand(subject, schemaString, schemaType, version), cancellationToken);

    /// <summary>
    /// Get compatibility configuration.
    /// </summary>
    public Task<string> GetCompatibilityConfigAsync(string? subject, CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new GetCompatibilityConfigCommand(subject), cancellationToken);

    /// <summary>
    /// Set compatibility configuration.
    /// </summary>
    public Task<SurgewaveErrorCode> SetCompatibilityConfigAsync(string compatibility, string? subject, CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new SetCompatibilityConfigCommand(compatibility, subject), cancellationToken);

    /// <summary>
    /// Get supported schema types.
    /// </summary>
    public Task<IReadOnlyList<string>> GetSchemaTypesAsync(CancellationToken cancellationToken = default)
        => _executor.ExecuteAsync(new GetSchemaTypesCommand(), cancellationToken);

    /// <summary>
    /// Start building a schema registration with fluent API.
    /// </summary>
    public SchemaBuilder RegisterSchema(string subject) => new(_client, subject);
}
