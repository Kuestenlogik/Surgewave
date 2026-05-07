using System.Diagnostics.CodeAnalysis;

namespace Kuestenlogik.Surgewave.Control.Services;

public interface IConnectorRegistryService
{
    Task<IReadOnlyList<ConnectorTypeInfo>> GetConnectorTypesAsync(CancellationToken cancellationToken = default);
    Task<ConnectorConfigSchema?> GetConfigSchemaAsync(string connectorType, CancellationToken cancellationToken = default);
}

public record ConnectorTypeInfo
{
    public required string Type { get; init; }
    public required string Name { get; init; }
    public required string Category { get; init; }
    public bool IsSource { get; init; }
    public bool IsSink { get; init; }
    public string? Description { get; init; }
    public string? Version { get; init; }
    public string? Author { get; init; }
    [SuppressMessage("Design", "CA1056:URI-like properties should not be strings")]
    public string? DocumentationUrl { get; init; }
    [SuppressMessage("Design", "CA1056:URI-like properties should not be strings")]
    public string? LicenseUrl { get; init; }
    public string[]? Tags { get; init; }
    public string? Icon { get; init; }

    /// <summary>
    /// Worker IDs that can run this connector type. Empty means local-only.
    /// </summary>
    public string[]? AvailableOnWorkers { get; init; }

    /// <summary>
    /// Whether this connector type is available locally (in-process).
    /// </summary>
    public bool IsLocal { get; init; } = true;

    /// <summary>
    /// Worker availability summary: "Local", "Remote", or "Local + Remote".
    /// </summary>
    public string AvailabilityLabel => (IsLocal, AvailableOnWorkers?.Length > 0) switch
    {
        (true, true) => "Local + Remote",
        (true, false) => "Local",
        (false, true) => "Remote",
        _ => "Local"
    };
}

public record ConnectorConfigSchema
{
    public required string Type { get; init; }
    public required List<ConfigKeyInfo> Keys { get; init; }
}

public record ConfigKeyInfo
{
    public required string Name { get; init; }
    public required string Type { get; init; }
    public string? DefaultValue { get; init; }
    public string? Documentation { get; init; }
    public required string Importance { get; init; }
    public bool IsRequired { get; init; }
    public string? Editor { get; init; }
    public string? EditorLanguage { get; init; }
    public string[]? Options { get; init; }
}
