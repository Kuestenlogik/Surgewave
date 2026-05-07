using System.ComponentModel.DataAnnotations;
using Kuestenlogik.Surgewave.Core.Configuration;

namespace Kuestenlogik.Surgewave.Schema.Registry.Linking;

/// <summary>
/// Configuration for cross-cluster schema linking.
/// Bound from appsettings.json under "Surgewave:SchemaLinking".
/// </summary>
public sealed class SchemaLinkingConfig : IValidatableConfig
{
    /// <summary>
    /// Enable cross-cluster schema linking.
    /// </summary>
    public bool Enabled { get; set; } = false;

    /// <summary>
    /// Remote schema registries to synchronize with.
    /// </summary>
    public List<LinkedSchemaRegistry> RemoteRegistries { get; set; } = [];

    /// <summary>
    /// Synchronization mode: Export (push local to remote), Import (pull remote to local),
    /// or Bidirectional (both).
    /// </summary>
    public SchemaSyncMode SyncMode { get; set; } = SchemaSyncMode.Bidirectional;

    /// <summary>
    /// Interval in seconds between synchronization cycles.
    /// </summary>
    [Range(1, int.MaxValue)]
    public int SyncIntervalSeconds { get; set; } = 30;

    /// <summary>
    /// Glob patterns for subjects to synchronize.
    /// Use <c>"*"</c> to sync all subjects.
    /// </summary>
    public List<string> SubjectPatterns { get; set; } = ["*"];

    /// <summary>
    /// Whether to synchronize compatibility configuration for subjects.
    /// </summary>
    public bool SyncCompatibilityConfig { get; set; } = true;

    /// <summary>
    /// Strategy for resolving conflicting schema versions.
    /// </summary>
    public ConflictResolution ConflictResolution { get; set; } = ConflictResolution.HighestVersion;

    /// <inheritdoc />
    public IReadOnlyList<string> Validate()
    {
        var errors = new List<string>(ConfigValidator.ValidateDataAnnotations(this));

        if (Enabled && RemoteRegistries.Count == 0)
            errors.Add($"{nameof(RemoteRegistries)}: must contain at least one entry when linking is enabled.");

        if (SubjectPatterns.Any(string.IsNullOrWhiteSpace))
            errors.Add($"{nameof(SubjectPatterns)}: must not contain empty entries.");

        for (int i = 0; i < RemoteRegistries.Count; i++)
        {
            errors.AddRange(RemoteRegistries[i].Validate().Select(e => $"{nameof(RemoteRegistries)}[{i}].{e}"));
        }

        return errors;
    }
}

/// <summary>
/// A remote schema registry endpoint to link with.
/// </summary>
public sealed class LinkedSchemaRegistry : IValidatableConfig
{
    /// <summary>
    /// Unique identifier for the remote cluster.
    /// </summary>
    [Required]
    [MinLength(1)]
    public string ClusterId { get; init; } = "";

    /// <summary>
    /// Base URL of the remote schema registry REST API.
    /// </summary>
    [Required]
    [Url]
    public string SchemaRegistryUrl { get; init; } = "";

    /// <summary>
    /// Optional human-readable display name for the remote registry.
    /// </summary>
    public string? DisplayName { get; init; }

    /// <inheritdoc />
    public IReadOnlyList<string> Validate() => ConfigValidator.ValidateDataAnnotations(this);
}

/// <summary>
/// Direction of schema synchronization.
/// </summary>
public enum SchemaSyncMode
{
    /// <summary>
    /// Push local schemas to remote registries only.
    /// </summary>
    Export,

    /// <summary>
    /// Pull remote schemas to local registry only.
    /// </summary>
    Import,

    /// <summary>
    /// Both push and pull schemas between registries.
    /// </summary>
    Bidirectional
}

/// <summary>
/// Strategy for resolving version conflicts between linked registries.
/// </summary>
public enum ConflictResolution
{
    /// <summary>
    /// The registry with the highest version number wins.
    /// </summary>
    HighestVersion,

    /// <summary>
    /// Local registry always wins on conflict.
    /// </summary>
    LocalWins,

    /// <summary>
    /// Remote registry always wins on conflict.
    /// </summary>
    RemoteWins
}
