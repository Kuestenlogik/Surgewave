namespace Kuestenlogik.Surgewave.Connect.Pipelines;

using System.Collections.Concurrent;

/// <summary>
/// In-memory store for pipeline version history.
/// Keeps a maximum of 50 versions per pipeline.
/// </summary>
public sealed class PipelineVersionStore
{
    private const int MaxVersionsPerPipeline = 50;

    private readonly ConcurrentDictionary<string, List<PipelineVersionEntry>> _versions = new();
    private readonly object _lock = new();

    /// <summary>
    /// Save a new version for a pipeline. Auto-increments the version number.
    /// </summary>
    public PipelineVersionEntry SaveVersion(string pipelineId, PipelineDefinition definition, string? changeDescription = null)
    {
        lock (_lock)
        {
            var versions = _versions.GetOrAdd(pipelineId, _ => []);

            var nextVersion = versions.Count > 0 ? versions[^1].Version + 1 : 1;

            var entry = new PipelineVersionEntry
            {
                Version = nextVersion,
                Definition = definition,
                CreatedAt = DateTimeOffset.UtcNow,
                ChangeDescription = changeDescription
            };

            versions.Add(entry);

            // Trim to max versions
            if (versions.Count > MaxVersionsPerPipeline)
            {
                versions.RemoveRange(0, versions.Count - MaxVersionsPerPipeline);
            }

            return entry;
        }
    }

    /// <summary>
    /// Get all version metadata for a pipeline (without full definitions for efficiency).
    /// </summary>
    public List<PipelineVersionEntry> GetVersions(string pipelineId)
    {
        lock (_lock)
        {
            if (!_versions.TryGetValue(pipelineId, out var versions))
                return [];

            return [.. versions];
        }
    }

    /// <summary>
    /// Get a specific version.
    /// </summary>
    public PipelineVersionEntry? GetVersion(string pipelineId, int version)
    {
        lock (_lock)
        {
            if (!_versions.TryGetValue(pipelineId, out var versions))
                return null;

            return versions.Find(v => v.Version == version);
        }
    }

    /// <summary>
    /// Compute diff between two versions.
    /// </summary>
    public PipelineVersionDiff? GetDiff(string pipelineId, int fromVersion, int toVersion)
    {
        lock (_lock)
        {
            if (!_versions.TryGetValue(pipelineId, out var versions))
                return null;

            var from = versions.Find(v => v.Version == fromVersion);
            var to = versions.Find(v => v.Version == toVersion);

            if (from is null || to is null)
                return null;

            return PipelineVersionDiff.Compute(fromVersion, toVersion, from.Definition, to.Definition);
        }
    }
}
