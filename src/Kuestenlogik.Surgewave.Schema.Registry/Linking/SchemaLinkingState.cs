using System.Text.Json;

namespace Kuestenlogik.Surgewave.Schema.Registry.Linking;

/// <summary>
/// Tracks the synchronization state for all schema links across clusters.
/// Persisted to disk for restart recovery.
/// </summary>
public sealed class SchemaLinkingState
{
    private static readonly JsonSerializerOptions s_jsonOptions = new() { WriteIndented = true };

    /// <summary>
    /// Schema links organized by cluster ID, then by subject name.
    /// Structure: ClusterId -> Subject -> SchemaLink
    /// </summary>
    public Dictionary<string, Dictionary<string, SchemaLink>> Links { get; set; } = [];

    /// <summary>
    /// Gets the link for a specific cluster and subject, or null if not tracked.
    /// </summary>
    public SchemaLink? GetLink(string clusterId, string subject)
    {
        if (Links.TryGetValue(clusterId, out var subjects) &&
            subjects.TryGetValue(subject, out var link))
        {
            return link;
        }
        return null;
    }

    /// <summary>
    /// Sets or updates the link for a specific cluster and subject.
    /// </summary>
    public void SetLink(string clusterId, string subject, SchemaLink link)
    {
        if (!Links.TryGetValue(clusterId, out var subjects))
        {
            subjects = [];
            Links[clusterId] = subjects;
        }
        subjects[subject] = link;
    }

    /// <summary>
    /// Gets all links across all clusters.
    /// </summary>
    public IReadOnlyList<SchemaLink> GetAllLinks()
    {
        var result = new List<SchemaLink>();
        foreach (var subjects in Links.Values)
        {
            result.AddRange(subjects.Values);
        }
        return result;
    }

    /// <summary>
    /// Gets all links for a specific subject across all clusters.
    /// </summary>
    public IReadOnlyList<SchemaLink> GetLinksForSubject(string subject)
    {
        var result = new List<SchemaLink>();
        foreach (var subjects in Links.Values)
        {
            if (subjects.TryGetValue(subject, out var link))
            {
                result.Add(link);
            }
        }
        return result;
    }

    /// <summary>
    /// Gets all links with a conflict status.
    /// </summary>
    public IReadOnlyList<SchemaLink> GetConflicts()
    {
        var result = new List<SchemaLink>();
        foreach (var subjects in Links.Values)
        {
            foreach (var link in subjects.Values)
            {
                if (link.Status == SchemaSyncStatus.Conflict)
                {
                    result.Add(link);
                }
            }
        }
        return result;
    }

    /// <summary>
    /// Persists the state to a JSON file.
    /// </summary>
    public void SaveToFile(string path)
    {
        var directory = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }

        var json = JsonSerializer.Serialize(this, s_jsonOptions);

        // Atomic write: write to temp file, then move
        var tempPath = path + ".tmp";
        File.WriteAllText(tempPath, json);
        File.Move(tempPath, path, overwrite: true);
    }

    /// <summary>
    /// Loads state from a JSON file, or returns a new empty state if the file does not exist.
    /// </summary>
    public static SchemaLinkingState LoadFromFile(string path)
    {
        if (!File.Exists(path))
        {
            return new SchemaLinkingState();
        }

        var json = File.ReadAllText(path);
        return JsonSerializer.Deserialize<SchemaLinkingState>(json, s_jsonOptions)
            ?? new SchemaLinkingState();
    }
}
