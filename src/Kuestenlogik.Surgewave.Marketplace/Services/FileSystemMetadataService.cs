using System.Collections.Concurrent;
using System.Text.Json;
using System.Text.RegularExpressions;
using Kuestenlogik.Surgewave.Marketplace.Models;

namespace Kuestenlogik.Surgewave.Marketplace.Services;

/// <summary>
/// Stores package metadata as JSON files on the file system.
/// Loads an in-memory index at startup for fast search.
/// Layout: {dataDir}/metadata/{id}/metadata.json
/// </summary>
public sealed class FileSystemMetadataService : IPackageMetadataService
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        WriteIndented = true
    };

    // Plugin-IDs sind kebab-case ASCII (lowercase, optional dotted
    // Namespaces). Path-Trennzeichen sind ausgeschlossen.
    private static readonly Regex IdPattern =
        new(@"^[a-z0-9](?:[a-z0-9.\-]{0,127})$", RegexOptions.Compiled);

    private readonly string _rootDir;
    private readonly ConcurrentDictionary<string, PackageMetadata> _index = new(StringComparer.OrdinalIgnoreCase);

    public FileSystemMetadataService(string dataDirectory)
    {
        _rootDir = Path.GetFullPath(Path.Combine(dataDirectory, "metadata"));
        Directory.CreateDirectory(_rootDir);
    }

    public async Task InitializeAsync(CancellationToken ct = default)
    {
        if (!Directory.Exists(_rootDir)) return;

        foreach (var dir in Directory.GetDirectories(_rootDir))
        {
            var metaPath = Path.Combine(dir, "metadata.json");
            if (!File.Exists(metaPath)) continue;

            try
            {
                var json = await File.ReadAllTextAsync(metaPath, ct);
                var meta = JsonSerializer.Deserialize<PackageMetadata>(json, JsonOptions);
                if (meta != null)
                    _index[meta.Id] = meta;
            }
            catch { /* skip corrupt entries */ }
        }
    }

    public Task<PackageMetadata?> GetAsync(string id, string? version = null, CancellationToken ct = default)
    {
        if (!_index.TryGetValue(id, out var meta))
            return Task.FromResult<PackageMetadata?>(null);

        if (version != null && !string.Equals(meta.Version, version, StringComparison.OrdinalIgnoreCase))
            return Task.FromResult<PackageMetadata?>(null);

        return Task.FromResult<PackageMetadata?>(meta);
    }

    public Task<IReadOnlyList<PackageMetadata>> SearchAsync(string? query, int skip, int take, CancellationToken ct = default)
    {
        var results = _index.Values
            .Where(m => m.Listed)
            .Where(m =>
            {
                if (string.IsNullOrWhiteSpace(query)) return true;
                var q = query.Trim();
                return (m.Id.Contains(q, StringComparison.OrdinalIgnoreCase))
                    || (m.Name.Contains(q, StringComparison.OrdinalIgnoreCase))
                    || (m.Description?.Contains(q, StringComparison.OrdinalIgnoreCase) == true)
                    || (m.Tags?.Any(t => t.Contains(q, StringComparison.OrdinalIgnoreCase)) == true);
            })
            .OrderByDescending(m => m.DownloadCount)
            .ThenBy(m => m.Name)
            .Skip(skip)
            .Take(take)
            .ToList();

        return Task.FromResult<IReadOnlyList<PackageMetadata>>(results);
    }

    public async Task SaveAsync(PackageMetadata metadata, CancellationToken ct = default)
    {
        var dir = GetMetadataDir(metadata.Id);
        Directory.CreateDirectory(dir);

        var metaPath = Path.Combine(dir, "metadata.json");
        var json = JsonSerializer.Serialize(metadata, JsonOptions);
        await File.WriteAllTextAsync(metaPath, json, ct);

        _index[metadata.Id] = metadata;
    }

    public Task DeleteAsync(string id, string version, CancellationToken ct = default)
    {
        var dir = GetMetadataDir(id);
        if (Directory.Exists(dir))
            Directory.Delete(dir, recursive: true);

        _index.TryRemove(id, out _);
        return Task.CompletedTask;
    }

    // Zentralisierte Path-Konstruktion mit Whitelist-Validation +
    // Containment-Check. Verhindert Path-Injection ueber den
    // 'id'-Parameter: ohne diese Funktion wuerde Path.Combine(_rootDir, "../../etc")
    // den Root-Dir verlassen.
    private string GetMetadataDir(string id)
    {
        if (string.IsNullOrEmpty(id) || !IdPattern.IsMatch(id))
            throw new ArgumentException($"Invalid plugin id: '{id}'", nameof(id));

        var full = Path.GetFullPath(Path.Combine(_rootDir, id));
        if (!full.StartsWith(_rootDir + Path.DirectorySeparatorChar, StringComparison.Ordinal))
            throw new InvalidOperationException(
                $"Resolved metadata path '{full}' escapes root '{_rootDir}'.");

        return full;
    }

    public Task<IReadOnlyList<string>> GetVersionsAsync(string id, CancellationToken ct = default)
    {
        if (_index.TryGetValue(id, out var meta))
            return Task.FromResult<IReadOnlyList<string>>(meta.AllVersions);

        return Task.FromResult<IReadOnlyList<string>>([]);
    }

    public Task<PackageStatistics> GetStatisticsAsync(CancellationToken ct = default)
    {
        var stats = new PackageStatistics
        {
            TotalPackages = _index.Count,
            TotalDownloads = _index.Values.Sum(m => m.DownloadCount),
            TotalVersions = _index.Values.Sum(m => m.AllVersions.Count)
        };
        return Task.FromResult(stats);
    }

    public async Task IncrementDownloadCountAsync(string id, string version, CancellationToken ct = default)
    {
        if (_index.TryGetValue(id, out var meta))
        {
            meta.DownloadCount++;
            await SaveAsync(meta, ct);
        }
    }
}
