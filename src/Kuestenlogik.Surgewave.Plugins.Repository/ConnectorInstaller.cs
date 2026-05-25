using System.IO.Compression;
using System.Reflection;
using System.Runtime.Loader;

namespace Kuestenlogik.Surgewave.Plugins.Repository;

/// <summary>
/// Manages installation and loading of connector packages.
/// </summary>
public sealed class ConnectorInstaller : IDisposable
{
    private static readonly System.Text.Json.JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true
    };

    private readonly string _installDirectory;
    private readonly Dictionary<string, AssemblyLoadContext> _loadContexts = new();
    private readonly Dictionary<string, InstalledConnector> _installedConnectors = new();

    /// <summary>
    /// Creates a new connector installer.
    /// </summary>
    /// <param name="installDirectory">Directory to install connectors to.</param>
    public ConnectorInstaller(string installDirectory)
    {
        _installDirectory = installDirectory;
        Directory.CreateDirectory(installDirectory);
        LoadInstalledConnectors();
    }

    /// <summary>
    /// Gets all installed connectors.
    /// </summary>
    public IReadOnlyDictionary<string, InstalledConnector> InstalledConnectors => _installedConnectors;

    /// <summary>
    /// Install a connector package.
    /// </summary>
    /// <param name="repository">Repository to download from.</param>
    /// <param name="packageId">Package ID.</param>
    /// <param name="version">Package version.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    public async Task InstallAsync(
        IConnectorRepository repository,
        string packageId,
        string version,
        CancellationToken cancellationToken = default)
    {
        var packageDir = GetPackageDirectory(packageId, version);

        // Remove existing installation if upgrading
        if (Directory.Exists(packageDir))
        {
            UnloadConnector(packageId);
            Directory.Delete(packageDir, true);
        }

        // Download package
        var tempDir = Path.Combine(Path.GetTempPath(), $"surgewave-connector-{Guid.NewGuid()}");
        Directory.CreateDirectory(tempDir);

        try
        {
            var packagePath = await repository.DownloadAsync(packageId, version, tempDir, cancellationToken);

            // Extract package
            Directory.CreateDirectory(packageDir);
            ZipFile.ExtractToDirectory(packagePath, packageDir, overwriteFiles: true);

            // Write metadata
            var metadata = new InstalledConnector
            {
                PackageId = packageId,
                Version = version,
                InstallDirectory = packageDir,
                InstalledAt = DateTimeOffset.UtcNow
            };

            await WriteMetadataAsync(packageDir, metadata, cancellationToken);
            _installedConnectors[packageId] = metadata;
        }
        finally
        {
            if (Directory.Exists(tempDir))
                Directory.Delete(tempDir, true);
        }
    }

    /// <summary>
    /// Uninstall a connector package.
    /// </summary>
    /// <param name="packageId">Package ID.</param>
    public void Uninstall(string packageId)
    {
        if (!_installedConnectors.TryGetValue(packageId, out var connector))
            return;

        UnloadConnector(packageId);

        if (Directory.Exists(connector.InstallDirectory))
        {
            Directory.Delete(connector.InstallDirectory, true);
        }

        _installedConnectors.Remove(packageId);
    }

    /// <summary>
    /// Load connector assemblies from an installed package.
    /// </summary>
    /// <param name="packageId">Package ID.</param>
    /// <returns>Loaded assemblies.</returns>
    public IReadOnlyList<Assembly> LoadConnector(string packageId)
    {
        if (!_installedConnectors.TryGetValue(packageId, out var connector))
            throw new InvalidOperationException($"Connector {packageId} is not installed");

        if (_loadContexts.TryGetValue(packageId, out var existingContext))
        {
            // Already loaded
            return existingContext.Assemblies.ToList();
        }

        var loadContext = new AssemblyLoadContext(packageId, isCollectible: true);
        var assemblies = new List<Assembly>();

        // Find DLLs in lib folder
        var libDir = Path.Combine(connector.InstallDirectory, "lib");
        if (Directory.Exists(libDir))
        {
            // Find the most appropriate target framework folder
            var tfmDir = FindBestTfmDirectory(libDir);
            if (tfmDir != null)
            {
                foreach (var dllPath in Directory.GetFiles(tfmDir, "*.dll"))
                {
                    try
                    {
                        var assembly = loadContext.LoadFromAssemblyPath(dllPath);
                        assemblies.Add(assembly);
                    }
                    catch (Exception)
                    {
                        // Skip assemblies that can't be loaded
                    }
                }
            }
        }

        _loadContexts[packageId] = loadContext;
        return assemblies;
    }

    /// <summary>
    /// Unload connector assemblies.
    /// </summary>
    /// <param name="packageId">Package ID.</param>
    public void UnloadConnector(string packageId)
    {
        if (_loadContexts.TryGetValue(packageId, out var loadContext))
        {
            loadContext.Unload();
            _loadContexts.Remove(packageId);
        }
    }

    /// <summary>
    /// Check if a connector is installed.
    /// </summary>
    /// <param name="packageId">Package ID.</param>
    /// <returns>True if installed.</returns>
    public bool IsInstalled(string packageId) => _installedConnectors.ContainsKey(packageId);

    /// <summary>
    /// Get installed version of a connector.
    /// </summary>
    /// <param name="packageId">Package ID.</param>
    /// <returns>Installed version or null.</returns>
    public string? GetInstalledVersion(string packageId) =>
        _installedConnectors.TryGetValue(packageId, out var connector) ? connector.Version : null;

    private string GetPackageDirectory(string packageId, string version) =>
        Path.Combine(_installDirectory, $"{packageId}.{version}");

    private void LoadInstalledConnectors()
    {
        if (!Directory.Exists(_installDirectory)) return;

        foreach (var dir in Directory.GetDirectories(_installDirectory))
        {
            // Bevorzugter Fall: connector.json (von ConnectorInstaller selber geschrieben,
            // typischerweise NuGet-Installs ueber dieses System).
            var metadataPath = Path.Combine(dir, "connector.json");
            if (File.Exists(metadataPath))
            {
                try
                {
                    var json = File.ReadAllText(metadataPath);
                    var metadata = System.Text.Json.JsonSerializer.Deserialize<InstalledConnector>(json);
                    if (metadata != null)
                    {
                        _installedConnectors[metadata.PackageId] = metadata;
                        continue;
                    }
                }
                catch (Exception)
                {
                    // Skip invalid metadata
                }
            }

            // Fallback: plugin.json (von .swpkg-Plugin-Pakete via PluginPackageManager.InstallAsync
            // angelegt). Schema: { "id": "...", "version": "...", "name": "...", "authors": [...],
            // "license": "...", "description": "...", "tags": [...] }
            // Siehe SurgewavePackageConventions.ManifestFileName.
            var manifestPath = Path.Combine(dir, "plugin.json");
            if (File.Exists(manifestPath))
            {
                try
                {
                    var json = File.ReadAllText(manifestPath);
                    using var doc = System.Text.Json.JsonDocument.Parse(json);
                    var root = doc.RootElement;
                    if (root.TryGetProperty("id", out var idEl)
                        && root.TryGetProperty("version", out var versionEl)
                        && idEl.ValueKind == System.Text.Json.JsonValueKind.String
                        && versionEl.ValueKind == System.Text.Json.JsonValueKind.String)
                    {
                        var packageId = idEl.GetString()!;
                        _installedConnectors[packageId] = new InstalledConnector
                        {
                            PackageId = packageId,
                            Version = versionEl.GetString()!,
                            InstallDirectory = dir,
                            InstalledAt = Directory.GetCreationTimeUtc(dir),
                            Name = TryGetString(root, "name") ?? packageId,
                            Author = TryGetFirstString(root, "authors") ?? string.Empty,
                            License = TryGetString(root, "license") ?? string.Empty,
                            Description = TryGetString(root, "description") ?? string.Empty,
                            Tags = TryGetStringArray(root, "tags")
                        };
                    }
                }
                catch (Exception)
                {
                    // Skip invalid manifest
                }
            }
        }
    }

    private static string? TryGetString(System.Text.Json.JsonElement root, string property)
        => root.TryGetProperty(property, out var el) && el.ValueKind == System.Text.Json.JsonValueKind.String
            ? el.GetString()
            : null;

    private static string? TryGetFirstString(System.Text.Json.JsonElement root, string property)
    {
        if (!root.TryGetProperty(property, out var el) || el.ValueKind != System.Text.Json.JsonValueKind.Array)
            return null;
        foreach (var item in el.EnumerateArray())
        {
            if (item.ValueKind == System.Text.Json.JsonValueKind.String)
                return item.GetString();
        }
        return null;
    }

    private static IReadOnlyList<string> TryGetStringArray(System.Text.Json.JsonElement root, string property)
    {
        if (!root.TryGetProperty(property, out var el) || el.ValueKind != System.Text.Json.JsonValueKind.Array)
            return [];
        var result = new List<string>(el.GetArrayLength());
        foreach (var item in el.EnumerateArray())
        {
            if (item.ValueKind == System.Text.Json.JsonValueKind.String)
            {
                var s = item.GetString();
                if (!string.IsNullOrEmpty(s)) result.Add(s);
            }
        }
        return result;
    }

    private static async Task WriteMetadataAsync(string dir, InstalledConnector metadata, CancellationToken ct)
    {
        var metadataPath = Path.Combine(dir, "connector.json");
        var json = System.Text.Json.JsonSerializer.Serialize(metadata, JsonOptions);
        await File.WriteAllTextAsync(metadataPath, json, ct);
    }

    private static string? FindBestTfmDirectory(string libDir)
    {
        // Prefer net10.0, then net9.0, net8.0, etc.
        var preferredTfms = new[] { "net10.0", "net9.0", "net8.0", "net7.0", "net6.0", "netstandard2.1", "netstandard2.0" };

        foreach (var tfm in preferredTfms)
        {
            var tfmPath = Path.Combine(libDir, tfm);
            if (Directory.Exists(tfmPath))
                return tfmPath;
        }

        // Fall back to first available
        var dirs = Directory.GetDirectories(libDir);
        return dirs.Length > 0 ? dirs[0] : null;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        foreach (var context in _loadContexts.Values)
        {
            context.Unload();
        }
        _loadContexts.Clear();
    }
}

/// <summary>
/// Information about an installed connector.
/// </summary>
public sealed record InstalledConnector
{
    /// <summary>
    /// Package ID.
    /// </summary>
    public required string PackageId { get; init; }

    /// <summary>
    /// Installed version.
    /// </summary>
    public required string Version { get; init; }

    /// <summary>
    /// Installation directory path.
    /// </summary>
    public required string InstallDirectory { get; init; }

    /// <summary>
    /// Installation timestamp.
    /// </summary>
    public DateTimeOffset InstalledAt { get; init; }

    /// <summary>Display name (z.B. "Akka") — aus plugin.json:name. Default = PackageId.</summary>
    public string Name { get; init; } = string.Empty;

    /// <summary>Erster Author aus plugin.json:authors[] — UI rendert ihn in Plugin-Cards.</summary>
    public string Author { get; init; } = string.Empty;

    /// <summary>SPDX-License-Identifier aus plugin.json:license. Beispiel: "Apache-2.0".</summary>
    public string License { get; init; } = string.Empty;

    /// <summary>Optional, beschreibt das Plugin (plugin.json:description).</summary>
    public string Description { get; init; } = string.Empty;

    /// <summary>Optional, plugin.json:tags[] — fuer Such-/Filter-Logik.</summary>
    public IReadOnlyList<string> Tags { get; init; } = [];
}
