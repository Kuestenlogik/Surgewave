using System.Text.Json;
using Kuestenlogik.Surgewave.Plugins.Packaging;

namespace Kuestenlogik.Surgewave.Plugins.Repository;

/// <summary>
/// Publishes packages to a local file-based registry.
/// </summary>
public sealed class LocalRegistryPublisher : IPublishableRepository
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        WriteIndented = true
    };

    private readonly string _registryRoot;

    public LocalRegistryPublisher(string registryRoot)
    {
        _registryRoot = registryRoot;
    }

    public bool CanPublish => true;

    public async Task<PackagePublishResult> PublishAsync(string packagePath, bool force = false, CancellationToken cancellationToken = default)
    {
        var warnings = new List<string>();

        // Validate the package
        var manager = new PluginPackageManager();
        var validation = await manager.ValidateAsync(packagePath, cancellationToken);
        if (!validation.IsValid)
        {
            return PackagePublishResult.Failed($"Invalid package: {string.Join("; ", validation.Errors)}");
        }

        var manifest = validation.Manifest!;
        var packageId = manifest.Id;
        var version = manifest.Version;

        // Compute SHA256
        var sha256 = await PackageChecksumCalculator.ComputeAsync(packagePath, cancellationToken);

        // Create package directory: registry/packages/{id}/{version}/
        var packageDir = Path.Combine(_registryRoot, "packages", packageId, version);
        var targetFile = Path.Combine(packageDir, $"{packageId}.{version}.swpkg");

        // Check for existing package
        if (File.Exists(targetFile) && !force)
        {
            return PackagePublishResult.Failed(
                $"Package {packageId} v{version} already exists. Use --force to overwrite.");
        }

        if (File.Exists(targetFile))
        {
            warnings.Add($"Overwriting existing package {packageId} v{version}");
        }

        // Copy package file
        Directory.CreateDirectory(packageDir);
        File.Copy(packagePath, targetFile, overwrite: true);

        // Write SHA256 sidecar
        var checksumFile = targetFile + ".sha256";
        await File.WriteAllTextAsync(checksumFile, $"{sha256}  {Path.GetFileName(targetFile)}", cancellationToken);

        // Update packages.json atomically
        await UpdatePackagesJsonAsync(packageId, version, manifest, sha256, cancellationToken);

        return PackagePublishResult.Succeeded(packageId, version, targetFile, warnings);
    }

    private async Task UpdatePackagesJsonAsync(
        string packageId,
        string version,
        PluginManifest manifest,
        string sha256,
        CancellationToken cancellationToken)
    {
        var packagesJsonPath = Path.Combine(_registryRoot, "packages.json");
        var packages = new List<Dictionary<string, object?>>();

        // Load existing packages.json
        if (File.Exists(packagesJsonPath))
        {
            var existingJson = await File.ReadAllTextAsync(packagesJsonPath, cancellationToken);
            packages = JsonSerializer.Deserialize<List<Dictionary<string, object?>>>(existingJson, JsonOptions) ?? [];
        }

        // Find or create entry for this package
        var existing = packages.FirstOrDefault(p =>
            p.TryGetValue("packageId", out var id) &&
            id?.ToString() == packageId);

        if (existing != null)
        {
            // Update existing entry
            existing["version"] = version;
            existing["sha256"] = sha256;
            existing["published"] = DateTimeOffset.UtcNow.ToString("o");

            // Update available versions
            if (existing.TryGetValue("availableVersions", out var versionsObj) &&
                versionsObj is JsonElement versionsElement)
            {
                var versions = versionsElement.EnumerateArray()
                    .Select(v => v.GetString() ?? "")
                    .Where(v => v != version)
                    .ToList();
                versions.Insert(0, version);
                existing["availableVersions"] = versions;
            }
            else
            {
                existing["availableVersions"] = new List<string> { version };
            }
        }
        else
        {
            // Add new entry
            var nameParts = packageId.Replace("Kuestenlogik.Surgewave.Connector.", "");
            var displayName = nameParts.Replace(".", " ") + " Connector";

            packages.Add(new Dictionary<string, object?>
            {
                ["packageId"] = packageId,
                ["version"] = version,
                ["name"] = manifest.Name ?? displayName,
                ["description"] = manifest.Description ?? "",
                ["author"] = manifest.Authors?.FirstOrDefault() ?? "Unknown",
                ["tags"] = manifest.Tags?.ToList() ?? new List<string>(),
                ["availableVersions"] = new List<string> { version },
                ["connectorTypes"] = new List<string>(),
                ["published"] = DateTimeOffset.UtcNow.ToString("o"),
                ["downloadCount"] = 0,
                ["sha256"] = sha256,
                ["dependencies"] = new List<object>()
            });
        }

        // Write atomically via temp file
        var tempPath = packagesJsonPath + ".tmp";
        var json = JsonSerializer.Serialize(packages, JsonOptions);
        await File.WriteAllTextAsync(tempPath, json, cancellationToken);
        File.Move(tempPath, packagesJsonPath, overwrite: true);
    }
}
