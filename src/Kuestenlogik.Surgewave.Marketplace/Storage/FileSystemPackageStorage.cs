using System.Text.RegularExpressions;

namespace Kuestenlogik.Surgewave.Marketplace.Storage;

/// <summary>
/// Stores .swpkg packages on the local file system.
/// Layout: {dataDir}/packages/{id}/{version}/{id}-{version}.swpkg
/// </summary>
public sealed class FileSystemPackageStorage : IPackageStorageService
{
    // Plugin-IDs: kebab-case ASCII, optional dotted namespaces.
    private static readonly Regex IdPattern =
        new(@"^[a-z0-9](?:[a-z0-9.\-]{0,127})$", RegexOptions.Compiled);

    // SemVer-Subset ohne Path-Trennzeichen.
    private static readonly Regex VersionPattern =
        new(@"^[0-9]+(?:\.[0-9]+)*(?:-[A-Za-z0-9.]+)?$", RegexOptions.Compiled);

    private readonly string _rootDir;

    public FileSystemPackageStorage(string dataDirectory)
    {
        _rootDir = Path.GetFullPath(Path.Combine(dataDirectory, "packages"));
        Directory.CreateDirectory(_rootDir);
    }

    public Task<Stream> GetPackageAsync(string id, string version, CancellationToken ct = default)
    {
        var path = GetPackagePath(id, version);
        if (!File.Exists(path))
            throw new FileNotFoundException($"Package {id} v{version} not found", path);

        return Task.FromResult<Stream>(new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read));
    }

    public async Task SavePackageAsync(string id, string version, Stream content, CancellationToken ct = default)
    {
        var dir = GetPackageDir(id, version);
        Directory.CreateDirectory(dir);

        var path = GetPackagePath(id, version);
        await using var fileStream = new FileStream(path, FileMode.Create, FileAccess.Write);
        await content.CopyToAsync(fileStream, ct);
    }

    public Task DeletePackageAsync(string id, string version, CancellationToken ct = default)
    {
        var dir = GetPackageDir(id, version);
        if (Directory.Exists(dir))
            Directory.Delete(dir, recursive: true);

        // Clean up empty parent
        var idDir = EnsureContained(Path.Combine(_rootDir, ValidateId(id)));
        if (Directory.Exists(idDir) && !Directory.EnumerateFileSystemEntries(idDir).Any())
            Directory.Delete(idDir);

        return Task.CompletedTask;
    }

    public Task<bool> ExistsAsync(string id, string version, CancellationToken ct = default)
    {
        return Task.FromResult(File.Exists(GetPackagePath(id, version)));
    }

    public Task<long> GetPackageSizeAsync(string id, string version, CancellationToken ct = default)
    {
        var path = GetPackagePath(id, version);
        return Task.FromResult(File.Exists(path) ? new FileInfo(path).Length : 0L);
    }

    // Validation + Containment-Check sind zentralisiert in den
    // beiden Path-Helpers, damit jede Aufrufstelle automatisch gegen
    // Path-Injection geschuetzt ist. Selbst wenn 'id' oder 'version'
    // ../ enthielten, wuerde ValidateId/ValidateVersion das schon im
    // Pattern-Matcher ablehnen — der EnsureContained-Check ist
    // belt-and-suspenders gegen unicode/normalization-Tricks und gegen
    // zukuenftige Aufrufer, die den Pfad selbst bauen.
    private string GetPackageDir(string id, string version)
    {
        var path = Path.Combine(_rootDir, ValidateId(id), ValidateVersion(version));
        return EnsureContained(path);
    }

    private string GetPackagePath(string id, string version)
    {
        var safeId = ValidateId(id);
        var safeVersion = ValidateVersion(version);
        var path = Path.Combine(_rootDir, safeId, safeVersion, $"{safeId}-{safeVersion}.swpkg");
        return EnsureContained(path);
    }

    public async Task SaveSignatureAsync(string id, string version, string extension, Stream content, CancellationToken ct = default)
    {
        if (extension is not (".sig" or ".cms"))
            throw new ArgumentException($"Unsupported signature extension '{extension}' — must be .sig or .cms", nameof(extension));

        Directory.CreateDirectory(GetPackageDir(id, version));
        var path = GetPackagePath(id, version) + extension;
        await using var fileStream = new FileStream(path, FileMode.Create, FileAccess.Write);
        await content.CopyToAsync(fileStream, ct);
    }

    public Task<Stream?> GetSignatureAsync(string id, string version, CancellationToken ct = default)
    {
        var basePath = GetPackagePath(id, version);
        foreach (var ext in new[] { ".sig", ".cms" })
        {
            var path = basePath + ext;
            if (File.Exists(path))
                return Task.FromResult<Stream?>(new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read));
        }
        return Task.FromResult<Stream?>(null);
    }

    public Task<string?> GetSignatureExtensionAsync(string id, string version, CancellationToken ct = default)
    {
        var basePath = GetPackagePath(id, version);
        foreach (var ext in new[] { ".sig", ".cms" })
        {
            if (File.Exists(basePath + ext))
                return Task.FromResult<string?>(ext);
        }
        return Task.FromResult<string?>(null);
    }

    public async Task SaveSbomAsync(string id, string version, byte[] sbomBytes, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(sbomBytes);
        Directory.CreateDirectory(GetPackageDir(id, version));
        await File.WriteAllBytesAsync(GetSbomPath(id, version), sbomBytes, ct);
    }

    public Task<Stream?> GetSbomAsync(string id, string version, CancellationToken ct = default)
    {
        var path = GetSbomPath(id, version);
        if (!File.Exists(path)) return Task.FromResult<Stream?>(null);
        return Task.FromResult<Stream?>(new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read));
    }

    private string GetSbomPath(string id, string version) =>
        EnsureContained(Path.Combine(GetPackageDir(id, version), "sbom.json"));

    private static string ValidateId(string id)
    {
        if (string.IsNullOrEmpty(id) || !IdPattern.IsMatch(id))
            throw new ArgumentException($"Invalid plugin id: '{id}'", nameof(id));
        return id;
    }

    private static string ValidateVersion(string version)
    {
        if (string.IsNullOrEmpty(version) || !VersionPattern.IsMatch(version))
            throw new ArgumentException($"Invalid plugin version: '{version}'", nameof(version));
        return version;
    }

    private string EnsureContained(string candidate)
    {
        var full = Path.GetFullPath(candidate);
        if (!full.StartsWith(_rootDir + Path.DirectorySeparatorChar, StringComparison.Ordinal)
            && !string.Equals(full, _rootDir, StringComparison.Ordinal))
        {
            throw new InvalidOperationException(
                $"Resolved path '{full}' escapes package storage root '{_rootDir}'.");
        }
        return full;
    }
}
