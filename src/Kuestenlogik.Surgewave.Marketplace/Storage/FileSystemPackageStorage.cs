namespace Kuestenlogik.Surgewave.Marketplace.Storage;

/// <summary>
/// Stores .swpkg packages on the local file system.
/// Layout: {dataDir}/packages/{id}/{version}/{id}-{version}.swpkg
/// </summary>
public sealed class FileSystemPackageStorage : IPackageStorageService
{
    private readonly string _rootDir;

    public FileSystemPackageStorage(string dataDirectory)
    {
        _rootDir = Path.Combine(dataDirectory, "packages");
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
        var idDir = Path.Combine(_rootDir, id);
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

    private string GetPackageDir(string id, string version) =>
        Path.Combine(_rootDir, id, version);

    private string GetPackagePath(string id, string version) =>
        Path.Combine(_rootDir, id, version, $"{id}-{version}.swpkg");

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
        Path.Combine(GetPackageDir(id, version), "sbom.json");
}
