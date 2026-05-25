namespace Kuestenlogik.Surgewave.Marketplace.Storage;

/// <summary>
/// Abstraction for storing and retrieving .swpkg plugin package files.
/// </summary>
public interface IPackageStorageService
{
    Task<Stream> GetPackageAsync(string id, string version, CancellationToken ct = default);
    Task SavePackageAsync(string id, string version, Stream content, CancellationToken ct = default);
    Task DeletePackageAsync(string id, string version, CancellationToken ct = default);
    Task<bool> ExistsAsync(string id, string version, CancellationToken ct = default);
    Task<long> GetPackageSizeAsync(string id, string version, CancellationToken ct = default);

    /// <summary>
    /// Saves the sidecar signature bytes for a package (<c>.sig</c> or <c>.cms</c>, determined by
    /// <paramref name="extension"/>). The marketplace preserves the exact sidecar format uploaded
    /// by the publisher so consumers can install using the same verification provider.
    /// </summary>
    Task SaveSignatureAsync(string id, string version, string extension, Stream content, CancellationToken ct = default);

    /// <summary>
    /// Returns the signature stream for a package, or <c>null</c> if no signature was uploaded.
    /// Callers can look up the extension separately via <see cref="GetSignatureExtensionAsync"/>.
    /// </summary>
    Task<Stream?> GetSignatureAsync(string id, string version, CancellationToken ct = default);

    /// <summary>Gets the extension of the stored signature file (<c>.sig</c> or <c>.cms</c>), or null if no signature exists.</summary>
    Task<string?> GetSignatureExtensionAsync(string id, string version, CancellationToken ct = default);

    /// <summary>
    /// Persists the bytes of a CycloneDX SBOM extracted from the uploaded package. Stored next
    /// to the <c>.swpkg</c> so downloads can serve it without re-unpacking the archive.
    /// </summary>
    Task SaveSbomAsync(string id, string version, byte[] sbomBytes, CancellationToken ct = default);

    /// <summary>Returns the stored SBOM stream, or <c>null</c> if the package had no SBOM.</summary>
    Task<Stream?> GetSbomAsync(string id, string version, CancellationToken ct = default);
}
