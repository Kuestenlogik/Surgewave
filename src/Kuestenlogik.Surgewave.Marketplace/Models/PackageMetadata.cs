using System.Text.Json.Serialization;

namespace Kuestenlogik.Surgewave.Marketplace.Models;

/// <summary>
/// Metadata for a published plugin package in the marketplace.
/// </summary>
public sealed class PackageMetadata
{
    [JsonPropertyName("id")]
    public required string Id { get; init; }

    [JsonPropertyName("version")]
    public required string Version { get; set; }

    [JsonPropertyName("name")]
    public required string Name { get; set; }

    [JsonPropertyName("description")]
    public string? Description { get; set; }

    [JsonPropertyName("authors")]
    public string[]? Authors { get; set; }

    [JsonPropertyName("tags")]
    public string[]? Tags { get; set; }

    [JsonPropertyName("license")]
    public string? License { get; set; }

    [JsonPropertyName("projectUrl")]
    public string? ProjectUrl { get; set; }

    [JsonPropertyName("iconUrl")]
    public string? IconUrl { get; set; }

    [JsonPropertyName("downloadCount")]
    public long DownloadCount { get; set; }

    [JsonPropertyName("publishedAt")]
    public DateTimeOffset PublishedAt { get; set; }

    [JsonPropertyName("listed")]
    public bool Listed { get; set; } = true;

    [JsonPropertyName("sha256")]
    public string? Sha256 { get; set; }

    [JsonPropertyName("packageSize")]
    public long PackageSize { get; set; }

    [JsonPropertyName("assemblies")]
    public string[]? Assemblies { get; set; }

    [JsonPropertyName("allVersions")]
    public List<string> AllVersions { get; set; } = [];

    /// <summary>
    /// Whether the package carries a cryptographic signature that the marketplace has verified.
    /// <c>false</c> for unsigned uploads (permitted when <c>Surgewave:Marketplace:RequireSignedUploads</c>
    /// is false) — in that case <see cref="SignerIdentity"/> and <see cref="SignerProvider"/> are null.
    /// </summary>
    [JsonPropertyName("isSigned")]
    public bool IsSigned { get; set; }

    /// <summary>
    /// Human-readable identity of the signer (public key filename for builtin-ecdsa, cert subject
    /// common name for sealbolt). Populated only when <see cref="IsSigned"/> is <c>true</c>.
    /// </summary>
    [JsonPropertyName("signerIdentity")]
    public string? SignerIdentity { get; set; }

    /// <summary>
    /// Name of the <see cref="Kuestenlogik.Surgewave.Plugins.Packaging.ISppSignerProvider"/> that verified the
    /// signature, e.g. "builtin-ecdsa" or "sealbolt". Null when unsigned.
    /// </summary>
    [JsonPropertyName("signerProvider")]
    public string? SignerProvider { get; set; }

    /// <summary>
    /// Whether the <c>.swpkg</c> ships a CycloneDX Software Bill of Materials (<c>sbom.json</c>).
    /// When <c>true</c>, the document is served from
    /// <c>/api/v1/packages/{id}/{version}/sbom</c>.
    /// </summary>
    [JsonPropertyName("hasSbom")]
    public bool HasSbom { get; set; }
}

/// <summary>
/// Overall marketplace statistics.
/// </summary>
public sealed class PackageStatistics
{
    public int TotalPackages { get; init; }
    public long TotalDownloads { get; init; }
    public int TotalVersions { get; init; }
}
