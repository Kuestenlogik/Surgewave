using System.Diagnostics.CodeAnalysis;

namespace Kuestenlogik.Surgewave.Plugins.Repository;

/// <summary>
/// Information about a connector package in the repository.
/// </summary>
[SuppressMessage("Design", "CA1056:URI properties should not be strings", Justification = "URLs from NuGet metadata are strings")]
public sealed record ConnectorPackageInfo
{
    /// <summary>
    /// Package ID (e.g., "Kuestenlogik.Surgewave.Connector.Hue").
    /// </summary>
    public required string PackageId { get; init; }

    /// <summary>
    /// Package version.
    /// </summary>
    public required string Version { get; init; }

    /// <summary>
    /// Connector display name.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// Connector description.
    /// </summary>
    public string? Description { get; init; }

    /// <summary>
    /// Author name.
    /// </summary>
    public string? Author { get; init; }

    /// <summary>
    /// Package icon URL.
    /// </summary>
    public string? IconUrl { get; init; }

    /// <summary>
    /// Project/repository URL.
    /// </summary>
    public string? ProjectUrl { get; init; }

    /// <summary>
    /// License expression or URL.
    /// </summary>
    public string? License { get; init; }

    /// <summary>
    /// Tags for discovery.
    /// </summary>
    public IReadOnlyList<string> Tags { get; init; } = [];

    /// <summary>
    /// Available versions in descending order.
    /// </summary>
    public IReadOnlyList<string> AvailableVersions { get; init; } = [];

    /// <summary>
    /// Whether the connector is currently installed.
    /// </summary>
    public bool IsInstalled { get; init; }

    /// <summary>
    /// Installed version if applicable.
    /// </summary>
    public string? InstalledVersion { get; init; }

    /// <summary>
    /// Total download count.
    /// </summary>
    public long DownloadCount { get; init; }

    /// <summary>
    /// Publication date.
    /// </summary>
    public DateTimeOffset? Published { get; init; }

    /// <summary>
    /// Connector types included in the package (source, sink, or both).
    /// </summary>
    public IReadOnlyList<string> ConnectorTypes { get; init; } = [];

    /// <summary>
    /// SHA256 checksum of the package file.
    /// </summary>
    public string? Sha256 { get; init; }

    /// <summary>
    /// Surgewave plugin dependencies (other connector packages this depends on).
    /// </summary>
    public IReadOnlyList<ConnectorDependencyInfo> Dependencies { get; init; } = [];

    /// <summary>
    /// Whether this package has dependencies that need to be resolved.
    /// </summary>
    public bool HasDependencies => Dependencies.Count > 0;

    /// <summary>
    /// Whether the marketplace verified a cryptographic signature on this package at upload.
    /// Repositories that do not supply signature metadata leave this <c>false</c>.
    /// </summary>
    public bool IsSigned { get; init; }

    /// <summary>
    /// Human-readable identity of the signer when <see cref="IsSigned"/> is <c>true</c>
    /// (e.g. <c>mycompany</c> for builtin-ecdsa, or a cert subject for sealbolt). <c>null</c> otherwise.
    /// </summary>
    public string? SignerIdentity { get; init; }

    /// <summary>
    /// Name of the <c>ISppSignerProvider</c> that verified the signature
    /// (<c>builtin-ecdsa</c>, <c>sealbolt</c>, ...). <c>null</c> when unsigned.
    /// </summary>
    public string? SignerProvider { get; init; }
}

/// <summary>
/// Information about a connector dependency.
/// </summary>
public sealed record ConnectorDependencyInfo
{
    /// <summary>
    /// The package ID of the dependency.
    /// </summary>
    public required string PackageId { get; init; }

    /// <summary>
    /// Version constraint (e.g., ">=1.0.0", "^2.0.0").
    /// </summary>
    public string VersionConstraint { get; init; } = "*";

    /// <summary>
    /// Whether this dependency is optional.
    /// </summary>
    public bool Optional { get; init; }
}
