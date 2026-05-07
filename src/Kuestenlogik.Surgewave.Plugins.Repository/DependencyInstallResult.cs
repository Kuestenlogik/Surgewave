namespace Kuestenlogik.Surgewave.Plugins.Repository;

/// <summary>
/// Result of installing a package with its dependencies.
/// </summary>
public sealed record DependencyInstallResult
{
    /// <summary>
    /// Whether installation was fully successful.
    /// </summary>
    public bool IsSuccess { get; init; }

    /// <summary>
    /// Whether some packages were installed but not all.
    /// </summary>
    public bool IsPartialSuccess { get; init; }

    /// <summary>
    /// Packages that were installed (new or upgraded).
    /// </summary>
    public IReadOnlyList<InstalledPackageInfo> InstalledPackages { get; init; } = [];

    /// <summary>
    /// Packages that were already installed (not changed).
    /// </summary>
    public IReadOnlyList<InstalledPackageInfo> AlreadyInstalledPackages { get; init; } = [];

    /// <summary>
    /// Errors encountered during installation.
    /// </summary>
    public IReadOnlyList<string> Errors { get; init; } = [];

    /// <summary>
    /// Warnings (non-fatal issues).
    /// </summary>
    public IReadOnlyList<string> Warnings { get; init; } = [];

    /// <summary>
    /// Total number of packages affected (installed + already installed).
    /// </summary>
    public int TotalPackages => InstalledPackages.Count + AlreadyInstalledPackages.Count;

    /// <summary>
    /// Number of new packages installed.
    /// </summary>
    public int NewlyInstalled => InstalledPackages.Count(p => !p.WasUpgraded);

    /// <summary>
    /// Number of packages upgraded.
    /// </summary>
    public int Upgraded => InstalledPackages.Count(p => p.WasUpgraded);

    /// <summary>
    /// Create a successful result.
    /// </summary>
    public static DependencyInstallResult Succeeded(
        IReadOnlyList<InstalledPackageInfo> installed,
        IReadOnlyList<InstalledPackageInfo> alreadyInstalled,
        IReadOnlyList<string>? warnings = null) => new()
    {
        IsSuccess = true,
        InstalledPackages = installed,
        AlreadyInstalledPackages = alreadyInstalled,
        Warnings = warnings ?? []
    };

    /// <summary>
    /// Create a partial success result.
    /// </summary>
    public static DependencyInstallResult PartialSuccess(
        IReadOnlyList<InstalledPackageInfo> installed,
        IReadOnlyList<string> errors,
        IReadOnlyList<string>? warnings = null) => new()
    {
        IsSuccess = false,
        IsPartialSuccess = true,
        InstalledPackages = installed,
        Errors = errors,
        Warnings = warnings ?? []
    };

    /// <summary>
    /// Create a failed result.
    /// </summary>
    public static DependencyInstallResult Failed(
        IReadOnlyList<string> errors,
        IReadOnlyList<string>? warnings = null) => new()
    {
        IsSuccess = false,
        Errors = errors,
        Warnings = warnings ?? []
    };
}

/// <summary>
/// Information about an installed package.
/// </summary>
public sealed record InstalledPackageInfo
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
    /// Whether this was installed as a dependency.
    /// </summary>
    public bool IsDependency { get; init; }

    /// <summary>
    /// Whether this was an upgrade from a previous version.
    /// </summary>
    public bool WasUpgraded { get; init; }

    /// <summary>
    /// Previous version if upgraded.
    /// </summary>
    public string? PreviousVersion { get; init; }
}
