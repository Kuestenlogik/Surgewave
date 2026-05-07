using System.Diagnostics.CodeAnalysis;

namespace Kuestenlogik.Surgewave.Client.Native.Operations.Plugins;

/// <summary>
/// Information about a plugin package.
/// </summary>
[SuppressMessage("Design", "CA1056:URI-like parameters should not be strings",
    Justification = "Wire protocol uses strings for URLs to avoid Uri serialization overhead")]
[SuppressMessage("Design", "CA1054:URI-like parameters should not be strings",
    Justification = "Wire protocol uses strings for URLs to avoid Uri serialization overhead")]
public sealed record PluginInfo(
    string PackageId,
    string Name,
    string Version,
    string? Description,
    string? Author,
    string? License,
    string? ProjectUrl,
    string? IconUrl,
    bool IsInstalled,
    string? InstalledVersion,
    long DownloadCount,
    IReadOnlyList<string> ConnectorTypes,
    IReadOnlyList<string> Tags,
    IReadOnlyList<string> AvailableVersions,
    IReadOnlyList<PluginDependency> Dependencies,
    bool IsSigned = false,
    string? SignerIdentity = null,
    string? SignerProvider = null);

/// <summary>
/// Plugin dependency information.
/// </summary>
public sealed record PluginDependency(
    string Id,
    string Version,
    bool Optional);

/// <summary>
/// Result of a plugin search operation.
/// </summary>
public sealed record PluginSearchResult(
    IReadOnlyList<PluginInfo> Plugins,
    int TotalCount);

/// <summary>
/// Information about an installed package.
/// </summary>
public sealed record InstalledPackageInfo(
    string PackageId,
    string Version,
    bool WasDependency);

/// <summary>
/// Result of a plugin installation operation.
/// </summary>
public sealed record PluginInstallResult(
    bool IsSuccess,
    bool IsPartialSuccess,
    IReadOnlyList<InstalledPackageInfo> InstalledPackages,
    IReadOnlyList<string> Errors);

/// <summary>
/// Result of a plugin uninstall operation.
/// </summary>
public sealed record PluginUninstallResult(
    bool IsSuccess,
    IReadOnlyList<string> RemovedPackages,
    string? Error);

/// <summary>
/// A node in the plugin dependency tree.
/// </summary>
public sealed record DependencyTreeNode(
    string PackageId,
    string Version,
    bool IsInstalled,
    bool IsMissing,
    bool IsCircular,
    IReadOnlyList<DependencyTreeNode> Children);
