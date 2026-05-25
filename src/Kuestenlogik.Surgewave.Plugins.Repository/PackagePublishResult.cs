namespace Kuestenlogik.Surgewave.Plugins.Repository;

/// <summary>
/// Result of a package publish operation.
/// </summary>
public sealed record PackagePublishResult
{
    public bool Success { get; init; }
    public string? PackageId { get; init; }
    public string? Version { get; init; }
    public string? RegistryPath { get; init; }
    public string? Error { get; init; }
    public IReadOnlyList<string> Warnings { get; init; } = [];

    public static PackagePublishResult Succeeded(string packageId, string version, string registryPath, IReadOnlyList<string>? warnings = null) =>
        new()
        {
            Success = true,
            PackageId = packageId,
            Version = version,
            RegistryPath = registryPath,
            Warnings = warnings ?? []
        };

    public static PackagePublishResult Failed(string error, IReadOnlyList<string>? warnings = null) =>
        new()
        {
            Success = false,
            Error = error,
            Warnings = warnings ?? []
        };
}
