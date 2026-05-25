namespace Kuestenlogik.Surgewave.Plugins.Packaging;

/// <summary>
/// Result of a plugin package validation operation.
/// </summary>
public sealed class PackageValidationResult
{
    public bool IsValid { get; private init; }
    public PluginManifest? Manifest { get; private init; }
    public IReadOnlyList<string> Errors { get; private init; } = [];
    public IReadOnlyList<string> Warnings { get; private init; } = [];

    public static PackageValidationResult Valid(PluginManifest manifest, List<string>? warnings = null) =>
        new()
        {
            IsValid = true,
            Manifest = manifest,
            Warnings = warnings ?? []
        };

    public static PackageValidationResult Invalid(string error) =>
        new()
        {
            IsValid = false,
            Errors = [error]
        };

    public static PackageValidationResult Invalid(List<string> errors, List<string>? warnings = null) =>
        new()
        {
            IsValid = false,
            Errors = errors,
            Warnings = warnings ?? []
        };
}
