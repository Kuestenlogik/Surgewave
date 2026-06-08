namespace Kuestenlogik.Surgewave.Control.Models.License;

/// <summary>
/// View-model copy of <c>Kuestenlogik.Surgewave.Broker.Plugins.LicenseStatusResponse</c>.
/// Kept here so the Control project doesn't need a hard reference on
/// the broker assembly.
/// </summary>
public sealed record LicenseStatusModel(
    string Edition,
    string? LicensedTo,
    DateTimeOffset? ExpiresAt,
    bool HasProvider);

/// <summary>
/// View-model copy of <c>Kuestenlogik.Surgewave.Broker.Plugins.LicensePluginRow</c>.
/// </summary>
public sealed record LicensePluginRowModel(
    string FeatureId,
    string DisplayName,
    bool RequiresLicense,
    bool IsActive,
    bool IsLicensed);
