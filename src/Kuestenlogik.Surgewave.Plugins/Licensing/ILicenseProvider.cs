namespace Kuestenlogik.Surgewave.Plugins.Licensing;

/// <summary>
/// Optional license provider for Surgewave.
/// When registered, the broker can distinguish between Community and Enterprise editions
/// and gate enterprise features accordingly.
/// When not registered (null), all community features are available.
/// </summary>
public interface ILicenseProvider
{
    /// <summary>The running edition (Community or Enterprise).</summary>
    SurgewaveEdition Edition { get; }

    /// <summary>Name of the licensee, or null for Community.</summary>
    string? LicensedTo { get; }

    /// <summary>License expiration date, or null for Community/perpetual.</summary>
    DateTimeOffset? ExpiresAt { get; }

    /// <summary>Check if a specific feature is licensed.</summary>
    bool IsFeatureEnabled(string featureName);
}
