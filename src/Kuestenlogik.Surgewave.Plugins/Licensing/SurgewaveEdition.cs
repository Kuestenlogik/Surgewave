namespace Kuestenlogik.Surgewave.Plugins.Licensing;

/// <summary>
/// The edition of the running Surgewave instance.
/// </summary>
public enum SurgewaveEdition
{
    /// <summary>Community Edition — core features only.</summary>
    Community = 0,

    /// <summary>Enterprise Edition — all features, backed by a valid license.</summary>
    Enterprise = 1,
}
