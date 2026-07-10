namespace Kuestenlogik.Surgewave.Clustering.Cluster;

/// <summary>
/// Neutral description of a broker feature range advertised during registration (#59 b5).
/// </summary>
/// <param name="Name">Feature name (e.g. "metadata.version").</param>
/// <param name="MinSupportedVersion">Minimum supported feature level.</param>
/// <param name="MaxSupportedVersion">Maximum supported feature level.</param>
public sealed record FeatureSpec(string Name, short MinSupportedVersion, short MaxSupportedVersion);
