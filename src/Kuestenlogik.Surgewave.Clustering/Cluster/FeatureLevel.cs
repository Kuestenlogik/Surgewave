namespace Kuestenlogik.Surgewave.Clustering.Cluster;

/// <summary>
/// One feature this broker announces: the range of levels it supports and the level it runs
/// at (the finalized level). Kafka finalizes every feature at a single level, so the finalized
/// range is <c>[Finalized, Finalized]</c> (#199).
/// </summary>
/// <param name="Name">The feature name, e.g. <c>metadata.version</c>.</param>
/// <param name="MinSupported">The lowest supported level.</param>
/// <param name="MaxSupported">The highest supported level.</param>
/// <param name="Finalized">The level the broker runs at; within the supported range.</param>
public sealed record FeatureLevel(string Name, short MinSupported, short MaxSupported, short Finalized)
{
    /// <summary>The supported range as the feature spec a broker registration carries.</summary>
    public FeatureSpec ToFeatureSpec() => new(Name, MinSupported, MaxSupported);
}

/// <summary>
/// A hard dependency between two feature levels, as Kafka declares them in
/// <c>FeatureVersion.dependencies()</c>: level <paramref name="Level"/> of
/// <paramref name="Feature"/> may only be finalized while <paramref name="RequiredFeature"/> is
/// finalized at <paramref name="RequiredLevel"/> or higher (#199).
/// </summary>
public sealed record FeatureDependency(string Feature, short Level, string RequiredFeature, short RequiredLevel);
