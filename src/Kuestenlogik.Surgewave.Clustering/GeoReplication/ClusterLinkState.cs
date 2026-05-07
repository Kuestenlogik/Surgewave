namespace Kuestenlogik.Surgewave.Clustering.GeoReplication;

/// <summary>
/// State of a cluster link.
/// </summary>
public enum ClusterLinkState
{
    Initializing,
    Active,
    Paused,
    Error
}

/// <summary>
/// Status information for a cluster link.
/// </summary>
public sealed record ClusterLinkStatus
{
    public required string LinkId { get; init; }
    public required ClusterLinkState State { get; init; }
    public string? RemoteClusterId { get; init; }
    public int MirroredTopicCount { get; init; }
    public long TotalLagMessages { get; init; }
    public DateTimeOffset? LastFetchTimestamp { get; init; }
    public string? ErrorMessage { get; init; }
}
