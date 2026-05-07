namespace Kuestenlogik.Surgewave.Testing.Chaos;

/// <summary>
/// Defines the target scope of an injected fault.
/// Null properties act as wildcards, matching any value.
/// </summary>
public sealed record FaultScope
{
    /// <summary>
    /// Target broker ID, or null to match all brokers.
    /// </summary>
    public int? BrokerId { get; init; }

    /// <summary>
    /// Target topic name, or null to match all topics.
    /// </summary>
    public string? Topic { get; init; }

    /// <summary>
    /// Target partition ID, or null to match all partitions.
    /// </summary>
    public int? Partition { get; init; }

    /// <summary>
    /// Target peer ID for transport faults, or null to match all peers.
    /// </summary>
    public int? TargetPeerId { get; init; }

    /// <summary>
    /// Probability of the fault being triggered (0.0 to 1.0).
    /// Defaults to 1.0 (always triggered).
    /// </summary>
    public double Probability { get; init; } = 1.0;

    /// <summary>
    /// Checks whether this scope matches the given broker ID.
    /// Returns true if BrokerId is null (wildcard) or matches exactly.
    /// </summary>
    public bool Matches(int brokerId) => BrokerId is null || BrokerId == brokerId;

    /// <summary>
    /// Checks whether this scope matches the given peer ID.
    /// Returns true if TargetPeerId is null (wildcard) or matches exactly.
    /// </summary>
    public bool MatchesPeer(int peerId) => TargetPeerId is null || TargetPeerId == peerId;
}
