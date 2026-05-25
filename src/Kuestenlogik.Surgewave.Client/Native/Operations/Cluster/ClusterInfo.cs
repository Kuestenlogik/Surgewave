namespace Kuestenlogik.Surgewave.Client.Native.Operations.Cluster;

/// <summary>
/// Cluster information.
/// </summary>
public record ClusterInfo
{
    public int BrokerId { get; init; }
    public string Host { get; init; } = string.Empty;
    public int Port { get; init; }
    public bool IsController { get; init; }
    public int ControllerId { get; init; }
    public int ControllerEpoch { get; init; }
    public bool UseRaftConsensus { get; init; }
    public bool IsRaftLeader { get; init; }
    public int RaftTerm { get; init; }
    public int TopicCount { get; init; }
    public int TotalPartitions { get; init; }
}
