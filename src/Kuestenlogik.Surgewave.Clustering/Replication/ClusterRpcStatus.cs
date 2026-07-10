namespace Kuestenlogik.Surgewave.Clustering.Replication;

/// <summary>
/// Neutral status codes for Surgewave inter-broker replication/metadata RPC. The numeric
/// values are PINNED to the Kafka ErrorCode members they replace — they go on the
/// inter-broker wire as raw int16 and are read back as raw shorts by the peer, so the
/// numbers are load-bearing (#59 b5). Do not renumber.
/// </summary>
public enum ClusterRpcStatus : short
{
    None = 0,
    Unknown = -1,
    NotLeaderForPartition = 6,
    StaleControllerEpoch = 11,
    UnsupportedVersion = 35,
    StaleBrokerEpoch = 77,
}
