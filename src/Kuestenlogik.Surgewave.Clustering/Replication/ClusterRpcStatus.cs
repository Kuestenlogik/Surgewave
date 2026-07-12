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

    // #60: additional fencing/epoch status the native inter-broker control plane needs
    // (LeaderAndIsr / AlterPartition / registration). Values stay pinned to the matching Kafka
    // ErrorCode members so native and Kafka-wire peers interpret them identically during a
    // mixed-version rolling upgrade.
    ReplicaNotAvailable = 9,
    NotController = 41,
    FencedLeaderEpoch = 74,
    UnknownTopicId = 100,
}
