namespace Kuestenlogik.Surgewave.Testing.Chaos;

/// <summary>
/// Types of faults that can be injected into a Surgewave cluster.
/// </summary>
public enum FaultType
{
    /// <summary>
    /// Simulates a network partition between nodes, preventing communication.
    /// </summary>
    NetworkPartition,

    /// <summary>
    /// Simulates a complete node crash where all operations fail.
    /// </summary>
    NodeCrash,

    /// <summary>
    /// Simulates disk I/O errors on storage operations.
    /// </summary>
    DiskIoError,

    /// <summary>
    /// Simulates slow network by injecting latency into operations.
    /// </summary>
    SlowNetwork,

    /// <summary>
    /// Simulates message corruption by flipping bits in read data.
    /// </summary>
    MessageCorruption,

    /// <summary>
    /// Disrupts leader election by dropping vote requests.
    /// </summary>
    LeaderElectionDisruption,

    /// <summary>
    /// Simulates a full disk where no more data can be written.
    /// </summary>
    StorageFullError,

    /// <summary>
    /// Simulates a connection reset during communication.
    /// </summary>
    ConnectionReset,

    /// <summary>
    /// Simulates partial writes where only some data is persisted.
    /// </summary>
    PartialWrite
}
