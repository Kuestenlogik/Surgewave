namespace Kuestenlogik.Surgewave.Clustering.Cluster;

/// <summary>
/// Tracks the health state of a broker in the cluster.
/// Used for failure detection via heartbeating.
/// </summary>
public sealed class BrokerHealthState
{
    /// <summary>
    /// The broker ID this health state tracks.
    /// </summary>
    public int BrokerId { get; init; }

    /// <summary>
    /// Timestamp of the last received heartbeat.
    /// </summary>
    public DateTimeOffset LastHeartbeat { get; set; } = DateTimeOffset.UtcNow;

    /// <summary>
    /// Whether the broker is considered alive.
    /// </summary>
    public bool IsAlive { get; set; } = true;

    /// <summary>
    /// Number of consecutive heartbeat failures.
    /// </summary>
    public int ConsecutiveFailures { get; set; } = 0;

    /// <summary>
    /// The broker's current epoch (increments on restart).
    /// </summary>
    public int BrokerEpoch { get; set; } = 0;

    /// <summary>
    /// Time since last heartbeat in milliseconds.
    /// </summary>
    public long TimeSinceLastHeartbeatMs =>
        (long)(DateTimeOffset.UtcNow - LastHeartbeat).TotalMilliseconds;

    /// <summary>
    /// Update health state when heartbeat is received.
    /// </summary>
    public void RecordHeartbeat(int epoch)
    {
        LastHeartbeat = DateTimeOffset.UtcNow;
        ConsecutiveFailures = 0;
        IsAlive = true;
        BrokerEpoch = epoch;
    }

    /// <summary>
    /// Mark a heartbeat failure.
    /// </summary>
    public void RecordFailure()
    {
        ConsecutiveFailures++;
    }

    /// <summary>
    /// Mark the broker as failed.
    /// </summary>
    public void MarkFailed()
    {
        IsAlive = false;
    }

    /// <summary>
    /// Check if the broker should be considered failed based on timeout.
    /// </summary>
    public bool ShouldMarkFailed(int timeoutMs)
    {
        return IsAlive && TimeSinceLastHeartbeatMs > timeoutMs;
    }
}

/// <summary>
/// Heartbeat request sent between brokers.
/// </summary>
public sealed record HeartbeatRequest(
    int BrokerId,
    int BrokerEpoch,
    long Timestamp,
    int ControllerId,
    int ControllerEpoch
);

/// <summary>
/// Heartbeat response from a broker.
/// </summary>
public sealed record HeartbeatResponse(
    int BrokerId,
    int BrokerEpoch,
    long Timestamp,
    bool IsController
);
