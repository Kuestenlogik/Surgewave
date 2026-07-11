namespace Kuestenlogik.Surgewave.Broker.Serverless;

/// <summary>
/// Lifecycle states for serverless broker instances.
/// Distinct from Clustering's <c>BrokerLifecycleState</c> which tracks Raft cluster membership.
/// </summary>
public enum ServerlessLifecycleState
{
    /// <summary>Starting up, loading metadata from object storage.</summary>
    ColdStarting,
    /// <summary>Warming caches, pre-fetching hot partitions.</summary>
    Warming,
    /// <summary>Fully operational, accepting reads and writes.</summary>
    Active,
    /// <summary>Draining: rejecting new writes, flushing buffers to remote storage.</summary>
    Draining,
    /// <summary>All data flushed, safe to terminate.</summary>
    ReadyToTerminate,
    /// <summary>Broker terminated.</summary>
    Terminated
}
