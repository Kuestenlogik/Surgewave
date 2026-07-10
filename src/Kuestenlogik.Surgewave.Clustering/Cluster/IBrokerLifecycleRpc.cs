namespace Kuestenlogik.Surgewave.Clustering.Cluster;

/// <summary>
/// Neutral broker-to-controller lifecycle RPC surface (#59 b5): registration and periodic
/// heartbeat. Inputs and outcomes are protocol-neutral records carrying only the fields the
/// caller supplies or reads, so no Kafka DTO is exposed here. The concrete wire
/// implementation lives in <see cref="BrokerLifecycleManager"/>.
/// </summary>
public interface IBrokerLifecycleRpc
{
    /// <summary>
    /// Register this broker with the controller and return the assigned epoch/status.
    /// </summary>
    Task<BrokerRegistrationOutcome> RegisterAsync(BrokerRegistrationInput input, CancellationToken ct = default);

    /// <summary>
    /// Send a heartbeat to the controller and return the reported fencing/caught-up/shutdown state.
    /// </summary>
    Task<BrokerHeartbeatOutcome> HeartbeatAsync(BrokerHeartbeatInput input, CancellationToken ct = default);
}
