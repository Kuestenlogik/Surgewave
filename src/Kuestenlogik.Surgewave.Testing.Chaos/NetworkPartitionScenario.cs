namespace Kuestenlogik.Surgewave.Testing.Chaos;

/// <summary>
/// Simulates a network partition that isolates a broker from a set of peers.
/// Creates bidirectional partition faults so neither side can communicate with the other.
/// Disposing or calling <see cref="Heal"/> removes all partition faults.
/// </summary>
public sealed class NetworkPartitionScenario : IDisposable
{
    private readonly ChaosEngine _engine;
    private readonly List<string> _faultIds = [];
    private bool _disposed;

    private NetworkPartitionScenario(ChaosEngine engine)
    {
        _engine = engine;
    }

    /// <summary>
    /// Creates a network partition isolating the specified broker from the other brokers.
    /// </summary>
    /// <param name="engine">The chaos engine to inject faults into.</param>
    /// <param name="isolatedBrokerId">The broker ID to isolate.</param>
    /// <param name="otherBrokerIds">The broker IDs to partition from.</param>
    /// <returns>A scenario that can be healed or disposed.</returns>
    public static NetworkPartitionScenario Create(ChaosEngine engine, int isolatedBrokerId, IEnumerable<int> otherBrokerIds)
    {
        ArgumentNullException.ThrowIfNull(engine);
        ArgumentNullException.ThrowIfNull(otherBrokerIds);

        var scenario = new NetworkPartitionScenario(engine);

        foreach (var peerId in otherBrokerIds)
        {
            // Partition from isolated broker to peer
            var id1 = engine.ActivateFault(FaultType.NetworkPartition, new FaultScope
            {
                BrokerId = isolatedBrokerId,
                TargetPeerId = peerId
            });
            scenario._faultIds.Add(id1);

            // Partition from peer back to isolated broker
            var id2 = engine.ActivateFault(FaultType.NetworkPartition, new FaultScope
            {
                BrokerId = peerId,
                TargetPeerId = isolatedBrokerId
            });
            scenario._faultIds.Add(id2);
        }

        return scenario;
    }

    /// <summary>
    /// Heals the network partition, restoring communication.
    /// </summary>
    public void Heal()
    {
        foreach (var id in _faultIds)
        {
            _engine.DeactivateFault(id);
        }
        _faultIds.Clear();
    }

    /// <summary>
    /// Heals the partition and disposes the scenario.
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;
        Heal();
    }
}
