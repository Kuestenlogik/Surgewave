using Kuestenlogik.Surgewave.Clustering.Raft;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kuestenlogik.Surgewave.Testing.Chaos;

/// <summary>
/// Wraps an <see cref="IRaftTransport"/> to inject faults controlled by a <see cref="ChaosEngine"/>.
/// Checks for active faults before each RPC and either throws, delays, or drops the request.
/// </summary>
public sealed class ChaosRaftTransport : IRaftTransport
{
    private readonly IRaftTransport _inner;
    private readonly ChaosEngine _engine;
    private readonly int _brokerId;
    private readonly ILogger _logger;

    /// <summary>
    /// Creates a new chaos Raft transport wrapping the inner transport.
    /// </summary>
    /// <param name="inner">The actual transport to delegate to.</param>
    /// <param name="engine">The chaos engine controlling fault injection.</param>
    /// <param name="brokerId">The broker ID this transport belongs to.</param>
    /// <param name="loggerFactory">Optional logger factory for diagnostics.</param>
    public ChaosRaftTransport(IRaftTransport inner, ChaosEngine engine, int brokerId, ILoggerFactory? loggerFactory = null)
    {
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentNullException.ThrowIfNull(engine);

        _inner = inner;
        _engine = engine;
        _brokerId = brokerId;
        _logger = (loggerFactory ?? NullLoggerFactory.Instance).CreateLogger<ChaosRaftTransport>();
    }

    /// <inheritdoc />
    public IReadOnlyList<int> GetPeerIds()
    {
        ThrowIfCrashed();
        return _inner.GetPeerIds();
    }

    /// <inheritdoc />
    public async Task<PreVoteResponse> SendPreVoteAsync(int peerId, PreVoteRequest request, CancellationToken ct)
    {
        ThrowIfCrashed();
        ThrowIfPartitioned(peerId);
        ThrowIfConnectionReset(peerId);

        if (_engine.IsFaultActive(FaultType.LeaderElectionDisruption, _brokerId, peerId))
        {
            _logger.LogWarning("Chaos: Dropping PreVote request to peer {PeerId} from broker {BrokerId} (election disruption)",
                peerId, _brokerId);
            return new PreVoteResponse(Term: 0, VoteGranted: false);
        }

        await InjectLatencyAsync(peerId, ct);
        return await _inner.SendPreVoteAsync(peerId, request, ct);
    }

    /// <inheritdoc />
    public async Task<RequestVoteResponse> SendRequestVoteAsync(int peerId, RequestVoteRequest request, CancellationToken ct)
    {
        ThrowIfCrashed();
        ThrowIfPartitioned(peerId);
        ThrowIfConnectionReset(peerId);

        if (_engine.IsFaultActive(FaultType.LeaderElectionDisruption, _brokerId, peerId))
        {
            _logger.LogWarning("Chaos: Dropping RequestVote to peer {PeerId} from broker {BrokerId} (election disruption)",
                peerId, _brokerId);
            return new RequestVoteResponse(Term: 0, VoteGranted: false);
        }

        await InjectLatencyAsync(peerId, ct);
        return await _inner.SendRequestVoteAsync(peerId, request, ct);
    }

    /// <inheritdoc />
    public async Task<AppendEntriesResponse> SendAppendEntriesAsync(int peerId, AppendEntriesRequest request, CancellationToken ct)
    {
        ThrowIfCrashed();
        ThrowIfPartitioned(peerId);
        ThrowIfConnectionReset(peerId);
        await InjectLatencyAsync(peerId, ct);
        return await _inner.SendAppendEntriesAsync(peerId, request, ct);
    }

    /// <inheritdoc />
    public async Task<bool> IsPeerReachableAsync(int peerId, CancellationToken ct)
    {
        ThrowIfCrashed();

        if (_engine.IsFaultActive(FaultType.NetworkPartition, _brokerId, peerId))
        {
            _logger.LogDebug("Chaos: Peer {PeerId} unreachable from broker {BrokerId} (network partition)",
                peerId, _brokerId);
            return false;
        }

        return await _inner.IsPeerReachableAsync(peerId, ct);
    }

    private void ThrowIfCrashed()
    {
        if (_engine.IsFaultActive(FaultType.NodeCrash, _brokerId))
        {
            throw new InvalidOperationException($"Chaos: Node crash simulated on broker {_brokerId}");
        }
    }

    private void ThrowIfPartitioned(int peerId)
    {
        if (_engine.IsFaultActive(FaultType.NetworkPartition, _brokerId, peerId))
        {
            _logger.LogWarning("Chaos: Network partition between broker {BrokerId} and peer {PeerId}",
                _brokerId, peerId);
            throw new IOException($"Chaos: Network partition between broker {_brokerId} and peer {peerId}");
        }
    }

    private void ThrowIfConnectionReset(int peerId)
    {
        if (_engine.IsFaultActive(FaultType.ConnectionReset, _brokerId, peerId))
        {
            _logger.LogWarning("Chaos: Connection reset between broker {BrokerId} and peer {PeerId}",
                _brokerId, peerId);
            throw new IOException($"Chaos: Connection reset by peer {peerId} (simulated) on broker {_brokerId}");
        }
    }

    private async Task InjectLatencyAsync(int peerId, CancellationToken ct)
    {
        var latency = _engine.GetInjectedLatency(FaultType.SlowNetwork, _brokerId);
        if (latency.HasValue)
        {
            _logger.LogDebug("Chaos: Injecting {Latency}ms latency on broker {BrokerId} to peer {PeerId}",
                latency.Value.TotalMilliseconds, _brokerId, peerId);
            await Task.Delay(latency.Value, ct);
        }
    }
}
