using Kuestenlogik.Surgewave.Clustering.Replication;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.Cluster;

/// <summary>
/// #60 Inc6b — the protocol-neutral broker-lifecycle loop: registers this broker with the controller
/// and heartbeats periodically, driving an injected <see cref="IBrokerLifecycleRpc"/> (native SRWV in
/// a plugin-free broker). It replaces the never-instantiated Kafka-wire <c>BrokerLifecycleManager</c>
/// and is the piece that actually makes a broker JOIN — so a native-only broker becomes visible to the
/// controller and the finalized inter-broker protocol level can rise to native.
/// <para>
/// It no-ops on the controller/seed itself: the RPC resolves the controller and refuses to dial self,
/// so the lowest-id broker (which is already self-registered in <see cref="ClusterState"/> at startup)
/// simply finds no controller to register with and idles. Standalone (no <c>ClusterNodes</c>) idles too.
/// </para>
/// </summary>
public sealed partial class BrokerLifecycleLoop : IAsyncDisposable
{
    private readonly IBrokerLifecycleRpc _rpc;
    private readonly ClusteringConfig _config;
    private readonly ILogger<BrokerLifecycleLoop> _logger;
    private readonly Func<long>? _metadataPosition;

    private readonly Guid _incarnationId = Guid.NewGuid();
    private long _brokerEpoch = -1;
    private CancellationTokenSource? _cts;
    private Task? _loopTask;

    /// <param name="metadataPosition">
    /// This broker's consumed metadata position, or <c>null</c> when it has none.
    /// </param>
    /// <remarks>
    /// Supplied only in Raft mode, where metadata is a replicated log and there is a
    /// position to be behind. In push mode there is none by construction, and the
    /// controller's caught-up check is correspondingly permissive — see the comment at
    /// the call site below (#160).
    /// </remarks>
    public BrokerLifecycleLoop(
        IBrokerLifecycleRpc rpc,
        ClusteringConfig config,
        ILogger<BrokerLifecycleLoop> logger,
        Func<long>? metadataPosition = null)
    {
        _rpc = rpc;
        _config = config;
        _logger = logger;
        _metadataPosition = metadataPosition;
    }

    /// <summary>The broker epoch assigned by the controller, or -1 before a successful registration.</summary>
    public long BrokerEpoch => Interlocked.Read(ref _brokerEpoch);

    public Task StartAsync(CancellationToken cancellationToken)
    {
        // A configured controller quorum is a cluster too (#169). Gating on ClusterNodes alone
        // would leave a broker that was told only where the controllers are idling forever —
        // and that is the shape an observer is deployed in, since it does not need to know
        // every broker, only the quorum.
        if (string.IsNullOrEmpty(_config.ClusterNodes)
            && string.IsNullOrWhiteSpace(_config.ControllerQuorumVoters))
        {
            LogStandalone();
            return Task.CompletedTask;
        }

        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _loopTask = Task.Run(() => LifecycleLoopAsync(_cts.Token), _cts.Token);
        return Task.CompletedTask;
    }

    // Retry cadence while still trying to register (join fast, then settle into HeartbeatIntervalMs).
    private int RegistrationRetryMs => Math.Min(1000, _config.HeartbeatIntervalMs);

    /// <summary>
    /// One unified, PACED loop: while unregistered it attempts registration each
    /// <see cref="RegistrationRetryMs"/> (a joiner keeps trying until the controller is up; the
    /// seed/controller — which resolves no controller to dial — simply keeps failing harmlessly,
    /// never a tight spin); once registered it heartbeats each HeartbeatIntervalMs and drops back to
    /// re-registering on a stale epoch / not-controller / transport failure.
    /// </summary>
    private async Task LifecycleLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            var delayMs = _config.HeartbeatIntervalMs;
            try
            {
                if (BrokerEpoch < 0)
                {
                    delayMs = RegistrationRetryMs;
                    var outcome = await _rpc.RegisterAsync(BuildRegistrationInput(), ct).ConfigureAwait(false);
                    if (outcome.Status == ClusterRpcStatus.None)
                    {
                        Interlocked.Exchange(ref _brokerEpoch, outcome.BrokerEpoch);
                        LogRegistered(_config.BrokerId, outcome.BrokerEpoch);
                    }
                    else
                    {
                        LogRegistrationPending(outcome.Status);
                    }
                }
                else
                {
                    var outcome = await _rpc.HeartbeatAsync(
                        new BrokerHeartbeatInput(
                            BrokerId: _config.BrokerId,
                            BrokerEpoch: BrokerEpoch,
                            // In Raft mode this is the committed index this broker has consumed,
                            // which the controller compares against the index of the broker's own
                            // registration entry — Kafka's rule, and its words: "we will only
                            // unfence a broker when its high watermark has reached its broker
                            // registration record".
                            //
                            // In push mode it stays 0, and not as a stand-in for a number nobody
                            // got round to sending: there is no position to send. Metadata arrives
                            // as controller pushes ordered by controller epoch and per-partition
                            // leader epoch — deliberately not by a per-push version, since a coarse
                            // one would drop disjoint partial pushes. "Behind" is undefined there,
                            // so the controller's check is permissive by construction (#160).
                            //
                            // Do not make push mode send -1 to give the check teeth: a fenced
                            // broker is left out of metadata pushes (#123), so a value that never
                            // satisfies the check would fence every broker permanently.
                            CurrentMetadataOffset: _metadataPosition?.Invoke() ?? 0,
                            WantFence: false,
                            WantShutDown: false),
                        ct).ConfigureAwait(false);

                    if (outcome.Status != ClusterRpcStatus.None)
                    {
                        // Stale epoch / not-controller / transport failure — drop back to re-register so
                        // we re-establish an epoch (and re-resolve the current controller).
                        LogHeartbeatRejected(outcome.Status);
                        Interlocked.Exchange(ref _brokerEpoch, -1);
                        delayMs = RegistrationRetryMs;
                    }
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                LogLoopError(ex);
                Interlocked.Exchange(ref _brokerEpoch, -1); // uncertain state — re-register
                delayMs = RegistrationRetryMs;
            }

            try
            {
                await Task.Delay(delayMs, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
        }
    }

    private BrokerRegistrationInput BuildRegistrationInput()
    {
        var listeners = new List<ListenerSpec>
        {
            new("PLAINTEXT", _config.Host, _config.Port, SecurityProtocol: 0),
        };
        if (_config.ReplicationPort != _config.Port)
            listeners.Add(new ListenerSpec("REPLICATION", _config.Host, _config.ReplicationPort, SecurityProtocol: 0));

        var features = new List<FeatureSpec> { InterBrokerProtocolFeature.LocalFeatureSpec };

        return new BrokerRegistrationInput(
            BrokerId: _config.BrokerId,
            ClusterId: _config.ClusterId ?? "surgewave-cluster",
            IncarnationId: _incarnationId,
            Listeners: listeners,
            Features: features,
            Rack: _config.Rack,
            PreviousBrokerEpoch: -1);
    }

    public async ValueTask DisposeAsync()
    {
        if (_cts is null)
            return;

        await _cts.CancelAsync().ConfigureAwait(false);
        if (_loopTask is not null)
        {
            try { await _loopTask.ConfigureAwait(false); }
            catch (OperationCanceledException) { /* loop ended by disposal */ }
        }
        _cts.Dispose();
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "No cluster nodes configured — broker lifecycle loop idle (standalone)")]
    private partial void LogStandalone();

    [LoggerMessage(Level = LogLevel.Information, Message = "Broker {BrokerId} registered natively, epoch {Epoch}")]
    private partial void LogRegistered(int brokerId, long epoch);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Native registration pending ({Status}) — no controller reachable yet, will retry")]
    private partial void LogRegistrationPending(ClusterRpcStatus status);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Native heartbeat rejected ({Status}) — re-registering")]
    private partial void LogHeartbeatRejected(ClusterRpcStatus status);

    [LoggerMessage(Level = LogLevel.Error, Message = "Native lifecycle loop error")]
    private partial void LogLoopError(Exception ex);
}
