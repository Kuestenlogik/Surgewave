using System.Buffers.Binary;
using System.Net.Sockets;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Kuestenlogik.Surgewave.Protocol.Kafka.Requests;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.Cluster;

/// <summary>
/// Broker lifecycle states for Raft cluster membership.
/// </summary>
public enum BrokerLifecycleState
{
    /// <summary>Broker has not started registration.</summary>
    NotStarted,

    /// <summary>Broker is attempting to register with the controller.</summary>
    Registering,

    /// <summary>Broker is registered but fenced (not yet caught up with metadata).</summary>
    Fenced,

    /// <summary>Broker is active and serving requests.</summary>
    Active,

    /// <summary>Broker is in controlled shutdown process.</summary>
    ShuttingDown,

    /// <summary>Broker has been shut down.</summary>
    Shutdown
}

/// <summary>
/// Event arguments for broker lifecycle state changes.
/// </summary>
public sealed class BrokerLifecycleEventArgs(BrokerLifecycleState oldState, BrokerLifecycleState newState) : EventArgs
{
    public BrokerLifecycleState OldState { get; } = oldState;
    public BrokerLifecycleState NewState { get; } = newState;
}

/// <summary>
/// Manages the broker lifecycle for Raft cluster membership.
/// Handles registration with the controller and periodic heartbeating.
/// </summary>
/// <remarks>
/// This is the client-side component that runs on each broker.
/// It sends BrokerRegistration and BrokerHeartbeat requests to the controller.
/// </remarks>
public sealed partial class BrokerLifecycleManager : IAsyncDisposable
{
    private readonly ClusteringConfig _config;
    private readonly ClusterState _clusterState;
    private readonly ILogger<BrokerLifecycleManager> _logger;

    private readonly Guid _incarnationId = Guid.NewGuid();
    private long _brokerEpoch = -1;
    private long _currentMetadataOffset = -1;
    private BrokerLifecycleState _state = BrokerLifecycleState.NotStarted;

    private TcpClient? _controllerConnection;
    private CancellationTokenSource? _cts;
    private Task? _heartbeatTask;
    private int _correlationId;

    private readonly object _stateLock = new();

    /// <summary>
    /// Event raised when the broker lifecycle state changes.
    /// </summary>
    public event EventHandler<BrokerLifecycleEventArgs>? OnStateChanged;

    /// <summary>
    /// Gets the current broker lifecycle state.
    /// </summary>
    public BrokerLifecycleState State
    {
        get { lock (_stateLock) return _state; }
    }

    /// <summary>
    /// Gets the unique incarnation ID for this broker instance.
    /// A new ID is generated each time the broker starts.
    /// </summary>
    public Guid IncarnationId => _incarnationId;

    /// <summary>
    /// Gets the broker epoch assigned by the controller.
    /// Returns -1 if not yet registered.
    /// </summary>
    public long BrokerEpoch => Interlocked.Read(ref _brokerEpoch);

    /// <summary>
    /// Gets whether the broker is currently fenced.
    /// A fenced broker cannot be a partition leader.
    /// </summary>
    public bool IsFenced => State is BrokerLifecycleState.Fenced or BrokerLifecycleState.Registering or BrokerLifecycleState.NotStarted;

    /// <summary>
    /// Gets whether the broker is ready to serve requests.
    /// </summary>
    public bool IsReady => State == BrokerLifecycleState.Active;

    public BrokerLifecycleManager(
        ClusteringConfig config,
        ClusterState clusterState,
        ILogger<BrokerLifecycleManager> logger)
    {
        _config = config;
        _clusterState = clusterState;
        _logger = logger;
    }

    /// <summary>
    /// Start the broker lifecycle manager.
    /// This initiates registration with the controller and starts heartbeating.
    /// </summary>
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (string.IsNullOrEmpty(_config.ClusterNodes))
        {
            LogNoControllerConfigured();
            return;
        }

        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        LogLifecycleManagerStarting(_config.BrokerId, _incarnationId);

        // Start registration and heartbeat loop
        _heartbeatTask = Task.Run(() => LifecycleLoopAsync(_cts.Token), _cts.Token);
    }

    /// <summary>
    /// Initiate controlled shutdown.
    /// The broker will signal the controller and wait for partition migrations.
    /// </summary>
    public async Task InitiateShutdownAsync(CancellationToken cancellationToken)
    {
        SetState(BrokerLifecycleState.ShuttingDown);

        // Send final heartbeat with WantShutDown=true
        try
        {
            await SendHeartbeatAsync(wantShutDown: true, cancellationToken);
        }
        catch (Exception ex)
        {
            LogShutdownHeartbeatFailed(ex);
        }
    }

    /// <summary>
    /// Update the current metadata offset.
    /// Called by the metadata log consumer as it processes records.
    /// </summary>
    public void UpdateMetadataOffset(long offset)
    {
        Interlocked.Exchange(ref _currentMetadataOffset, offset);
    }

    public async ValueTask DisposeAsync()
    {
        _cts?.Cancel();

        if (_heartbeatTask != null)
        {
            try { await _heartbeatTask; }
            catch (OperationCanceledException) { }
        }

        _controllerConnection?.Dispose();
        _cts?.Dispose();

        SetState(BrokerLifecycleState.Shutdown);
    }

    private async Task LifecycleLoopAsync(CancellationToken ct)
    {
        // Initial registration with retry
        await RegisterWithControllerAsync(ct);

        // Heartbeat loop
        while (!ct.IsCancellationRequested && State != BrokerLifecycleState.Shutdown)
        {
            try
            {
                var wantShutDown = State == BrokerLifecycleState.ShuttingDown;
                await SendHeartbeatAsync(wantShutDown, ct);
                await Task.Delay(_config.HeartbeatIntervalMs, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                LogHeartbeatError(ex);

                // Re-register if we lost connection
                if (State == BrokerLifecycleState.Active || State == BrokerLifecycleState.Fenced)
                {
                    SetState(BrokerLifecycleState.Registering);
                    await RegisterWithControllerAsync(ct);
                }
            }
        }
    }

    private async Task RegisterWithControllerAsync(CancellationToken ct)
    {
        SetState(BrokerLifecycleState.Registering);

        var retryCount = 0;
        var maxRetries = 10;
        var baseDelayMs = 1000;

        while (!ct.IsCancellationRequested && retryCount < maxRetries)
        {
            try
            {
                // Connect to controller
                if (!await EnsureControllerConnectionAsync(ct))
                {
                    retryCount++;
                    await Task.Delay(baseDelayMs * (1 << Math.Min(retryCount, 5)), ct);
                    continue;
                }

                // Send registration request
                var request = CreateRegistrationRequest();
                var response = await SendRegistrationRequestAsync(request, ct);

                if (response.ErrorCode == ErrorCode.None)
                {
                    Interlocked.Exchange(ref _brokerEpoch, response.BrokerEpoch);
                    SetState(BrokerLifecycleState.Fenced);
                    LogRegistrationSuccessful(_config.BrokerId, response.BrokerEpoch);
                    return;
                }

                LogRegistrationFailed(response.ErrorCode);
                retryCount++;
                await Task.Delay(baseDelayMs * (1 << Math.Min(retryCount, 5)), ct);
            }
            catch (Exception ex)
            {
                LogRegistrationError(ex);
                DisconnectFromController();
                retryCount++;
                await Task.Delay(baseDelayMs * (1 << Math.Min(retryCount, 5)), ct);
            }
        }

        if (retryCount >= maxRetries)
        {
            LogRegistrationExhausted(maxRetries);
        }
    }

    private async Task SendHeartbeatAsync(bool wantShutDown, CancellationToken ct)
    {
        if (!await EnsureControllerConnectionAsync(ct))
        {
            throw new InvalidOperationException("No connection to controller");
        }

        var request = new BrokerHeartbeatRequest
        {
            ApiKey = ApiKey.BrokerHeartbeat,
            ApiVersion = 1,
            CorrelationId = Interlocked.Increment(ref _correlationId),
            ClientId = $"surgewave-broker-{_config.BrokerId}",
            BrokerId = _config.BrokerId,
            BrokerEpoch = BrokerEpoch,
            CurrentMetadataOffset = Interlocked.Read(ref _currentMetadataOffset),
            WantFence = false,
            WantShutDown = wantShutDown
        };

        var response = await SendHeartbeatRequestAsync(request, ct);

        if (response.ErrorCode == ErrorCode.StaleBrokerEpoch)
        {
            // Our epoch is stale, need to re-register
            LogStaleEpoch(BrokerEpoch);
            SetState(BrokerLifecycleState.Registering);
            throw new InvalidOperationException("Stale broker epoch, re-registration required");
        }

        if (response.ErrorCode != ErrorCode.None)
        {
            LogHeartbeatFailed(response.ErrorCode);
            return;
        }

        // Update state based on response
        if (response.ShouldShutDown && State == BrokerLifecycleState.ShuttingDown)
        {
            LogShutdownApproved();
            SetState(BrokerLifecycleState.Shutdown);
        }
        else if (!response.IsFenced && State == BrokerLifecycleState.Fenced)
        {
            LogBrokerUnfenced(_config.BrokerId);
            SetState(BrokerLifecycleState.Active);
        }
        else if (response.IsFenced && State == BrokerLifecycleState.Active)
        {
            LogBrokerFenced(_config.BrokerId);
            SetState(BrokerLifecycleState.Fenced);
        }

        LogHeartbeatSent(response.IsCaughtUp, response.IsFenced);
    }

    private BrokerRegistrationRequest CreateRegistrationRequest()
    {
        var listeners = new List<BrokerRegistrationRequest.Listener>
        {
            new()
            {
                Name = "PLAINTEXT",
                Host = _config.Host,
                Port = (ushort)_config.Port,
                SecurityProtocol = 0 // PLAINTEXT
            }
        };

        // Add replication listener if different
        if (_config.ReplicationPort != _config.Port)
        {
            listeners.Add(new BrokerRegistrationRequest.Listener
            {
                Name = "REPLICATION",
                Host = _config.Host,
                Port = (ushort)_config.ReplicationPort,
                SecurityProtocol = 0
            });
        }

        var features = new List<BrokerRegistrationRequest.Feature>
        {
            new() { Name = "metadata.version", MinSupportedVersion = 1, MaxSupportedVersion = 20 }
        };

        return new BrokerRegistrationRequest
        {
            ApiKey = ApiKey.BrokerRegistration,
            ApiVersion = 3,
            CorrelationId = Interlocked.Increment(ref _correlationId),
            ClientId = $"surgewave-broker-{_config.BrokerId}",
            BrokerId = _config.BrokerId,
            ClusterId = _config.ClusterId ?? "surgewave-cluster",
            IncarnationId = _incarnationId,
            Listeners = listeners,
            Features = features,
            Rack = _config.Rack,
            IsMigratingZkBroker = false,
            LogDirs = [Guid.NewGuid()], // Single log directory
            PreviousBrokerEpoch = -1
        };
    }

    private async Task<bool> EnsureControllerConnectionAsync(CancellationToken ct)
    {
        if (_controllerConnection?.Connected == true)
            return true;

        DisconnectFromController();

        // Parse controller address from ClusterNodes
        var controllerEndpoint = GetControllerEndpoint();
        if (controllerEndpoint == null)
        {
            LogNoControllerFound();
            return false;
        }

        try
        {
            _controllerConnection = new TcpClient();
            using var connectCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            connectCts.CancelAfter(5000);

            await _controllerConnection.ConnectAsync(controllerEndpoint.Value.host, controllerEndpoint.Value.port, connectCts.Token);
            LogConnectedToController(controllerEndpoint.Value.host, controllerEndpoint.Value.port);
            return true;
        }
        catch (Exception ex)
        {
            LogControllerConnectionFailed(controllerEndpoint.Value.host, controllerEndpoint.Value.port, ex);
            DisconnectFromController();
            return false;
        }
    }

    private (string host, int port)? GetControllerEndpoint()
    {
        // First try to use known controller from cluster state
        if (_clusterState.ControllerId >= 0)
        {
            var controller = _clusterState.Brokers.Values.FirstOrDefault(b => b.BrokerId == _clusterState.ControllerId);
            if (controller != null)
            {
                return (controller.Host, controller.Port);
            }
        }

        // Fall back to first configured cluster node
        if (string.IsNullOrEmpty(_config.ClusterNodes))
            return null;

        var parts = _config.ClusterNodes.Split(',')[0].Trim().Split(':');
        if (parts.Length >= 2 && int.TryParse(parts[1], out var port))
        {
            return (parts[0], port);
        }

        if (parts.Length == 1)
        {
            return (parts[0], 9092);
        }

        return null;
    }

    private void DisconnectFromController()
    {
        _controllerConnection?.Dispose();
        _controllerConnection = null;
    }

    private async Task<BrokerRegistrationResponse> SendRegistrationRequestAsync(BrokerRegistrationRequest request, CancellationToken ct)
    {
        var stream = _controllerConnection!.GetStream();

        // Serialize request
        var requestBytes = SerializeRequest(request);

        // Send
        await stream.WriteAsync(requestBytes, ct);
        await stream.FlushAsync(ct);

        // Read response
        return await ReadRegistrationResponseAsync(stream, request.ApiVersion, request.CorrelationId, ct);
    }

    private async Task<BrokerHeartbeatResponse> SendHeartbeatRequestAsync(BrokerHeartbeatRequest request, CancellationToken ct)
    {
        var stream = _controllerConnection!.GetStream();

        // Serialize request
        var requestBytes = SerializeRequest(request);

        // Send
        await stream.WriteAsync(requestBytes, ct);
        await stream.FlushAsync(ct);

        // Read response
        return await ReadHeartbeatResponseAsync(stream, request.ApiVersion, request.CorrelationId, ct);
    }

    private static byte[] SerializeRequest(KafkaRequest request)
    {
        var bodyBytes = request.Serialize();

        // Prepend size
        var result = new byte[4 + bodyBytes.Length];
        BinaryPrimitives.WriteInt32BigEndian(result.AsSpan(0, 4), bodyBytes.Length);
        bodyBytes.CopyTo(result, 4);

        return result;
    }

    private static async Task<BrokerRegistrationResponse> ReadRegistrationResponseAsync(
        NetworkStream stream, short apiVersion, int correlationId, CancellationToken ct)
    {
        // Read size
        var sizeBuffer = new byte[4];
        await stream.ReadExactlyAsync(sizeBuffer, ct);
        var size = BinaryPrimitives.ReadInt32BigEndian(sizeBuffer);

        if (size <= 0 || size > 1024 * 1024)
            throw new InvalidOperationException($"Invalid response size: {size}");

        // Read body
        var body = new byte[size];
        await stream.ReadExactlyAsync(body, ct);

        var reader = new KafkaProtocolReader(body);
        return BrokerRegistrationResponse.ReadFrom(reader, apiVersion, correlationId);
    }

    private static async Task<BrokerHeartbeatResponse> ReadHeartbeatResponseAsync(
        NetworkStream stream, short apiVersion, int correlationId, CancellationToken ct)
    {
        // Read size
        var sizeBuffer = new byte[4];
        await stream.ReadExactlyAsync(sizeBuffer, ct);
        var size = BinaryPrimitives.ReadInt32BigEndian(sizeBuffer);

        if (size <= 0 || size > 1024 * 1024)
            throw new InvalidOperationException($"Invalid response size: {size}");

        // Read body
        var body = new byte[size];
        await stream.ReadExactlyAsync(body, ct);

        var reader = new KafkaProtocolReader(body);
        return BrokerHeartbeatResponse.ReadFrom(reader, apiVersion, correlationId);
    }

    private void SetState(BrokerLifecycleState newState)
    {
        BrokerLifecycleState oldState;
        lock (_stateLock)
        {
            oldState = _state;
            if (oldState == newState) return;
            _state = newState;
        }

        LogStateChanged(oldState, newState);
        OnStateChanged?.Invoke(this, new BrokerLifecycleEventArgs(oldState, newState));
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Broker lifecycle manager starting: BrokerId={BrokerId}, IncarnationId={IncarnationId}")]
    private partial void LogLifecycleManagerStarting(int brokerId, Guid incarnationId);

    [LoggerMessage(Level = LogLevel.Warning, Message = "No controller configured in ClusterNodes, running in standalone mode")]
    private partial void LogNoControllerConfigured();

    [LoggerMessage(Level = LogLevel.Information, Message = "Successfully registered with controller: BrokerId={BrokerId}, BrokerEpoch={BrokerEpoch}")]
    private partial void LogRegistrationSuccessful(int brokerId, long brokerEpoch);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Registration failed with error: {ErrorCode}")]
    private partial void LogRegistrationFailed(ErrorCode errorCode);

    [LoggerMessage(Level = LogLevel.Error, Message = "Registration error")]
    private partial void LogRegistrationError(Exception ex);

    [LoggerMessage(Level = LogLevel.Error, Message = "Registration exhausted after {MaxRetries} retries")]
    private partial void LogRegistrationExhausted(int maxRetries);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Heartbeat sent: IsCaughtUp={IsCaughtUp}, IsFenced={IsFenced}")]
    private partial void LogHeartbeatSent(bool isCaughtUp, bool isFenced);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Heartbeat failed with error: {ErrorCode}")]
    private partial void LogHeartbeatFailed(ErrorCode errorCode);

    [LoggerMessage(Level = LogLevel.Error, Message = "Heartbeat error")]
    private partial void LogHeartbeatError(Exception ex);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Stale broker epoch {BrokerEpoch}, need to re-register")]
    private partial void LogStaleEpoch(long brokerEpoch);

    [LoggerMessage(Level = LogLevel.Information, Message = "Broker {BrokerId} unfenced - now active")]
    private partial void LogBrokerUnfenced(int brokerId);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Broker {BrokerId} fenced by controller")]
    private partial void LogBrokerFenced(int brokerId);

    [LoggerMessage(Level = LogLevel.Information, Message = "Shutdown approved by controller")]
    private partial void LogShutdownApproved();

    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to send shutdown heartbeat")]
    private partial void LogShutdownHeartbeatFailed(Exception ex);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Connected to controller at {Host}:{Port}")]
    private partial void LogConnectedToController(string host, int port);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to connect to controller at {Host}:{Port}")]
    private partial void LogControllerConnectionFailed(string host, int port, Exception ex);

    [LoggerMessage(Level = LogLevel.Warning, Message = "No controller found in cluster state")]
    private partial void LogNoControllerFound();

    [LoggerMessage(Level = LogLevel.Information, Message = "Broker lifecycle state changed: {OldState} -> {NewState}")]
    private partial void LogStateChanged(BrokerLifecycleState oldState, BrokerLifecycleState newState);
}
