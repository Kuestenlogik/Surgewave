using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net.Sockets;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.Cluster;

/// <summary>
/// Event arguments for broker health change events.
/// </summary>
public sealed class BrokerHealthEventArgs(int brokerId) : EventArgs
{
    /// <summary>
    /// The broker ID that changed state.
    /// </summary>
    public int BrokerId { get; } = brokerId;
}

/// <summary>
/// Async event handler delegate for broker health events.
/// </summary>
public delegate Task AsyncEventHandler<TEventArgs>(object sender, TEventArgs e) where TEventArgs : EventArgs;

/// <summary>
/// Manages broker heartbeating for failure detection.
/// Sends periodic heartbeats to all known brokers and tracks their health state.
/// </summary>
public sealed partial class HeartbeatManager : IAsyncDisposable
{
    private const short HeartbeatApiKey = 100; // Custom API key for heartbeat
    private const short HeartbeatApiVersion = 0;

    private readonly ILogger<HeartbeatManager> _logger;
    private readonly ClusterState _clusterState;
    private readonly ClusteringConfig _config;
    private readonly ConcurrentDictionary<int, BrokerHealthState> _brokerHealth = new();
    private readonly ConcurrentDictionary<int, TcpClient> _connections = new();

    private CancellationTokenSource? _cts;
    private Task? _heartbeatTask;
    private Task? _monitorTask;
    private int _correlationId;

    /// <summary>
    /// Event raised when a broker is detected as failed.
    /// </summary>
    public event AsyncEventHandler<BrokerHealthEventArgs>? OnBrokerFailed;

    /// <summary>
    /// Event raised when a failed broker comes back online.
    /// </summary>
    public event AsyncEventHandler<BrokerHealthEventArgs>? OnBrokerRecovered;

    public HeartbeatManager(
        ILogger<HeartbeatManager> logger,
        ClusterState clusterState,
        ClusteringConfig config)
    {
        _logger = logger;
        _clusterState = clusterState;
        _config = config;
    }

    /// <summary>
    /// Get the health state of a specific broker.
    /// </summary>
    public BrokerHealthState? GetBrokerHealth(int brokerId) =>
        _brokerHealth.TryGetValue(brokerId, out var state) ? state : null;

    /// <summary>
    /// Get all broker health states.
    /// </summary>
    public IReadOnlyDictionary<int, BrokerHealthState> AllBrokerHealth => _brokerHealth;

    /// <summary>
    /// Check if a broker is currently alive.
    /// </summary>
    public bool IsBrokerAlive(int brokerId) =>
        _brokerHealth.TryGetValue(brokerId, out var state) && state.IsAlive;

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        // Initialize health state for all known brokers
        foreach (var (brokerId, _) in _clusterState.Brokers)
        {
            if (brokerId != _config.BrokerId)
            {
                _brokerHealth[brokerId] = new BrokerHealthState { BrokerId = brokerId };
            }
        }

        // Start heartbeat sender loop
        _heartbeatTask = Task.Run(() => HeartbeatLoopAsync(_cts.Token), _cts.Token);

        // Start health monitor loop
        _monitorTask = Task.Run(() => MonitorLoopAsync(_cts.Token), _cts.Token);

        LogHeartbeatManagerStarted(_config.HeartbeatIntervalMs, _config.HeartbeatTimeoutMs);
        return Task.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        _cts?.Cancel();

        if (_heartbeatTask != null)
        {
            try { await _heartbeatTask; } catch (OperationCanceledException) { }
        }

        if (_monitorTask != null)
        {
            try { await _monitorTask; } catch (OperationCanceledException) { }
        }

        // Close all connections
        foreach (var (_, client) in _connections)
        {
            client.Dispose();
        }
        _connections.Clear();

        _cts?.Dispose();
    }

    /// <summary>
    /// Process an incoming heartbeat request and return a response.
    /// </summary>
    public HeartbeatResponse ProcessHeartbeat(HeartbeatRequest request)
    {
        var brokerId = request.BrokerId;

        // Update or create health state for the sending broker
        var healthState = _brokerHealth.GetOrAdd(brokerId, id => new BrokerHealthState { BrokerId = id });

        var wasAlive = healthState.IsAlive;
        healthState.RecordHeartbeat(request.BrokerEpoch);

        if (!wasAlive)
        {
            LogBrokerRecovered(brokerId);
            _ = Task.Run(async () =>
            {
                if (OnBrokerRecovered != null)
                    await OnBrokerRecovered(this, new BrokerHealthEventArgs(brokerId));
            });
        }

        return new HeartbeatResponse(
            _config.BrokerId,
            0, // Our broker epoch
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            _clusterState.ControllerId == _config.BrokerId
        );
    }

    private async Task HeartbeatLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await SendHeartbeatsAsync(ct);
                await Task.Delay(_config.HeartbeatIntervalMs, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                LogHeartbeatLoopError(ex);
            }
        }
    }

    private async Task MonitorLoopAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(1000, ct); // Check every second
                await CheckBrokerHealthAsync(ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                LogMonitorLoopError(ex);
            }
        }
    }

    private async Task SendHeartbeatsAsync(CancellationToken ct)
    {
        var tasks = new List<Task>();

        foreach (var (brokerId, broker) in _clusterState.Brokers)
        {
            if (brokerId == _config.BrokerId)
                continue;

            tasks.Add(SendHeartbeatToBrokerAsync(brokerId, broker, ct));
        }

        await Task.WhenAll(tasks);
    }

    private async Task SendHeartbeatToBrokerAsync(int brokerId, BrokerNode broker, CancellationToken ct)
    {
        try
        {
            var client = await GetOrCreateConnectionAsync(brokerId, broker.Host, broker.ReplicationPort, ct);
            if (client == null || !client.Connected)
            {
                RecordHeartbeatFailure(brokerId);
                return;
            }

            var request = new HeartbeatRequest(
                _config.BrokerId,
                0, // Our broker epoch
                DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                _clusterState.ControllerId,
                _clusterState.ControllerEpoch
            );

            var correlationId = Interlocked.Increment(ref _correlationId);
            var requestBytes = SerializeHeartbeatRequest(request, correlationId);

            await using var stream = client.GetStream();

            // Set timeout for the operation
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(_config.HeartbeatTimeoutMs);

            await stream.WriteAsync(requestBytes, cts.Token);
            await stream.FlushAsync(cts.Token);

            var response = await ReadHeartbeatResponseAsync(stream, cts.Token);
            if (response != null)
            {
                var healthState = _brokerHealth.GetOrAdd(brokerId, id => new BrokerHealthState { BrokerId = id });
                var wasAlive = healthState.IsAlive;
                healthState.RecordHeartbeat(response.BrokerEpoch);

                if (!wasAlive)
                {
                    LogBrokerRecovered(brokerId);
                    if (OnBrokerRecovered != null)
                        await OnBrokerRecovered(this, new BrokerHealthEventArgs(brokerId));
                }
            }
            else
            {
                RecordHeartbeatFailure(brokerId);
            }
        }
        catch (Exception ex)
        {
            LogHeartbeatSendError(brokerId, ex);
            RecordHeartbeatFailure(brokerId);

            // Remove failed connection
            if (_connections.TryRemove(brokerId, out var oldClient))
            {
                oldClient.Dispose();
            }
        }
    }

    private async Task<TcpClient?> GetOrCreateConnectionAsync(int brokerId, string host, int replicationPort, CancellationToken ct)
    {
        if (_connections.TryGetValue(brokerId, out var existing) && existing.Connected)
            return existing;

        const int maxRetries = 3;
        var delayMs = RetryHelper.DefaultInitialDelayMs;

        for (var attempt = 1; attempt <= maxRetries; attempt++)
        {
            try
            {
                var client = new TcpClient();

                using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                cts.CancelAfter(5000); // 5 second connection timeout

                await client.ConnectAsync(host, replicationPort, cts.Token);

                _connections[brokerId] = client;
                LogConnectedToBroker(brokerId, host, replicationPort);
                return client;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return null;
            }
            catch (Exception ex)
            {
                if (attempt < maxRetries)
                {
                    LogConnectionRetry(brokerId, host, replicationPort, attempt);
                    await Task.Delay(delayMs, ct);
                    delayMs *= 2;
                }
                else
                {
                    LogConnectionFailed(brokerId, host, replicationPort, ex);
                }
            }
        }

        return null;
    }

    private void RecordHeartbeatFailure(int brokerId)
    {
        if (_brokerHealth.TryGetValue(brokerId, out var state))
        {
            state.RecordFailure();
        }
    }

    private async Task CheckBrokerHealthAsync(CancellationToken ct)
    {
        foreach (var (brokerId, state) in _brokerHealth)
        {
            if (state.ShouldMarkFailed(_config.HeartbeatTimeoutMs) ||
                state.ConsecutiveFailures >= _config.MaxHeartbeatFailures)
            {
                if (state.IsAlive)
                {
                    state.MarkFailed();
                    LogBrokerFailed(brokerId, state.TimeSinceLastHeartbeatMs, state.ConsecutiveFailures);

                    if (OnBrokerFailed != null)
                    {
                        await OnBrokerFailed(this, new BrokerHealthEventArgs(brokerId));
                    }
                }
            }
        }
    }

    private byte[] SerializeHeartbeatRequest(HeartbeatRequest request, int correlationId)
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);

        // Reserve space for size
        writer.Write(0);

        // API Key
        writer.Write(BinaryPrimitives.ReverseEndianness(HeartbeatApiKey));

        // API Version
        writer.Write(BinaryPrimitives.ReverseEndianness(HeartbeatApiVersion));

        // Correlation ID
        writer.Write(BinaryPrimitives.ReverseEndianness(correlationId));

        // Client ID (null)
        writer.Write(BinaryPrimitives.ReverseEndianness((short)-1));

        // Heartbeat fields
        writer.Write(BinaryPrimitives.ReverseEndianness(request.BrokerId));
        writer.Write(BinaryPrimitives.ReverseEndianness(request.BrokerEpoch));
        writer.Write(BinaryPrimitives.ReverseEndianness(request.Timestamp));
        writer.Write(BinaryPrimitives.ReverseEndianness(request.ControllerId));
        writer.Write(BinaryPrimitives.ReverseEndianness(request.ControllerEpoch));

        var bytes = ms.ToArray();

        // Write size at beginning
        BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(0, 4), bytes.Length - 4);

        return bytes;
    }

    private async Task<HeartbeatResponse?> ReadHeartbeatResponseAsync(NetworkStream stream, CancellationToken ct)
    {
        try
        {
            // Read size
            var sizeBuffer = new byte[4];
            var read = await stream.ReadAsync(sizeBuffer, ct);
            if (read == 0)
                return null;

            var size = BinaryPrimitives.ReadInt32BigEndian(sizeBuffer);
            if (size <= 0 || size > 1024)
                return null;

            // Read body
            var body = new byte[size];
            await stream.ReadExactlyAsync(body, ct);

            return ParseHeartbeatResponse(body);
        }
        catch
        {
            return null;
        }
    }

    private static HeartbeatResponse? ParseHeartbeatResponse(byte[] data)
    {
        if (data.Length < 20)
            return null;

        var offset = 0;

        // Correlation ID (skip)
        offset += 4;

        // Broker ID
        var brokerId = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
        offset += 4;

        // Broker Epoch
        var brokerEpoch = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
        offset += 4;

        // Timestamp
        var timestamp = BinaryPrimitives.ReadInt64BigEndian(data.AsSpan(offset, 8));
        offset += 8;

        // Is Controller
        var isController = data[offset] != 0;

        return new HeartbeatResponse(brokerId, brokerEpoch, timestamp, isController);
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Heartbeat manager started (interval={IntervalMs}ms, timeout={TimeoutMs}ms)")]
    private partial void LogHeartbeatManagerStarted(int intervalMs, int timeoutMs);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Connected to broker {BrokerId} at {Host}:{Port}")]
    private partial void LogConnectedToBroker(int brokerId, string host, int port);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to connect to broker {BrokerId} at {Host}:{Port}")]
    private partial void LogConnectionFailed(int brokerId, string host, int port, Exception ex);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Connection to broker {BrokerId} at {Host}:{Port} failed on attempt {Attempt}, retrying")]
    private partial void LogConnectionRetry(int brokerId, string host, int port, int attempt);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Broker {BrokerId} marked as FAILED (no heartbeat for {Ms}ms, failures={Failures})")]
    private partial void LogBrokerFailed(int brokerId, long ms, int failures);

    [LoggerMessage(Level = LogLevel.Information, Message = "Broker {BrokerId} recovered")]
    private partial void LogBrokerRecovered(int brokerId);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Error sending heartbeat to broker {BrokerId}")]
    private partial void LogHeartbeatSendError(int brokerId, Exception ex);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error in heartbeat loop")]
    private partial void LogHeartbeatLoopError(Exception ex);

    [LoggerMessage(Level = LogLevel.Error, Message = "Error in monitor loop")]
    private partial void LogMonitorLoopError(Exception ex);
}
