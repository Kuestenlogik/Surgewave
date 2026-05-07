using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net.Sockets;
using Kuestenlogik.Surgewave.Clustering.Raft;
using Kuestenlogik.Surgewave.Protocol.Kafka;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.Cluster;

/// <summary>
/// Client for pushing metadata updates from the controller to all brokers.
/// Maintains connection pooling and handles retries.
/// </summary>
public sealed partial class MetadataUpdateClient : IAsyncDisposable
{
    private const short MetadataUpdateApiKey = 103;
    private const short MetadataUpdateApiVersion = 0;
    private const int ConnectionTimeoutMs = 5000;
    private const int RequestTimeoutMs = 10000;

    private readonly ILogger<MetadataUpdateClient> _logger;
    private readonly ClusterState _clusterState;
    private readonly ClusteringConfig _config;
    private readonly ConcurrentDictionary<int, TcpClient> _connections = new();

    private int _correlationId;

    public MetadataUpdateClient(
        ILogger<MetadataUpdateClient> logger,
        ClusterState clusterState,
        ClusteringConfig config)
    {
        _logger = logger;
        _clusterState = clusterState;
        _config = config;
    }

    /// <summary>
    /// Broadcast a metadata update to all known brokers (except self).
    /// Returns the number of brokers that successfully received the update.
    /// </summary>
    public async Task<int> BroadcastMetadataUpdateAsync(
        MetadataCommandType commandType,
        byte[] commandData,
        CancellationToken ct = default)
    {
        var version = _clusterState.IncrementMetadataVersion();
        var timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        var request = new MetadataUpdateRequest(
            _config.BrokerId,
            _clusterState.ControllerEpoch,
            version,
            commandType,
            commandData,
            timestamp
        );

        var tasks = new List<Task<bool>>();

        foreach (var (brokerId, broker) in _clusterState.Brokers)
        {
            if (brokerId == _config.BrokerId)
                continue;

            tasks.Add(SendMetadataUpdateAsync(brokerId, broker.Host, broker.ReplicationPort, request, ct));
        }

        if (tasks.Count == 0)
            return 0;

        var results = await Task.WhenAll(tasks);
        var successCount = results.Count(r => r);

        LogBroadcastComplete(commandType, version, successCount, tasks.Count);

        return successCount;
    }

    /// <summary>
    /// Send a metadata update to a specific broker.
    /// </summary>
    public async Task<bool> SendMetadataUpdateAsync(
        int brokerId,
        string host,
        int replicationPort,
        MetadataUpdateRequest request,
        CancellationToken ct = default)
    {
        const int maxRetries = 3;
        var retryDelayMs = 100;

        for (var attempt = 1; attempt <= maxRetries; attempt++)
        {
            try
            {
                var client = await GetOrCreateConnectionAsync(brokerId, host, replicationPort, ct);
                if (client == null || !client.Connected)
                {
                    if (attempt < maxRetries)
                    {
                        await Task.Delay(retryDelayMs, ct);
                        retryDelayMs *= 2; // Exponential backoff
                    }
                    continue;
                }

                var correlationId = Interlocked.Increment(ref _correlationId);
                var requestBytes = SerializeMetadataUpdateRequest(request, correlationId);

                await using var stream = client.GetStream();

                using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                cts.CancelAfter(RequestTimeoutMs);

                await stream.WriteAsync(requestBytes, cts.Token);
                await stream.FlushAsync(cts.Token);

                var response = await ReadMetadataUpdateResponseAsync(stream, cts.Token);
                if (response != null)
                {
                    if (response.ErrorCode == (short)ErrorCode.None)
                    {
                        LogUpdateSent(brokerId, request.CommandType, request.MetadataVersion);
                        return true;
                    }

                    LogUpdateRejected(brokerId, (ErrorCode)response.ErrorCode, request.MetadataVersion);
                    return false;
                }

                LogUpdateNoResponse(brokerId, attempt);
            }
            catch (Exception ex)
            {
                LogUpdateError(brokerId, attempt, ex);

                // Remove failed connection
                if (_connections.TryRemove(brokerId, out var oldClient))
                {
                    oldClient.Dispose();
                }

                if (attempt < maxRetries)
                {
                    await Task.Delay(retryDelayMs, ct);
                    retryDelayMs *= 2;
                }
            }
        }

        return false;
    }

    private async Task<TcpClient?> GetOrCreateConnectionAsync(int brokerId, string host, int replicationPort, CancellationToken ct)
    {
        if (_connections.TryGetValue(brokerId, out var existing) && existing.Connected)
            return existing;

        try
        {
            var client = new TcpClient();

            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(ConnectionTimeoutMs);

            await client.ConnectAsync(host, replicationPort, cts.Token);

            _connections[brokerId] = client;
            LogConnectedToBroker(brokerId, host, replicationPort);
            return client;
        }
        catch (Exception ex)
        {
            LogConnectionFailed(brokerId, host, replicationPort, ex);
            return null;
        }
    }

    private byte[] SerializeMetadataUpdateRequest(MetadataUpdateRequest request, int correlationId)
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);

        // Reserve space for size
        writer.Write(0);

        // API Key
        writer.Write(BinaryPrimitives.ReverseEndianness(MetadataUpdateApiKey));

        // API Version
        writer.Write(BinaryPrimitives.ReverseEndianness(MetadataUpdateApiVersion));

        // Correlation ID
        writer.Write(BinaryPrimitives.ReverseEndianness(correlationId));

        // Client ID (null)
        writer.Write(BinaryPrimitives.ReverseEndianness((short)-1));

        // MetadataUpdate fields
        writer.Write(BinaryPrimitives.ReverseEndianness(request.ControllerId));
        writer.Write(BinaryPrimitives.ReverseEndianness(request.ControllerEpoch));
        writer.Write(BinaryPrimitives.ReverseEndianness(request.MetadataVersion));
        writer.Write(BinaryPrimitives.ReverseEndianness((int)request.CommandType));
        writer.Write(BinaryPrimitives.ReverseEndianness(request.Timestamp));
        writer.Write(BinaryPrimitives.ReverseEndianness(request.CommandData.Length));
        writer.Write(request.CommandData);

        var bytes = ms.ToArray();

        // Write size at beginning
        BinaryPrimitives.WriteInt32BigEndian(bytes.AsSpan(0, 4), bytes.Length - 4);

        return bytes;
    }

    private static async Task<MetadataUpdateResponse?> ReadMetadataUpdateResponseAsync(NetworkStream stream, CancellationToken ct)
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

            return ParseMetadataUpdateResponse(body);
        }
        catch
        {
            return null;
        }
    }

    private static MetadataUpdateResponse? ParseMetadataUpdateResponse(byte[] data)
    {
        if (data.Length < 18) // 4 (correlationId) + 4 (brokerId) + 2 (errorCode) + 8 (version)
            return null;

        var offset = 0;

        // Correlation ID (skip)
        offset += 4;

        // Broker ID
        var brokerId = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
        offset += 4;

        // Error Code
        var errorCode = BinaryPrimitives.ReadInt16BigEndian(data.AsSpan(offset, 2));
        offset += 2;

        // Metadata Version
        var version = BinaryPrimitives.ReadInt64BigEndian(data.AsSpan(offset, 8));

        return new MetadataUpdateResponse(brokerId, errorCode, version);
    }

    public async ValueTask DisposeAsync()
    {
        foreach (var (_, client) in _connections)
        {
            client.Dispose();
        }
        _connections.Clear();
        await Task.CompletedTask;
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "Connected to broker {BrokerId} at {Host}:{Port} for metadata updates")]
    private partial void LogConnectedToBroker(int brokerId, string host, int port);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Failed to connect to broker {BrokerId} at {Host}:{Port} for metadata updates")]
    private partial void LogConnectionFailed(int brokerId, string host, int port, Exception ex);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Sent metadata update to broker {BrokerId}: {CommandType} (version={Version})")]
    private partial void LogUpdateSent(int brokerId, MetadataCommandType commandType, long version);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Broker {BrokerId} rejected metadata update: {ErrorCode} (version={Version})")]
    private partial void LogUpdateRejected(int brokerId, ErrorCode errorCode, long version);

    [LoggerMessage(Level = LogLevel.Warning, Message = "No response from broker {BrokerId} (attempt {Attempt})")]
    private partial void LogUpdateNoResponse(int brokerId, int attempt);

    [LoggerMessage(Level = LogLevel.Warning, Message = "Error sending metadata update to broker {BrokerId} (attempt {Attempt})")]
    private partial void LogUpdateError(int brokerId, int attempt, Exception ex);

    [LoggerMessage(Level = LogLevel.Information, Message = "Broadcast metadata update: {CommandType} (version={Version}, success={SuccessCount}/{TotalCount})")]
    private partial void LogBroadcastComplete(MetadataCommandType commandType, long version, int successCount, int totalCount);
}
