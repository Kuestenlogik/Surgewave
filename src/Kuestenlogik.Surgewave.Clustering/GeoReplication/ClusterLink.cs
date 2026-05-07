using System.Collections.Concurrent;
using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Transport;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.GeoReplication;

/// <summary>
/// Represents an active connection to a remote cluster for geo-replication.
/// Uses the Surgewave binary protocol to fetch data from the remote broker,
/// riding on top of <see cref="IPeerTransport"/> so the same TCP/QUIC
/// transport selection applies to cross-cluster links as to inter-broker.
/// </summary>
public sealed class ClusterLink : IAsyncDisposable
{
    private readonly ClusterLinkConfig _config;
    private readonly IPeerTransport _peerTransport;
    private readonly ILogger _logger;
    private IPeerConnection? _connection;
    private int _correlationId;
    private bool _disposed;

    public string LinkId => _config.LinkId;
    public ClusterLinkConfig Config => _config;
    public ClusterLinkState State { get; private set; } = ClusterLinkState.Initializing;
    public string? ErrorMessage { get; private set; }
    public DateTimeOffset? LastFetchTimestamp { get; internal set; }
    public bool IsConnected => _connection?.IsConnected == true;

    public ClusterLink(ClusterLinkConfig config, IPeerTransport peerTransport, ILogger logger)
    {
        _config = config;
        _peerTransport = peerTransport;
        _logger = logger;
    }

    public async Task ConnectAsync(CancellationToken ct = default)
    {
        const int maxRetries = 3;
        var delayMs = 500;

        for (var attempt = 1; attempt <= maxRetries; attempt++)
        {
            try
            {
                var (host, port) = ParseBootstrapServer(_config.RemoteBootstrapServers);
                _connection = await _peerTransport.ConnectAsync(host, port, ct);
                State = ClusterLinkState.Active;
                ErrorMessage = null;
                _logger.LogInformation("Cluster link {LinkId} connected to {Remote}", LinkId, _config.RemoteBootstrapServers);
                return;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                if (attempt < maxRetries)
                {
                    _logger.LogWarning(ex, "Cluster link {LinkId} connection attempt {Attempt} failed, retrying in {DelayMs}ms",
                        LinkId, attempt, delayMs);
                    await Task.Delay(delayMs, ct);
                    delayMs *= 2;
                }
                else
                {
                    State = ClusterLinkState.Error;
                    ErrorMessage = ex.Message;
                    _logger.LogError(ex, "Cluster link {LinkId} failed to connect after {MaxRetries} attempts", LinkId, maxRetries);
                    throw;
                }
            }
        }
    }

    public async Task DisconnectAsync()
    {
        if (_connection is not null)
        {
            await _connection.DisposeAsync();
            _connection = null;
        }
        State = ClusterLinkState.Paused;
    }

    /// <summary>
    /// Fetch data from a remote topic partition starting at the given offset.
    /// Returns the raw record batch bytes.
    /// </summary>
    public async Task<RemoteFetchResult> FetchAsync(string topic, int partition, long offset, int maxBytes, CancellationToken ct)
    {
        if (_connection == null || !IsConnected)
            throw new InvalidOperationException($"Cluster link {LinkId} is not connected");

        await using var lease = await _connection.AcquireStreamAsync(ct);
        var correlationId = Interlocked.Increment(ref _correlationId);
        var requestBytes = SerializeFetchRequest(topic, partition, offset, maxBytes, correlationId);

        await lease.Stream.WriteAsync(requestBytes, ct);
        await lease.Stream.FlushAsync(ct);

        var responseBytes = await ReadResponseAsync(lease.Stream, ct);
        return ParseFetchResponse(responseBytes);
    }

    public async Task<List<RemoteTopicInfo>> GetRemoteTopicsAsync(CancellationToken ct)
    {
        if (_connection == null || !IsConnected)
            throw new InvalidOperationException($"Cluster link {LinkId} is not connected");

        await using var lease = await _connection.AcquireStreamAsync(ct);
        var correlationId = Interlocked.Increment(ref _correlationId);
        var requestBytes = SerializeMetadataRequest(correlationId);

        await lease.Stream.WriteAsync(requestBytes, ct);
        await lease.Stream.FlushAsync(ct);

        var responseBytes = await ReadResponseAsync(lease.Stream, ct);
        return ParseMetadataResponse(responseBytes);
    }

    public void SetState(ClusterLinkState state, string? errorMessage = null)
    {
        State = state;
        ErrorMessage = errorMessage;
    }

    private static (string host, int port) ParseBootstrapServer(string servers)
    {
        var first = servers.Split(',')[0].Trim();
        var parts = first.Split(':');
        return (parts[0], parts.Length > 1 ? int.Parse(parts[1]) : 9092);
    }

    private byte[] SerializeFetchRequest(string topic, int partition, long offset, int maxBytes, int correlationId)
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);

        // Size placeholder
        writer.Write(0);
        // API Key (Fetch = 1)
        writer.Write(BinaryPrimitives.ReverseEndianness((short)1));
        // API Version
        writer.Write(BinaryPrimitives.ReverseEndianness((short)11));
        // Correlation ID
        writer.Write(BinaryPrimitives.ReverseEndianness(correlationId));
        // Client ID (null)
        writer.Write(BinaryPrimitives.ReverseEndianness((short)-1));
        // Replica ID (-1 = consumer)
        writer.Write(BinaryPrimitives.ReverseEndianness(-1));
        // Max Wait Ms
        writer.Write(BinaryPrimitives.ReverseEndianness(500));
        // Min Bytes
        writer.Write(BinaryPrimitives.ReverseEndianness(1));
        // Max Bytes
        writer.Write(BinaryPrimitives.ReverseEndianness(maxBytes));
        // Isolation Level (0 = READ_UNCOMMITTED)
        writer.Write((byte)0);
        // Session ID
        writer.Write(BinaryPrimitives.ReverseEndianness(0));
        // Session Epoch
        writer.Write(BinaryPrimitives.ReverseEndianness(-1));

        // Topics array (1 topic)
        writer.Write(BinaryPrimitives.ReverseEndianness(1));

        var topicBytes = System.Text.Encoding.UTF8.GetBytes(topic);
        writer.Write(BinaryPrimitives.ReverseEndianness((short)topicBytes.Length));
        writer.Write(topicBytes);

        // Partitions (1 partition)
        writer.Write(BinaryPrimitives.ReverseEndianness(1));
        writer.Write(BinaryPrimitives.ReverseEndianness(partition));
        writer.Write(BinaryPrimitives.ReverseEndianness(-1)); // Current leader epoch
        writer.Write(BinaryPrimitives.ReverseEndianness(offset));
        writer.Write(BinaryPrimitives.ReverseEndianness(-1L)); // Log start offset
        writer.Write(BinaryPrimitives.ReverseEndianness(maxBytes));

        // Forgotten topics
        writer.Write(BinaryPrimitives.ReverseEndianness(0));
        // Rack ID
        writer.Write(BinaryPrimitives.ReverseEndianness((short)-1));

        var data = ms.ToArray();
        BinaryPrimitives.WriteInt32BigEndian(data.AsSpan(0, 4), data.Length - 4);
        return data;
    }

    private byte[] SerializeMetadataRequest(int correlationId)
    {
        using var ms = new MemoryStream();
        using var writer = new BinaryWriter(ms);

        // Size placeholder
        writer.Write(0);
        // API Key (Metadata = 3)
        writer.Write(BinaryPrimitives.ReverseEndianness((short)3));
        // API Version
        writer.Write(BinaryPrimitives.ReverseEndianness((short)9));
        // Correlation ID
        writer.Write(BinaryPrimitives.ReverseEndianness(correlationId));
        // Client ID (null)
        writer.Write(BinaryPrimitives.ReverseEndianness((short)-1));
        // Topics = null (all topics)
        writer.Write(BinaryPrimitives.ReverseEndianness(-1));
        // Allow auto topic creation = false
        writer.Write((byte)0);
        // Include cluster authorized operations = false
        writer.Write((byte)0);
        // Include topic authorized operations = false
        writer.Write((byte)0);

        var data = ms.ToArray();
        BinaryPrimitives.WriteInt32BigEndian(data.AsSpan(0, 4), data.Length - 4);
        return data;
    }

    private static async Task<byte[]> ReadResponseAsync(Stream stream, CancellationToken ct)
    {
        var sizeBuffer = new byte[4];
        await stream.ReadExactlyAsync(sizeBuffer, ct);
        var size = BinaryPrimitives.ReadInt32BigEndian(sizeBuffer);

        var body = new byte[size];
        await stream.ReadExactlyAsync(body, ct);
        return body;
    }

    private static RemoteFetchResult ParseFetchResponse(byte[] data)
    {
        var result = new RemoteFetchResult();
        var offset = 0;

        // Correlation ID
        offset += 4;
        // Throttle time
        offset += 4;
        // Error code
        offset += 2;
        // Session ID
        offset += 4;

        // Topics array
        var topicCount = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
        offset += 4;

        for (int i = 0; i < topicCount; i++)
        {
            // Topic name
            var topicLen = BinaryPrimitives.ReadInt16BigEndian(data.AsSpan(offset, 2));
            offset += 2;
            offset += topicLen;

            // Partitions
            var partitionCount = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
            offset += 4;

            for (int j = 0; j < partitionCount; j++)
            {
                // Partition
                offset += 4;
                // Error code
                result.ErrorCode = BinaryPrimitives.ReadInt16BigEndian(data.AsSpan(offset, 2));
                offset += 2;
                // High watermark
                result.HighWatermark = BinaryPrimitives.ReadInt64BigEndian(data.AsSpan(offset, 8));
                offset += 8;
                // Last stable offset
                offset += 8;
                // Log start offset
                result.LogStartOffset = BinaryPrimitives.ReadInt64BigEndian(data.AsSpan(offset, 8));
                offset += 8;
                // Aborted transactions
                var abortedCount = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
                offset += 4;
                offset += abortedCount * 16;
                // Preferred read replica
                offset += 4;
                // Record batch
                var recordsLen = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
                offset += 4;

                if (recordsLen > 0)
                {
                    result.RecordBatch = data.AsSpan(offset, recordsLen).ToArray();
                    offset += recordsLen;
                }
            }
        }

        return result;
    }

    private static List<RemoteTopicInfo> ParseMetadataResponse(byte[] data)
    {
        var topics = new List<RemoteTopicInfo>();
        var offset = 0;

        // Correlation ID
        offset += 4;
        // Throttle time (v3+)
        offset += 4;

        // Brokers array - skip
        var brokerCount = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
        offset += 4;
        for (int i = 0; i < brokerCount; i++)
        {
            // Node ID
            offset += 4;
            // Host
            var hostLen = BinaryPrimitives.ReadInt16BigEndian(data.AsSpan(offset, 2));
            offset += 2;
            if (hostLen > 0) offset += hostLen;
            // Port
            offset += 4;
            // Rack (nullable)
            var rackLen = BinaryPrimitives.ReadInt16BigEndian(data.AsSpan(offset, 2));
            offset += 2;
            if (rackLen > 0) offset += rackLen;
        }

        // Cluster ID (v2+)
        var clusterIdLen = BinaryPrimitives.ReadInt16BigEndian(data.AsSpan(offset, 2));
        offset += 2;
        if (clusterIdLen > 0) offset += clusterIdLen;

        // Controller ID (v1+)
        offset += 4;

        // Topics array
        var topicCount = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
        offset += 4;

        for (int i = 0; i < topicCount; i++)
        {
            // Error code
            offset += 2;
            // Topic name
            var topicLen = BinaryPrimitives.ReadInt16BigEndian(data.AsSpan(offset, 2));
            offset += 2;
            var topicName = System.Text.Encoding.UTF8.GetString(data, offset, topicLen);
            offset += topicLen;
            // Is internal
            var isInternal = data[offset] != 0;
            offset += 1;

            // Partitions
            var partCount = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
            offset += 4;

            for (int j = 0; j < partCount; j++)
            {
                // Error code
                offset += 2;
                // Partition index
                offset += 4;
                // Leader
                offset += 4;
                // Leader epoch (v7+)
                offset += 4;
                // Replicas
                var replicaCount = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
                offset += 4;
                offset += replicaCount * 4;
                // ISR
                var isrCount = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
                offset += 4;
                offset += isrCount * 4;
                // Offline replicas (v5+)
                var offlineCount = BinaryPrimitives.ReadInt32BigEndian(data.AsSpan(offset, 4));
                offset += 4;
                offset += offlineCount * 4;
            }

            if (!isInternal)
            {
                topics.Add(new RemoteTopicInfo
                {
                    Name = topicName,
                    PartitionCount = partCount,
                    IsInternal = isInternal
                });
            }
        }

        return topics;
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        if (_connection is not null)
        {
            await _connection.DisposeAsync();
            _connection = null;
        }
    }
}

/// <summary>
/// Result of a fetch operation from a remote cluster.
/// </summary>
public sealed class RemoteFetchResult
{
    public short ErrorCode { get; set; }
    public long HighWatermark { get; set; }
    public long LogStartOffset { get; set; }
    public byte[] RecordBatch { get; set; } = [];
}

/// <summary>
/// Topic info from remote cluster metadata.
/// </summary>
public sealed class RemoteTopicInfo
{
    public required string Name { get; set; }
    public int PartitionCount { get; set; }
    public bool IsInternal { get; set; }
}
