using System.Buffers.Binary;
using Kuestenlogik.Surgewave.Client.Abstractions;
using Kuestenlogik.Surgewave.Client.Consumer;
using Kuestenlogik.Surgewave.Client.Native;
using Kuestenlogik.Surgewave.Client.Native.Operations.ConsumerGroups;
using Kuestenlogik.Surgewave.Client.Serialization;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Transport;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Client;

/// <summary>
/// A strongly-typed Surgewave consumer with optional handler-based dispatch.
/// </summary>
/// <typeparam name="TKey">The key type.</typeparam>
/// <typeparam name="TValue">The value type.</typeparam>
/// <example>
/// <code>
/// // Option 1: Manual consumption with pattern matching
/// await using var consumer = new SurgewaveConsumer&lt;string, OrderEvent&gt;(options =>
/// {
///     options.BootstrapServers = "localhost:9092";
///     options.ValueDeserializer = Serializers.PolymorphicJsonDeserializer&lt;OrderEvent&gt;(
///         typeof(OrderCreated), typeof(OrderShipped));
/// });
/// consumer.Subscribe("orders");
///
/// var result = await consumer.ConsumeAsync();
/// switch (result?.Value)
/// {
///     case OrderCreated created: Console.WriteLine($"Created: {created.OrderId}"); break;
///     case OrderShipped shipped: Console.WriteLine($"Shipped: {shipped.TrackingNumber}"); break;
/// }
///
/// // Option 2: Handler-based dispatch
/// consumer.OnMessage&lt;OrderCreated&gt;(async (msg, ct) =>
/// {
///     Console.WriteLine($"Order created: {msg.Value.OrderId}");
/// });
/// consumer.OnMessage&lt;OrderShipped&gt;(async (msg, ct) =>
/// {
///     Console.WriteLine($"Order shipped: {msg.Value.TrackingNumber}");
/// });
/// await consumer.ConsumeLoopAsync(cancellationToken);
/// </code>
/// </example>
public sealed class SurgewaveConsumer<TKey, TValue> : IConsumer<TKey, TValue>
{
    private readonly SurgewaveConsumerOptions<TKey, TValue> _options;
    private readonly string _host;
    private readonly int _port;
    private SurgewaveNativeClient _client;
    private readonly List<string> _subscribedTopics = [];
    private readonly Dictionary<(string topic, int partition), long> _offsets = [];
    // Already-decoded batch buffer per (topic,partition): the facade fetches a whole batch, so
    // serve its remaining messages by cursor instead of re-fetching + re-decoding one full batch
    // per delivered message (#80). Single-consumer-thread contract, same as _offsets. Invalidated
    // on Assign/Seek/retention-jump/reconnect so a relocated offset never serves a stale record.
    private readonly Dictionary<(string topic, int partition), (List<ReceivedMessage> Messages, int Index)> _decodedBuffers = [];
    private readonly Dictionary<Type, Func<ConsumeResult<TKey, TValue>, CancellationToken, Task>> _handlers = [];
    private readonly HashSet<(string topic, int partition)> _pausedPartitions = [];
    private Func<ConsumeResult<TKey, TValue>, CancellationToken, Task>? _defaultHandler;
    private bool _disposed;
    private bool _isConnected;
    private int _reconnectAttempts;

    // Consumer group state
    private string? _memberId;
    private int _generationId;
    private CancellationTokenSource? _heartbeatCts;
    private Task? _heartbeatTask;
    private Task? _autoCommitTask;
    private bool _initialized;

    /// <summary>
    /// Event raised when the consumer disconnects from the broker.
    /// </summary>
    public event EventHandler<DisconnectedEventArgs>? Disconnected;

    /// <summary>
    /// Event raised when the consumer reconnects to the broker.
    /// </summary>
    public event EventHandler? Reconnected;

    /// <summary>
    /// Event raised when partitions are assigned (for consumer groups).
    /// </summary>
    public event EventHandler<PartitionsAssignedEventArgs>? PartitionsAssigned;

    /// <summary>
    /// Event raised when partitions are revoked (for consumer groups).
    /// </summary>
    public event EventHandler<PartitionsRevokedEventArgs>? PartitionsRevoked;

    /// <inheritdoc />
    public ProtocolType Protocol => ProtocolType.SurgewaveNative;

    /// <inheritdoc />
    public bool IsConnected => _isConnected;

    /// <inheritdoc />
    public IReadOnlyList<(string topic, int partition)> Assignment => _offsets.Keys.ToList();

    /// <summary>
    /// Gets the set of currently paused partitions.
    /// </summary>
    public IReadOnlySet<(string topic, int partition)> PausedPartitions => _pausedPartitions;

    // Internal methods to raise rebalance events (for future consumer group implementation)
    internal void OnPartitionsAssigned(IReadOnlyList<(string Topic, int Partition)> partitions)
        => PartitionsAssigned?.Invoke(this, new PartitionsAssignedEventArgs(partitions));

    internal void OnPartitionsRevoked(IReadOnlyList<(string Topic, int Partition)> partitions)
        => PartitionsRevoked?.Invoke(this, new PartitionsRevokedEventArgs(partitions));

    /// <summary>
    /// Creates a new consumer with configuration.
    /// </summary>
    public SurgewaveConsumer(Action<SurgewaveConsumerOptions<TKey, TValue>> configure)
    {
        _options = new SurgewaveConsumerOptions<TKey, TValue>();
        configure(_options);
        _options.Validate();

        (_host, _port) = ParseBootstrapServers(_options.BootstrapServers!);
        _client = new SurgewaveNativeClient(_host, _port, _options.Transport);
        ConnectWithRetry();
    }

    /// <summary>
    /// Creates a new consumer with options object.
    /// </summary>
    public SurgewaveConsumer(SurgewaveConsumerOptions<TKey, TValue> options)
    {
        _options = options;
        _options.Validate();

        (_host, _port) = ParseBootstrapServers(_options.BootstrapServers!);
        _client = new SurgewaveNativeClient(_host, _port, _options.Transport);
        ConnectWithRetry();
    }

    private SurgewaveConsumer(SurgewaveConsumerOptions<TKey, TValue> options, SurgewaveNativeClient client, string host, int port)
    {
        _options = options;
        _client = client;
        _host = host;
        _port = port;
        _isConnected = true;
        _reconnectAttempts = 0;
    }

    /// <summary>
    /// Creates a new consumer asynchronously (preferred over constructor in async contexts).
    /// </summary>
#pragma warning disable CA2000 // Client ownership transfers to returned consumer
    public static async Task<SurgewaveConsumer<TKey, TValue>> CreateAsync(
        Action<SurgewaveConsumerOptions<TKey, TValue>> configure,
        ILogger? logger = null,
        CancellationToken cancellationToken = default)
    {
        var options = new SurgewaveConsumerOptions<TKey, TValue>();
        configure(options);
        options.Validate();

        var (host, port) = ParseBootstrapServers(options.BootstrapServers!);
        var client = new SurgewaveNativeClient(host, port, options.Transport, logger: logger);
        try
        {
            await client.ConnectAsync(cancellationToken);
        }
        catch
        {
            await client.DisposeAsync();
            throw;
        }

        return new SurgewaveConsumer<TKey, TValue>(options, client, host, port);
    }

    /// <summary>
    /// Creates a new consumer asynchronously (preferred over constructor in async contexts).
    /// </summary>
    public static async Task<SurgewaveConsumer<TKey, TValue>> CreateAsync(
        SurgewaveConsumerOptions<TKey, TValue> options,
        ILogger? logger = null,
        CancellationToken cancellationToken = default)
    {
        options.Validate();

        var (host, port) = ParseBootstrapServers(options.BootstrapServers!);
        var client = new SurgewaveNativeClient(host, port, options.Transport, logger: logger);
        try
        {
            await client.ConnectAsync(cancellationToken);
        }
        catch
        {
            await client.DisposeAsync();
            throw;
        }

        return new SurgewaveConsumer<TKey, TValue>(options, client, host, port);
    }
#pragma warning restore CA2000

    private void ConnectWithRetry()
    {
        try
        {
            _client.ConnectAsync().GetAwaiter().GetResult();
            _isConnected = true;
            _reconnectAttempts = 0;
        }
        catch (Exception ex)
        {
            _isConnected = false;
            throw new BrokerConnectionException(
                $"Failed to connect to broker at {_host}:{_port}",
                _host, _port, ex);
        }
    }

    /// <summary>
    /// Subscribe to topics. Partition discovery and consumer group join happen lazily on first consume.
    /// </summary>
    public void Subscribe(params string[] topics)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _subscribedTopics.AddRange(topics);
        // Partition discovery/group join deferred to first ConsumeAsync()
    }

    /// <summary>
    /// Subscribe to topics asynchronously with immediate partition discovery and consumer group support.
    /// </summary>
    public async Task SubscribeAsync(CancellationToken cancellationToken = default, params string[] topics)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        _subscribedTopics.AddRange(topics);
        await InitializeSubscriptionAsync(cancellationToken);
    }

    /// <summary>
    /// Initialize subscription by discovering partitions or joining consumer group.
    /// </summary>
    private async Task InitializeSubscriptionAsync(CancellationToken cancellationToken)
    {
        if (_initialized) return;

        // Ensure topics exist on the broker (triggers auto-creation if enabled)
        await EnsureTopicsExistAsync(cancellationToken);

        if (!string.IsNullOrEmpty(_options.GroupId))
        {
            // Consumer group mode - join group and wait for assignment
            await JoinConsumerGroupAsync(cancellationToken);
        }
        else
        {
            // Standalone mode - discover all partitions via topic metadata
            await DiscoverTopicPartitionsAsync(cancellationToken);
        }

        // Only mark initialized if we actually have partition assignments
        _initialized = _offsets.Count > 0;
    }

    /// <summary>
    /// Ensure all subscribed topics exist on the broker by triggering auto-creation.
    /// Sends a ListOffsets request for partition 0 of each topic, which causes the broker
    /// to auto-create the topic if AutoCreateTopics is enabled.
    /// Respects TopicDiscoveryTimeoutMs for waiting.
    /// </summary>
    private async Task EnsureTopicsExistAsync(CancellationToken cancellationToken)
    {
        var startTime = DateTime.UtcNow;
        var pendingTopics = new HashSet<string>(_subscribedTopics);

        while (pendingTopics.Count > 0)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var created = new List<string>();
            foreach (var topic in pendingTopics)
            {
                try
                {
                    // GetEarliestOffsetAsync triggers auto-creation on the broker (ListOffsets handler)
                    await _client.Messaging.GetEarliestOffsetAsync(topic, 0, cancellationToken);
                    created.Add(topic);
                }
                catch (ProtocolException)
                {
                    // Topic could not be created (AutoCreateTopics disabled) — will be retried or timeout
                }
            }

            foreach (var topic in created)
                pendingTopics.Remove(topic);

            if (pendingTopics.Count == 0)
                break;

            // Check timeout
            var elapsed = DateTime.UtcNow - startTime;
            if (_options.TopicDiscoveryTimeoutMs == 0)
            {
                throw new TopicPartitionException(
                    $"Topics not found: {string.Join(", ", pendingTopics)}. " +
                    "Set TopicDiscoveryTimeoutMs > 0 to wait for topics to be created.");
            }

            if (_options.TopicDiscoveryTimeoutMs > 0 && elapsed.TotalMilliseconds >= _options.TopicDiscoveryTimeoutMs)
            {
                throw new TopicPartitionException(
                    $"Timeout waiting for topics: {string.Join(", ", pendingTopics)}. " +
                    $"Waited {elapsed.TotalSeconds:F1}s (TopicDiscoveryTimeoutMs={_options.TopicDiscoveryTimeoutMs}).");
            }

            await Task.Delay(_options.TopicDiscoveryRetryIntervalMs, cancellationToken);
        }
    }

    /// <summary>
    /// Discover partitions for subscribed topics, waiting if topics don't exist yet.
    /// Topics should already exist at this point (ensured by EnsureTopicsExistAsync).
    /// </summary>
    private async Task DiscoverTopicPartitionsAsync(CancellationToken cancellationToken)
    {
        var topicInfos = await _client.Topics.ListAsync(cancellationToken);
        var topicMap = topicInfos.ToDictionary(t => t.Name, t => t.PartitionCount);

        foreach (var topic in _subscribedTopics)
        {
            if (topicMap.TryGetValue(topic, out var partitionCount))
            {
                for (int partition = 0; partition < partitionCount; partition++)
                {
                    var startOffset = _options.AutoOffsetReset == AutoOffsetReset.Earliest
                        ? await _client.Messaging.GetEarliestOffsetAsync(topic, partition, cancellationToken)
                        : await _client.Messaging.GetLatestOffsetAsync(topic, partition, cancellationToken);
                    _offsets[(topic, partition)] = startOffset;
                }
            }
        }
    }

    /// <summary>
    /// Join a consumer group and receive partition assignment.
    /// </summary>
    private async Task JoinConsumerGroupAsync(CancellationToken cancellationToken)
    {
        // Build consumer protocol metadata (topics to subscribe)
        var metadata = ConsumerProtocolCodec.BuildConsumerMetadata(_subscribedTopics);
        var protocols = new List<(string Name, byte[] Metadata)>
        {
            ("range", metadata),
            ("roundrobin", metadata)
        };

        // Join group
        var joinResult = await _client.Groups.JoinAsync(
            _options.GroupId!,
            _memberId,
            _options.ClientId ?? "surgewave-consumer",
            "consumer",
            _options.SessionTimeoutMs,
            _options.MaxPollIntervalMs, // rebalance timeout
            protocols,
            cancellationToken);

        if (joinResult.ErrorCode != 0)
        {
            throw new ProtocolException(
                SurgewaveOpCode.JoinGroup,
                (SurgewaveErrorCode)joinResult.ErrorCode);
        }

        _memberId = joinResult.MemberId;
        _generationId = joinResult.GenerationId;

        // If we're the leader, compute assignments
        List<(string MemberId, byte[] Assignment)> assignments = [];

        if (joinResult.LeaderId == _memberId)
        {
            assignments = ComputeAssignments(joinResult.Members, _subscribedTopics);
        }

        // Sync group to get our assignment
        var syncResult = await _client.Groups.SyncAsync(
            _options.GroupId!,
            _memberId,
            _generationId,
            assignments,
            cancellationToken);

        if (syncResult.ErrorCode != 0)
        {
            throw new ProtocolException(
                SurgewaveOpCode.SyncGroup,
                (SurgewaveErrorCode)syncResult.ErrorCode);
        }

        // Parse assignment and update offsets
        var assignedPartitions = ConsumerProtocolCodec.ParseAssignment(syncResult.Assignment);

        // Fetch committed offsets for assigned partitions
        foreach (var (topic, partition) in assignedPartitions)
        {
            try
            {
                var committed = await _client.Groups.FetchOffsetAsync(
                    _options.GroupId!, topic, partition, cancellationToken);

                if (committed >= 0)
                {
                    // Validate committed offset against topic bounds (like Kafka's auto.offset.reset
                    // behavior for out-of-range offsets). A stale committed offset that is beyond the
                    // topic's current end would cause the consumer to wait forever for non-existent data.
                    var latestOffset = await _client.Messaging.GetLatestOffsetAsync(topic, partition, cancellationToken);
                    if (committed <= latestOffset)
                    {
                        _offsets[(topic, partition)] = committed;
                    }
                    else
                    {
                        // Committed offset is beyond topic end — fall back to auto offset reset
                        var offset = _options.AutoOffsetReset == AutoOffsetReset.Earliest
                            ? await _client.Messaging.GetEarliestOffsetAsync(topic, partition, cancellationToken)
                            : latestOffset;
                        _offsets[(topic, partition)] = offset;
                    }
                }
                else
                {
                    var offset = _options.AutoOffsetReset == AutoOffsetReset.Earliest
                        ? await _client.Messaging.GetEarliestOffsetAsync(topic, partition, cancellationToken)
                        : await _client.Messaging.GetLatestOffsetAsync(topic, partition, cancellationToken);
                    _offsets[(topic, partition)] = offset;
                }
            }
            catch
            {
                // No committed offset, query actual offset from broker
                var offset = _options.AutoOffsetReset == AutoOffsetReset.Earliest
                    ? await _client.Messaging.GetEarliestOffsetAsync(topic, partition, cancellationToken)
                    : await _client.Messaging.GetLatestOffsetAsync(topic, partition, cancellationToken);
                _offsets[(topic, partition)] = offset;
            }
        }

        // Trigger PartitionsAssigned event
        OnPartitionsAssigned(assignedPartitions);

        // Start heartbeat background task
        StartHeartbeatTask();

        // Start auto-commit task if enabled
        if (_options.EnableAutoCommit)
        {
            StartAutoCommitTask();
        }
    }

    /// <summary>
    /// Compute partition assignments as leader.
    /// Uses simple round-robin assignment.
    /// </summary>
    private List<(string MemberId, byte[] Assignment)> ComputeAssignments(
        List<JoinGroupMember> members,
        List<string> topics)
    {
        // Get topic metadata to know partition counts
        var topicInfos = _client.Topics.ListAsync().GetAwaiter().GetResult();
        var topicPartitions = new List<(string Topic, int Partition)>();

        foreach (var topic in topics)
        {
            var info = topicInfos.FirstOrDefault(t => t.Name == topic);
            var partitionCount = info?.PartitionCount ?? 1;

            for (int p = 0; p < partitionCount; p++)
            {
                topicPartitions.Add((topic, p));
            }
        }

        // Round-robin assign partitions to members
        var memberAssignments = members.ToDictionary(
            m => m.MemberId,
            _ => new List<(string Topic, int Partition)>());

        var memberIds = members.Select(m => m.MemberId).ToList();
        for (int i = 0; i < topicPartitions.Count; i++)
        {
            var memberId = memberIds[i % memberIds.Count];
            memberAssignments[memberId].Add(topicPartitions[i]);
        }

        // Encode assignments
        return memberAssignments.Select(kvp =>
            (kvp.Key, ConsumerProtocolCodec.BuildAssignment(kvp.Value))).ToList();
    }

    /// <summary>
    /// Assign specific topic-partition.
    /// </summary>
    public void Assign(string topic, int partition, long offset = 0)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _offsets[(topic, partition)] = offset;
        _decodedBuffers.Remove((topic, partition)); // offset moved — a stale buffer must not serve (#80)
    }

    /// <summary>
    /// Consume a single message.
    /// </summary>
    public async Task<ConsumeResult<TKey, TValue>?> ConsumeAsync(CancellationToken cancellationToken = default)
    {
        return await ConsumeAsync(TimeSpan.FromMilliseconds(_options.MaxPollIntervalMs), cancellationToken);
    }

    /// <summary>
    /// Consume a single message with timeout.
    /// </summary>
    /// <exception cref="BrokerConnectionException">Thrown when connection fails and auto-reconnect is disabled or exhausted.</exception>
    public async Task<ConsumeResult<TKey, TValue>?> ConsumeAsync(
        TimeSpan timeout,
        CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        // Lazy initialization on first consume (for fire-and-forget Subscribe())
        // Also retry initialization if previous attempt yielded 0 partitions (topic didn't exist yet)
        if (_subscribedTopics.Count > 0 && _offsets.Count == 0)
        {
            _initialized = false;
            await InitializeSubscriptionAsync(cancellationToken);
        }

        if (_offsets.Count == 0)
            throw new InvalidConfigurationException("Topics", null, "No topics subscribed or assigned. Use Subscribe() or Assign() first");

        foreach (var ((topic, partition), offset) in _offsets.ToList())
        {
            if (offset < 0) continue; // Skip if offset is -1 (latest, not yet determined)
            if (_pausedPartitions.Contains((topic, partition))) continue; // Skip paused partitions

            // Serve from the already-decoded buffer before issuing any fetch (#80). Semantics are
            // unchanged: we keep the Offset>=offset filter, advance _offsets by exactly +1 per
            // served message, and deserialize after advancing — identical to the fetch path below.
            if (_decodedBuffers.TryGetValue((topic, partition), out var buffered))
            {
                var idx = buffered.Index;
                while (idx < buffered.Messages.Count && buffered.Messages[idx].Offset < offset)
                    idx++;

                if (idx < buffered.Messages.Count)
                {
                    // A fetch would have observed cancellation here; a buffered serve must too.
                    cancellationToken.ThrowIfCancellationRequested();
                    var bufMsg = buffered.Messages[idx];
                    _decodedBuffers[(topic, partition)] = (buffered.Messages, idx + 1);
                    _offsets[(topic, partition)] = bufMsg.Offset + 1;
                    return await BuildConsumeResultAsync(topic, partition, bufMsg, cancellationToken).ConfigureAwait(false);
                }

                _decodedBuffers.Remove((topic, partition)); // exhausted — fall through to fetch
            }

            try
            {
                // Per-partition long-poll budget: honour the caller's overall
                // timeout. Without this, a Consume(timeout: 500ms) would still
                // sit in a 5s long-poll per partition — N partitions × 5s on a
                // drained topic blows past any test expectation.
                var perPartitionWaitMs = Math.Max(1, (int)Math.Min(timeout.TotalMilliseconds, 5000));
                var result = await _client.Messaging.ReceiveAsync(topic, partition, offset, _options.FetchMaxBytes, maxWaitMs: perPartitionWaitMs, cancellationToken);
                _isConnected = true;
                _reconnectAttempts = 0;

                // Handle case where requested offset has no data but newer data exists
                // This can happen when old segments were deleted (retention policy)
                if (result.Messages.Count == 0 && result.HighWatermark > offset)
                {
                    // Jump to latest available data
                    var latestOffset = await _client.Messaging.GetLatestOffsetAsync(topic, partition, cancellationToken);
                    _offsets[(topic, partition)] = latestOffset;
                    _decodedBuffers.Remove((topic, partition)); // offset relocated — drop stale buffer
                    continue;
                }

                if (result.Messages.Count > 0)
                {
                    // Find the first message at or after the requested offset
                    // (broker may return a batch that starts before the requested offset)
                    var firstIndex = result.Messages.FindIndex(m => m.Offset >= offset);
                    if (firstIndex < 0)
                    {
                        // All messages are below requested offset - shouldn't happen but handle gracefully
                        continue;
                    }
                    var msg = result.Messages[firstIndex];
                    // Keep the rest of the decoded batch to serve on subsequent calls (#80).
                    _decodedBuffers[(topic, partition)] = (result.Messages, firstIndex + 1);
                    _offsets[(topic, partition)] = msg.Offset + 1;

                    return await BuildConsumeResultAsync(topic, partition, msg, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (Exception ex) when (IsConnectionError(ex))
            {
                await HandleDisconnectionAsync(ex, cancellationToken);
                // After reconnect, retry the fetch
                return await ConsumeAsync(timeout, cancellationToken);
            }
        }

        // No messages available, wait a bit
        await Task.Delay(Math.Min(100, (int)timeout.TotalMilliseconds), cancellationToken);
        return null;
    }

    /// <summary>
    /// Deserializes one already-decoded message into a <see cref="ConsumeResult{TKey, TValue}"/>.
    /// Shared by the buffered-serve and fetch-serve paths so both behave identically (#80).
    /// </summary>
    private async Task<ConsumeResult<TKey, TValue>> BuildConsumeResultAsync(
        string topic, int partition, ReceivedMessage msg, CancellationToken cancellationToken)
    {
        var keyValue = msg.Key is { Length: > 0 }
            ? await DeserializeKeyAsync(msg.Key, topic, cancellationToken).ConfigureAwait(false)
            : default;
        var valueValue = await DeserializeValueAsync(msg.Value, topic, cancellationToken).ConfigureAwait(false);

        return new ConsumeResult<TKey, TValue>
        {
            Topic = topic,
            Partition = partition,
            Offset = msg.Offset,
            Timestamp = DateTimeOffset.FromUnixTimeMilliseconds(msg.Timestamp),
            Key = keyValue,
            Value = valueValue,
            Headers = msg.Headers
        };
    }

    private ValueTask<TKey> DeserializeKeyAsync(byte[] data, string topic, CancellationToken cancellationToken)
    {
        if (_options.AsyncKeyDeserializer != null)
            return _options.AsyncKeyDeserializer.DeserializeAsync(data, topic, cancellationToken);

        return new ValueTask<TKey>(_options.KeyDeserializer.Deserialize(data.AsSpan(), topic));
    }

    private ValueTask<TValue> DeserializeValueAsync(byte[] data, string topic, CancellationToken cancellationToken)
    {
        if (_options.AsyncValueDeserializer != null)
            return _options.AsyncValueDeserializer.DeserializeAsync(data, topic, cancellationToken);

        return new ValueTask<TValue>(_options.ValueDeserializer.Deserialize(data.AsSpan(), topic));
    }

    private static bool IsConnectionError(Exception ex)
    {
        return ex is System.Net.Sockets.SocketException
            || ex is System.IO.IOException
            || ex is InvalidOperationException ioe && ioe.Message.Contains("not connected");
    }

    private async Task HandleDisconnectionAsync(Exception ex, CancellationToken cancellationToken)
    {
        _isConnected = false;
        // Drop all decoded buffers: the connection broke and offsets may relocate on reconnect,
        // so nothing buffered from the old client should be served (#80).
        _decodedBuffers.Clear();
        Disconnected?.Invoke(this, new DisconnectedEventArgs(ex));

        if (!_options.EnableAutoReconnect)
        {
            throw new BrokerConnectionException(
                $"Disconnected from broker at {_host}:{_port}",
                _host, _port, ex);
        }

        // Attempt reconnection with exponential backoff
        while (_reconnectAttempts < _options.MaxReconnectAttempts && !cancellationToken.IsCancellationRequested)
        {
            _reconnectAttempts++;
            var delay = Math.Min(
                _options.ReconnectBackoffMs * (1 << Math.Min(_reconnectAttempts - 1, 10)),
                _options.ReconnectBackoffMaxMs);

            await Task.Delay(delay, cancellationToken);

            try
            {
                await _client.DisposeAsync();
                _client = new SurgewaveNativeClient(_host, _port, _options.Transport);
                await _client.ConnectAsync();
                _isConnected = true;
                _reconnectAttempts = 0;
                Reconnected?.Invoke(this, EventArgs.Empty);
                return;
            }
            catch
            {
                // Continue retry loop
            }
        }

        throw new BrokerConnectionException(
            $"Failed to reconnect to broker at {_host}:{_port} after {_reconnectAttempts} attempts",
            _host, _port, ex);
    }

    /// <summary>
    /// Seek to a specific offset.
    /// </summary>
    public void Seek(string topic, int partition, long offset)
    {
        _offsets[(topic, partition)] = offset;
        _decodedBuffers.Remove((topic, partition)); // offset moved — a stale buffer must not serve (#80)
    }

    /// <summary>
    /// Commit offsets for all assigned partitions (for consumer groups).
    /// </summary>
    public async Task CommitAsync(CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(_options.GroupId))
            throw new InvalidConfigurationException(nameof(_options.GroupId), null, "Offset commit requires a consumer group");

        if (_memberId == null)
            throw new InvalidConfigurationException("MemberId", null, "Consumer has not joined a group. Use SubscribeAsync() first");

        foreach (var ((topic, partition), offset) in _offsets.ToList())
        {
            await _client.Groups.CommitOffsetAsync(
                _options.GroupId,
                _memberId,
                _generationId,
                topic,
                partition,
                offset,
                cancellationToken);
        }
    }

    /// <summary>
    /// Commit offset for a specific partition.
    /// </summary>
    public async Task CommitAsync(string topic, int partition, long offset, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(_options.GroupId))
            throw new InvalidConfigurationException(nameof(_options.GroupId), null, "Offset commit requires a consumer group");

        if (_memberId == null)
            throw new InvalidConfigurationException("MemberId", null, "Consumer has not joined a group. Use SubscribeAsync() first");

        await _client.Groups.CommitOffsetAsync(
            _options.GroupId,
            _memberId,
            _generationId,
            topic,
            partition,
            offset,
            cancellationToken);

        _offsets[(topic, partition)] = offset;
    }

    /// <inheritdoc />
    public Task CommitAsync(TopicPartitionOffset offset, CancellationToken cancellationToken = default)
        => CommitAsync(offset.Topic, offset.Partition, offset.Offset, cancellationToken);

    /// <inheritdoc />
    public Task CommitAsync(ConsumeResult<TKey, TValue> result, CancellationToken cancellationToken = default)
        => CommitAsync(result.Topic, result.Partition, result.Offset + 1, cancellationToken);

    /// <inheritdoc />
    public async Task CommitAsync(IEnumerable<TopicPartitionOffset> offsets, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(_options.GroupId))
            throw new InvalidConfigurationException(nameof(_options.GroupId), null, "Offset commit requires a consumer group");

        if (_memberId == null)
            throw new InvalidConfigurationException("MemberId", null, "Consumer has not joined a group. Use SubscribeAsync() first");

        // Batch commit - commit all offsets
        foreach (var offset in offsets)
        {
            await _client.Groups.CommitOffsetAsync(
                _options.GroupId,
                _memberId,
                _generationId,
                offset.Topic,
                offset.Partition,
                offset.Offset,
                cancellationToken).ConfigureAwait(false);

            _offsets[(offset.Topic, offset.Partition)] = offset.Offset;
        }
    }

    /// <summary>
    /// Start the heartbeat background task.
    /// </summary>
    private void StartHeartbeatTask()
    {
        _heartbeatCts = new CancellationTokenSource();
        _heartbeatTask = HeartbeatLoopAsync(_heartbeatCts.Token);
    }

    /// <summary>
    /// Heartbeat loop to keep the consumer group session alive.
    /// </summary>
    private async Task HeartbeatLoopAsync(CancellationToken cancellationToken)
    {
        var interval = TimeSpan.FromMilliseconds(_options.HeartbeatIntervalMs);

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(interval, cancellationToken);

                var errorCode = await _client.Groups.HeartbeatAsync(
                    _options.GroupId!,
                    _memberId!,
                    _generationId,
                    cancellationToken);

                if (errorCode == (ushort)SurgewaveErrorCode.RebalanceInProgress)
                {
                    // Rebalance triggered - rejoin group
                    await HandleRebalanceAsync(cancellationToken);
                }
                else if (errorCode != 0)
                {
                    // Other error - may need to rejoin
                    _isConnected = false;
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception)
            {
                // Log and continue - heartbeat failed, session may expire
                _isConnected = false;
            }
        }
    }

    /// <summary>
    /// Handle a rebalance event by revoking partitions and rejoining the group.
    /// </summary>
    private async Task HandleRebalanceAsync(CancellationToken cancellationToken)
    {
        // Revoke current partitions
        var currentPartitions = _offsets.Keys.ToList();
        OnPartitionsRevoked(currentPartitions);
        _offsets.Clear();

        // Rejoin group
        await JoinConsumerGroupAsync(cancellationToken);
    }

    /// <summary>
    /// Start the auto-commit background task.
    /// </summary>
    private void StartAutoCommitTask()
    {
        if (!_options.EnableAutoCommit || string.IsNullOrEmpty(_options.GroupId))
            return;

        _autoCommitTask = AutoCommitLoopAsync(_heartbeatCts!.Token);
    }

    /// <summary>
    /// Auto-commit loop to periodically commit offsets.
    /// </summary>
    private async Task AutoCommitLoopAsync(CancellationToken cancellationToken)
    {
        var interval = TimeSpan.FromMilliseconds(_options.AutoCommitIntervalMs);

        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(interval, cancellationToken);

                if (_memberId != null && _offsets.Count > 0)
                {
                    await CommitAsync(cancellationToken);
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch
            {
                // Log and continue - auto-commit failed
            }
        }
    }

    /// <summary>
    /// Pause consumption from the specified partitions.
    /// </summary>
    public void Pause(params (string topic, int partition)[] partitions)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        foreach (var tp in partitions)
            _pausedPartitions.Add(tp);
    }

    /// <summary>
    /// Resume consumption from the specified partitions.
    /// </summary>
    public void Resume(params (string topic, int partition)[] partitions)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        foreach (var tp in partitions)
            _pausedPartitions.Remove(tp);
    }

    /// <summary>
    /// Get the current position (next offset to be fetched) for a partition.
    /// </summary>
    public long Position(string topic, int partition)
    {
        if (_offsets.TryGetValue((topic, partition), out var offset))
            return offset;

        throw new InvalidConfigurationException("Partition", (topic, partition), "Partition not assigned");
    }

    /// <summary>
    /// Get the lag for a specific partition (high watermark - current position).
    /// </summary>
    public async Task<long> GetLagAsync(string topic, int partition, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (!_offsets.TryGetValue((topic, partition), out var currentOffset))
            throw new InvalidConfigurationException("Partition", (topic, partition), "Partition not assigned");

        var highWatermark = await _client.Messaging.GetLatestOffsetAsync(topic, partition, cancellationToken);
        return Math.Max(0, highWatermark - currentOffset);
    }

    /// <summary>
    /// Get the lag for all assigned partitions.
    /// </summary>
    public async Task<Dictionary<(string topic, int partition), long>> GetAllLagAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var result = new Dictionary<(string topic, int partition), long>();

        foreach (var ((topic, partition), currentOffset) in _offsets)
        {
            var highWatermark = await _client.Messaging.GetLatestOffsetAsync(topic, partition, cancellationToken);
            result[(topic, partition)] = Math.Max(0, highWatermark - currentOffset);
        }

        return result;
    }

    /// <summary>
    /// Register a handler for a specific derived type.
    /// Use with polymorphic deserializers to dispatch messages based on their runtime type.
    /// </summary>
    /// <typeparam name="TDerived">The derived type to handle.</typeparam>
    /// <param name="handler">The async handler function.</param>
    /// <returns>This consumer for fluent chaining.</returns>
    public SurgewaveConsumer<TKey, TValue> OnMessage<TDerived>(
        Func<ConsumeResult<TKey, TDerived>, CancellationToken, Task> handler)
        where TDerived : TValue
    {
        _handlers[typeof(TDerived)] = async (result, ct) =>
        {
            if (result.Value is TDerived derivedValue)
            {
                var derivedResult = new ConsumeResult<TKey, TDerived>
                {
                    Topic = result.Topic,
                    Partition = result.Partition,
                    Offset = result.Offset,
                    Timestamp = result.Timestamp,
                    Key = result.Key,
                    Value = derivedValue
                };
                await handler(derivedResult, ct);
            }
        };
        return this;
    }

    /// <summary>
    /// Register a default handler for messages that don't match any specific type handler.
    /// </summary>
    /// <param name="handler">The async handler function.</param>
    /// <returns>This consumer for fluent chaining.</returns>
    public SurgewaveConsumer<TKey, TValue> OnMessage(
        Func<ConsumeResult<TKey, TValue>, CancellationToken, Task> handler)
    {
        _defaultHandler = handler;
        return this;
    }

    /// <summary>
    /// Consume messages continuously and dispatch to registered handlers.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token to stop consuming.</param>
    /// <exception cref="InvalidConfigurationException">No handlers registered.</exception>
    public async Task ConsumeLoopAsync(CancellationToken cancellationToken = default)
    {
        if (_handlers.Count == 0 && _defaultHandler == null)
            throw new InvalidConfigurationException("Handlers", null, "No message handlers registered. Use OnMessage<T>() to register handlers");

        while (!cancellationToken.IsCancellationRequested)
        {
            var result = await ConsumeAsync(cancellationToken);
            if (result == null) continue;

            await DispatchToHandlerAsync(result, cancellationToken);
        }
    }

    /// <summary>
    /// Consume a single message and dispatch to the appropriate handler.
    /// </summary>
    /// <returns>True if a message was processed, false if no message was available.</returns>
    public async Task<bool> ConsumeAndDispatchAsync(CancellationToken cancellationToken = default)
    {
        var result = await ConsumeAsync(cancellationToken);
        if (result == null) return false;

        await DispatchToHandlerAsync(result, cancellationToken);
        return true;
    }

    private async Task DispatchToHandlerAsync(ConsumeResult<TKey, TValue> result, CancellationToken cancellationToken)
    {
        var valueType = result.Value?.GetType() ?? typeof(TValue);

        // Try to find a handler for the exact runtime type
        if (_handlers.TryGetValue(valueType, out var handler))
        {
            await handler(result, cancellationToken);
            return;
        }

        // Try base types in the hierarchy
        var baseType = valueType.BaseType;
        while (baseType != null && baseType != typeof(object))
        {
            if (_handlers.TryGetValue(baseType, out handler))
            {
                await handler(result, cancellationToken);
                return;
            }
            baseType = baseType.BaseType;
        }

        // Fall back to default handler
        if (_defaultHandler != null)
        {
            await _defaultHandler(result, cancellationToken);
        }
    }

    private static (string host, int port) ParseBootstrapServers(string servers)
    {
        var parts = servers.Split(':');
        return (parts[0], parts.Length > 1 ? int.Parse(parts[1]) : 9092);
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed) return;
        _disposed = true;

        // Stop background tasks
        if (_heartbeatCts != null)
        {
            await _heartbeatCts.CancelAsync();

            if (_heartbeatTask != null)
            {
                try
                {
                    await _heartbeatTask;
                }
                catch (OperationCanceledException)
                {
                    // Expected
                }
            }

            if (_autoCommitTask != null)
            {
                try
                {
                    await _autoCommitTask;
                }
                catch (OperationCanceledException)
                {
                    // Expected
                }
            }

            _heartbeatCts.Dispose();
        }

        // Leave consumer group gracefully
        if (!string.IsNullOrEmpty(_options.GroupId) && _memberId != null)
        {
            try
            {
                await _client.Groups.LeaveAsync(_options.GroupId, _memberId);
            }
            catch
            {
                // Best effort - ignore errors on leave
            }
        }

        await _client.DisposeAsync();
    }
}
