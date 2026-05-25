namespace Kuestenlogik.Surgewave.Client.Native;

/// <summary>
/// Seek mode for receive operations.
/// </summary>
public enum SeekMode
{
    Offset,
    Beginning,
    End,
    Timestamp
}

/// <summary>
/// Fluent builder for receive operations.
/// </summary>
public sealed class ReceiveBuilder
{
    private readonly SurgewaveNativeClient _client;
    private readonly string _topic;
    private int _partition;
    private int[]? _partitions;
    private bool _allPartitions;
    private int _parallelism = 1;
    private long _offset;
    private SeekMode _seekMode = SeekMode.Offset;
    private DateTimeOffset? _seekTimestamp;
    private int _maxBytes = 1024 * 1024;
    private int? _takeLimit;
    private List<Func<ReceivedMessage, bool>>? _filters;

    internal ReceiveBuilder(SurgewaveNativeClient client, string topic)
    {
        _client = client;
        _topic = topic;
    }

    /// <summary>
    /// Receive from a specific partition.
    /// </summary>
    public ReceiveBuilder FromPartition(int partition) { _partition = partition; _partitions = null; _allPartitions = false; return this; }

    /// <summary>
    /// Receive from multiple partitions.
    /// </summary>
    public ReceiveBuilder FromPartitions(params int[] partitions)
    {
        _partitions = partitions;
        _allPartitions = false;
        return this;
    }

    /// <summary>
    /// Receive from all partitions.
    /// </summary>
    public ReceiveBuilder FromAllPartitions()
    {
        _allPartitions = true;
        return this;
    }

    /// <summary>
    /// Set parallelism for multi-partition receive.
    /// </summary>
    public ReceiveBuilder WithParallelism(int parallelism)
    {
        _parallelism = Math.Max(1, parallelism);
        return this;
    }

    /// <summary>
    /// Start from a specific offset.
    /// </summary>
    public ReceiveBuilder FromOffset(long offset) { _offset = offset; _seekMode = SeekMode.Offset; return this; }

    /// <summary>
    /// Start from the beginning of the log.
    /// </summary>
    public ReceiveBuilder FromBeginning() { _offset = 0; _seekMode = SeekMode.Beginning; return this; }

    /// <summary>
    /// Start from the end of the log (newest messages only).
    /// </summary>
    public ReceiveBuilder FromEnd()
    {
        _seekMode = SeekMode.End;
        return this;
    }

    /// <summary>
    /// Start from a specific timestamp.
    /// </summary>
    public ReceiveBuilder Since(DateTimeOffset timestamp)
    {
        _seekTimestamp = timestamp;
        _seekMode = SeekMode.Timestamp;
        return this;
    }

    /// <summary>
    /// Set maximum bytes to receive.
    /// </summary>
    public ReceiveBuilder WithMaxBytes(int maxBytes) { _maxBytes = maxBytes; return this; }

    /// <summary>
    /// Limit the number of messages to receive.
    /// </summary>
    public ReceiveBuilder Take(int count)
    {
        _takeLimit = count;
        return this;
    }

    /// <summary>
    /// Filter messages (client-side).
    /// </summary>
    public ReceiveBuilder Where(Func<ReceivedMessage, bool> predicate)
    {
        _filters ??= new List<Func<ReceivedMessage, bool>>();
        _filters.Add(predicate);
        return this;
    }

    /// <summary>
    /// Filter by timestamp range.
    /// </summary>
    public ReceiveBuilder Between(DateTimeOffset? after = null, DateTimeOffset? before = null)
    {
        var afterMs = after?.ToUnixTimeMilliseconds();
        var beforeMs = before?.ToUnixTimeMilliseconds();
        return Where(m =>
            (!afterMs.HasValue || m.Timestamp >= afterMs.Value) &&
            (!beforeMs.HasValue || m.Timestamp <= beforeMs.Value));
    }

    /// <summary>
    /// Filter by key.
    /// </summary>
    public ReceiveBuilder WithKey(Func<byte[]?, bool> predicate)
        => Where(m => predicate(m.Key));

    /// <summary>
    /// Filter by string key.
    /// </summary>
    public ReceiveBuilder WithKey(string key)
        => Where(m => m.KeyString == key);

    private async Task<long> ResolveStartOffsetAsync(int partition, CancellationToken ct)
    {
        return _seekMode switch
        {
            SeekMode.Beginning => await _client.Messaging.GetEarliestOffsetAsync(_topic, partition, ct),
            SeekMode.End => await _client.Messaging.GetLatestOffsetAsync(_topic, partition, ct),
            SeekMode.Timestamp when _seekTimestamp.HasValue =>
                await _client.Messaging.GetOffsetForTimestampAsync(_topic, partition, _seekTimestamp.Value, ct),
            _ => _offset
        };
    }

    private bool PassesFilters(ReceivedMessage msg)
    {
        if (_filters == null) return true;
        foreach (var filter in _filters)
            if (!filter(msg)) return false;
        return true;
    }

    /// <summary>
    /// Execute and get messages as a list.
    /// </summary>
    public async Task<List<ReceivedMessage>> ToListAsync(CancellationToken cancellationToken = default)
    {
        var result = new List<ReceivedMessage>();
        await foreach (var msg in Stream(cancellationToken))
            result.Add(msg);
        return result;
    }

    /// <summary>
    /// Execute a single receive operation.
    /// </summary>
    public Task<ReceiveResult> ExecuteAsync(CancellationToken cancellationToken = default)
        => _client.Messaging.ReceiveAsync(_topic, _partition, _offset, _maxBytes, maxWaitMs: 5000, cancellationToken);

    /// <summary>
    /// Stream messages continuously.
    /// </summary>
    public async IAsyncEnumerable<ReceivedMessage> Stream(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        // Handle multi-partition receive
        if (_partitions != null || _allPartitions)
        {
            await foreach (var msg in ReceiveMultiPartitionAsync(cancellationToken))
                yield return msg;
            yield break;
        }

        var currentOffset = await ResolveStartOffsetAsync(_partition, cancellationToken);
        var count = 0;

        while (!cancellationToken.IsCancellationRequested)
        {
            var result = await _client.Messaging.ReceiveAsync(_topic, _partition, currentOffset, _maxBytes, maxWaitMs: 5000, cancellationToken);

            if (result.Messages.Count == 0)
                break;

            foreach (var msg in result.Messages)
            {
                currentOffset = msg.Offset + 1;

                if (!PassesFilters(msg))
                    continue;

                yield return msg;
                count++;

                if (_takeLimit.HasValue && count >= _takeLimit.Value)
                    yield break;
            }

            if (currentOffset >= result.HighWatermark)
                break;
        }
    }

    private async IAsyncEnumerable<ReceivedMessage> ReceiveMultiPartitionAsync(
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var partitions = _partitions ?? await GetAllPartitionsAsync(cancellationToken);
        var count = 0;

        var channel = System.Threading.Channels.Channel.CreateBounded<ReceivedMessage>(
            new System.Threading.Channels.BoundedChannelOptions(1000)
            {
                FullMode = System.Threading.Channels.BoundedChannelFullMode.Wait
            });

        var receiveTasks = partitions.Select(async partition =>
        {
            try
            {
                var offset = await ResolveStartOffsetAsync(partition, cancellationToken);
                while (!cancellationToken.IsCancellationRequested)
                {
                    var result = await _client.Messaging.ReceiveAsync(_topic, partition, offset, _maxBytes, maxWaitMs: 5000, cancellationToken);
                    if (result.Messages.Count == 0) break;

                    foreach (var msg in result.Messages)
                    {
                        offset = msg.Offset + 1;
                        if (PassesFilters(msg))
                            await channel.Writer.WriteAsync(msg, cancellationToken);
                    }

                    if (offset >= result.HighWatermark) break;
                }
            }
            catch (OperationCanceledException) { }
        }).ToArray();

        _ = Task.WhenAll(receiveTasks).ContinueWith(_ => channel.Writer.Complete(), cancellationToken, TaskContinuationOptions.None, TaskScheduler.Default);

        await foreach (var msg in channel.Reader.ReadAllAsync(cancellationToken))
        {
            yield return msg;
            count++;
            if (_takeLimit.HasValue && count >= _takeLimit.Value)
                yield break;
        }
    }

    private async Task<int[]> GetAllPartitionsAsync(CancellationToken ct)
    {
        var topics = await _client.Topics.ListAsync(ct);
        var topic = topics.FirstOrDefault(t => t.Name == _topic);
        if (topic == null) return [0];
        return Enumerable.Range(0, topic.PartitionCount).ToArray();
    }

    /// <summary>
    /// Stream messages as typed with deserialization.
    /// </summary>
    public async IAsyncEnumerable<TypedReceivedMessage<TKey, TValue>> Stream<TKey, TValue>(
        Kuestenlogik.Surgewave.Client.Serialization.IDeserializer<TKey>? keyDeserializer = null,
        Kuestenlogik.Surgewave.Client.Serialization.IDeserializer<TValue>? valueDeserializer = null,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var keyDeser = keyDeserializer ?? GetDefaultDeserializer<TKey>();
        var valueDeser = valueDeserializer ?? GetDefaultDeserializer<TValue>();

        await foreach (var msg in Stream(cancellationToken))
        {
            var key = msg.Key != null ? keyDeser.Deserialize(msg.Key, _topic) : default;
            var value = valueDeser.Deserialize(msg.Value, _topic);
            yield return new TypedReceivedMessage<TKey, TValue>(msg.Offset, msg.Timestamp, key, value);
        }
    }

    /// <summary>
    /// Transform and stream messages.
    /// </summary>
    public async IAsyncEnumerable<TResult> Select<TResult>(
        Func<ReceivedMessage, TResult> selector,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var msg in Stream(cancellationToken))
            yield return selector(msg);
    }

    private static Kuestenlogik.Surgewave.Client.Serialization.IDeserializer<T> GetDefaultDeserializer<T>()
    {
        var type = typeof(T);

        if (type == typeof(string))
            return (Kuestenlogik.Surgewave.Client.Serialization.IDeserializer<T>)(object)Kuestenlogik.Surgewave.Client.Serialization.Serializers.StringDeserializer;

        if (type == typeof(byte[]))
            return (Kuestenlogik.Surgewave.Client.Serialization.IDeserializer<T>)(object)Kuestenlogik.Surgewave.Client.Serialization.Serializers.ByteArrayDeserializer;

        if (type == typeof(int))
            return (Kuestenlogik.Surgewave.Client.Serialization.IDeserializer<T>)(object)Kuestenlogik.Surgewave.Client.Serialization.Serializers.Int32Deserializer;

        if (type == typeof(long))
            return (Kuestenlogik.Surgewave.Client.Serialization.IDeserializer<T>)(object)Kuestenlogik.Surgewave.Client.Serialization.Serializers.Int64Deserializer;

        if (type == typeof(Guid))
            return (Kuestenlogik.Surgewave.Client.Serialization.IDeserializer<T>)(object)Kuestenlogik.Surgewave.Client.Serialization.Serializers.GuidDeserializer;

        return Kuestenlogik.Surgewave.Client.Serialization.Serializers.JsonDeserializer<T>();
    }
}
