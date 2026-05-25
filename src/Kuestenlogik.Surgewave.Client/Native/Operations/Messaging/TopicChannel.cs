namespace Kuestenlogik.Surgewave.Client.Native;

/// <summary>
/// Bidirectional channel for sending and receiving messages on a topic.
/// </summary>
public sealed class TopicChannel
{
    private readonly SurgewaveMessagingOperations _messaging;
    private readonly string _topic;
    private readonly int _partition;
    private long _currentOffset;

    internal TopicChannel(SurgewaveMessagingOperations messaging, string topic, int partition)
    {
        _messaging = messaging;
        _topic = topic;
        _partition = partition;
    }

    /// <summary>
    /// Send a message to the channel.
    /// </summary>
    public Task<long> SendAsync(byte[] value, byte[]? key = null, CancellationToken ct = default)
        => _messaging.SendAsync(_topic, _partition, key, value, ct);

    /// <summary>
    /// Send a string message to the channel.
    /// </summary>
    public Task<long> SendAsync(string value, string? key = null, CancellationToken ct = default)
        => _messaging.SendAsync(_topic, _partition, key, value, ct);

    /// <summary>
    /// Receive the next batch of messages from the channel.
    /// </summary>
    public async Task<List<ReceivedMessage>> ReceiveAsync(int maxMessages = 100, CancellationToken ct = default)
    {
        var result = await _messaging.ReceiveAsync(_topic, _partition, _currentOffset, 1024 * 1024, maxWaitMs: 5000, ct);
        if (result.Messages.Count > 0)
            _currentOffset = result.Messages[^1].Offset + 1;
        return result.Messages.Take(maxMessages).ToList();
    }

    /// <summary>
    /// Stream messages from the channel continuously.
    /// </summary>
    public IAsyncEnumerable<ReceivedMessage> StreamAsync(CancellationToken ct = default)
        => _messaging.Receive(_topic).FromPartition(_partition).FromOffset(_currentOffset).Stream(ct);

    /// <summary>
    /// Seek to a specific position.
    /// </summary>
    public TopicChannel SeekTo(long offset) { _currentOffset = offset; return this; }

    /// <summary>
    /// Seek to the beginning.
    /// </summary>
    public async Task<TopicChannel> SeekToBeginningAsync(CancellationToken ct = default)
    {
        _currentOffset = await _messaging.GetEarliestOffsetAsync(_topic, _partition, ct);
        return this;
    }

    /// <summary>
    /// Seek to the end.
    /// </summary>
    public async Task<TopicChannel> SeekToEndAsync(CancellationToken ct = default)
    {
        _currentOffset = await _messaging.GetLatestOffsetAsync(_topic, _partition, ct);
        return this;
    }
}
