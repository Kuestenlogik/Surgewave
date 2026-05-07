namespace Kuestenlogik.Surgewave.Gateway.WebSocket.Subscriptions;

/// <summary>
/// Represents an active subscription to a topic.
/// </summary>
public sealed class Subscription : IAsyncDisposable
{
    private readonly CancellationTokenSource _cts;
    private Task? _streamingTask;
    private int _disposed;

    /// <summary>
    /// Unique subscription identifier.
    /// </summary>
    public string SubscriptionId { get; }

    /// <summary>
    /// Session ID that owns this subscription.
    /// </summary>
    public string SessionId { get; }

    /// <summary>
    /// Cluster ID this subscription is for.
    /// </summary>
    public string ClusterId { get; }

    /// <summary>
    /// Topic being subscribed to.
    /// </summary>
    public string Topic { get; }

    /// <summary>
    /// Partitions being consumed (null = all partitions).
    /// </summary>
    public int[]? Partitions { get; }

    /// <summary>
    /// Consumer group name if using consumer groups.
    /// </summary>
    public string? ConsumerGroup { get; }

    /// <summary>
    /// When the subscription was created.
    /// </summary>
    public DateTimeOffset CreatedAt { get; }

    /// <summary>
    /// Cancellation token for this subscription.
    /// </summary>
    public CancellationToken CancellationToken => _cts.Token;

    /// <summary>
    /// Whether the subscription is active.
    /// </summary>
    public bool IsActive => _streamingTask != null && !_streamingTask.IsCompleted && _disposed == 0;

    public Subscription(
        string sessionId,
        string clusterId,
        string topic,
        int[]? partitions,
        string? consumerGroup)
    {
        SubscriptionId = Guid.NewGuid().ToString("N")[..12];
        SessionId = sessionId;
        ClusterId = clusterId;
        Topic = topic;
        Partitions = partitions;
        ConsumerGroup = consumerGroup;
        CreatedAt = DateTimeOffset.UtcNow;
        _cts = new CancellationTokenSource();
    }

    /// <summary>
    /// Sets the streaming task for this subscription.
    /// </summary>
    internal void SetStreamingTask(Task task)
    {
        _streamingTask = task;
    }

    /// <summary>
    /// Cancels the subscription.
    /// </summary>
    public async Task CancelAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;

        await _cts.CancelAsync();

        if (_streamingTask != null)
        {
            try
            {
                await _streamingTask.WaitAsync(TimeSpan.FromSeconds(5));
            }
            catch (OperationCanceledException)
            {
                // Expected
            }
            catch (TimeoutException)
            {
                // Task didn't complete in time, continue anyway
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        await CancelAsync();
        _cts.Dispose();
    }
}
