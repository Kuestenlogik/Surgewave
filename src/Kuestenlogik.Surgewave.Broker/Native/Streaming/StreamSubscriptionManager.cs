using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Core.Storage;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Native.Streaming;

/// <summary>
/// Manages all active push subscriptions for a single client connection.
/// Thread-safe. Enforces a maximum number of concurrent subscriptions.
/// </summary>
public sealed class StreamSubscriptionManager : IAsyncDisposable
{
    private readonly ConcurrentDictionary<string, StreamSubscription> _subscriptions = new(StringComparer.Ordinal);
    private readonly LogManager _logManager;
    private readonly RecordBatchSerializer _recordBatchSerializer;
    private readonly ILogger _logger;
    private readonly int _maxSubscriptions;

    /// <summary>Gets the number of active subscriptions for this connection.</summary>
    public int ActiveCount => _subscriptions.Count;

    public StreamSubscriptionManager(
        LogManager logManager,
        RecordBatchSerializer recordBatchSerializer,
        ILogger logger,
        int maxSubscriptions = 100)
    {
        _logManager = logManager;
        _recordBatchSerializer = recordBatchSerializer;
        _logger = logger;
        _maxSubscriptions = maxSubscriptions;
    }

    /// <summary>
    /// Creates and starts a push subscription for a topic and set of partitions.
    /// </summary>
    /// <param name="subscriptionId">Unique subscription identifier.</param>
    /// <param name="topic">Topic to subscribe to.</param>
    /// <param name="partitions">Partitions to subscribe to.</param>
    /// <param name="initialOffsets">Starting offset for each partition (must be same length as partitions).</param>
    /// <param name="maxBytesPerPush">Maximum bytes per push batch per partition.</param>
    /// <param name="sendDelegate">Callback to send push frames to the client.</param>
    /// <returns>True on success; false if the max subscription limit is reached or the ID is a duplicate.</returns>
    public bool Subscribe(
        string subscriptionId,
        string topic,
        int[] partitions,
        long[] initialOffsets,
        int maxBytesPerPush,
        Func<string, int, long, int, ReadOnlyMemory<byte>, CancellationToken, Task> sendDelegate)
    {
        if (_subscriptions.Count >= _maxSubscriptions)
        {
            _logger.LogWarning(
                "Cannot create subscription {SubscriptionId}: max subscriptions ({Max}) reached",
                subscriptionId, _maxSubscriptions);
            return false;
        }

        var subscription = new StreamSubscription(
            subscriptionId,
            topic,
            partitions,
            initialOffsets,
            maxBytesPerPush,
            _logManager,
            _recordBatchSerializer,
            _logger);

        if (!_subscriptions.TryAdd(subscriptionId, subscription))
        {
            _logger.LogWarning("Duplicate subscription ID: {SubscriptionId}", subscriptionId);
            // Dispose the newly created subscription since we won't use it
            _ = subscription.DisposeAsync().AsTask();
            return false;
        }

        subscription.StartAsync(sendDelegate);

        _logger.LogDebug(
            "Started subscription {SubscriptionId} for topic {Topic} on {PartitionCount} partition(s)",
            subscriptionId, topic, partitions.Length);

        return true;
    }

    /// <summary>
    /// Stops and removes a subscription by ID.
    /// </summary>
    /// <returns>True if the subscription was found and stopped; false if not found.</returns>
    public async Task<bool> UnsubscribeAsync(string subscriptionId)
    {
        if (!_subscriptions.TryRemove(subscriptionId, out var subscription))
        {
            _logger.LogDebug("Subscription {SubscriptionId} not found for unsubscribe", subscriptionId);
            return false;
        }

        await subscription.StopAsync();
        await subscription.DisposeAsync();

        _logger.LogDebug("Stopped subscription {SubscriptionId}", subscriptionId);
        return true;
    }

    /// <summary>
    /// Adds credit bytes to a subscription for flow control (called on StreamAck).
    /// </summary>
    public bool AddCredit(string subscriptionId, long creditBytes)
    {
        if (!_subscriptions.TryGetValue(subscriptionId, out var subscription))
            return false;

        subscription.AddCredit(creditBytes);
        return true;
    }

    /// <summary>
    /// Stops and removes all subscriptions. Called on connection close.
    /// </summary>
    public async Task UnsubscribeAllAsync()
    {
        var subscriptions = _subscriptions.Values.ToArray();
        _subscriptions.Clear();

        var stopTasks = subscriptions.Select(async s =>
        {
            try
            {
                await s.StopAsync();
                await s.DisposeAsync();
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "Error stopping subscription {SubscriptionId} during cleanup", s.SubscriptionId);
            }
        });

        await Task.WhenAll(stopTasks);

        if (subscriptions.Length > 0)
        {
            _logger.LogDebug("Unsubscribed all {Count} subscriptions on connection close", subscriptions.Length);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await UnsubscribeAllAsync();
    }
}
