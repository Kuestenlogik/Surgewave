using System.IO.Hashing;
using System.Runtime.CompilerServices;

namespace Kuestenlogik.Surgewave.Broker.Native.Coordination;

/// <summary>
/// Manages subscription types (Exclusive, Shared, Failover, KeyShared) for consumer groups.
/// Thread-safe with simple lock-based synchronization matching the NativeGroupCoordinator pattern.
/// </summary>
public sealed class SubscriptionManager
{
    /// <summary>
    /// Total number of virtual hash slots for KeyShared consistent hashing.
    /// </summary>
    private const int HashSlotCount = 65536;

    private readonly Dictionary<string, SubscriptionState> _subscriptions = [];
    private readonly Lock _lock = new();

    /// <summary>
    /// Subscribe a consumer to a named subscription with the given type.
    /// For Standard: always succeeds (no-op tracking).
    /// For Exclusive: rejects if another consumer is already subscribed.
    /// For Shared: adds consumer to round-robin rotation.
    /// For Failover: first consumer is active, others are standby.
    /// For KeyShared: adds consumer and redistributes hash ring.
    /// </summary>
    public SubscriptionResult Subscribe(string subscriptionName, SubscriptionType type, string consumerId)
    {
        lock (_lock)
        {
            if (!_subscriptions.TryGetValue(subscriptionName, out var state))
            {
                state = new SubscriptionState { Name = subscriptionName, Type = type };
                _subscriptions[subscriptionName] = state;
            }
            else if (state.Type != type)
            {
                return new SubscriptionResult
                {
                    ErrorCode = 51,
                    ErrorMessage = $"Subscription '{subscriptionName}' already exists with type {state.Type}, cannot change to {type}.",
                    IsActive = false
                };
            }

            // Prevent duplicate consumer registration
            if (state.Consumers.Contains(consumerId))
            {
                return CreateSuccessResult(state, consumerId);
            }

            return type switch
            {
                SubscriptionType.Standard => SubscribeStandard(state, consumerId),
                SubscriptionType.Exclusive => SubscribeExclusive(state, consumerId),
                SubscriptionType.Shared => SubscribeShared(state, consumerId),
                SubscriptionType.Failover => SubscribeFailover(state, consumerId),
                SubscriptionType.KeyShared => SubscribeKeyShared(state, consumerId),
                _ => new SubscriptionResult
                {
                    ErrorCode = 52,
                    ErrorMessage = $"Unknown subscription type: {type}.",
                    IsActive = false
                }
            };
        }
    }

    /// <summary>
    /// Remove a consumer from a subscription.
    /// For Exclusive: frees the slot.
    /// For Failover: promotes next standby if the active consumer leaves.
    /// For KeyShared: redistributes hash ranges.
    /// </summary>
    public void Unsubscribe(string subscriptionName, string consumerId)
    {
        lock (_lock)
        {
            if (!_subscriptions.TryGetValue(subscriptionName, out var state))
                return;

            if (!state.Consumers.Remove(consumerId))
                return;

            switch (state.Type)
            {
                case SubscriptionType.Failover:
                    if (state.ActiveConsumer == consumerId)
                    {
                        // Promote next standby
                        state.ActiveConsumer = state.Consumers.Count > 0 ? state.Consumers[0] : null;
                    }
                    break;

                case SubscriptionType.KeyShared:
                    state.HashRanges.Remove(consumerId);
                    RedistributeHashRanges(state);
                    break;
            }

            // Clean up empty subscriptions
            if (state.Consumers.Count == 0)
            {
                _subscriptions.Remove(subscriptionName);
            }
        }
    }

    /// <summary>
    /// Determine which consumer should receive a message based on subscription type and optional message key.
    /// For Standard: returns null (use normal partition assignment).
    /// For Exclusive: returns the single consumer.
    /// For Shared: returns next consumer via round-robin.
    /// For Failover: returns the active consumer.
    /// For KeyShared: returns the consumer mapped to the key's hash slot.
    /// </summary>
    public string? GetTargetConsumer(string subscriptionName, byte[]? messageKey)
    {
        lock (_lock)
        {
            if (!_subscriptions.TryGetValue(subscriptionName, out var state))
                return null;

            if (state.Consumers.Count == 0)
                return null;

            return state.Type switch
            {
                SubscriptionType.Standard => null,
                SubscriptionType.Exclusive => state.Consumers[0],
                SubscriptionType.Shared => GetSharedTarget(state),
                SubscriptionType.Failover => state.ActiveConsumer,
                SubscriptionType.KeyShared => GetKeySharedTarget(state, messageKey),
                _ => null
            };
        }
    }

    /// <summary>
    /// Handle a consumer failure by removing it from all subscriptions.
    /// Triggers failover promotion where applicable.
    /// </summary>
    public void HandleConsumerFailure(string consumerId)
    {
        lock (_lock)
        {
            // Collect subscription names first to avoid modifying during iteration
            var subscriptionNames = _subscriptions.Keys.ToList();

            foreach (var name in subscriptionNames)
            {
                if (!_subscriptions.TryGetValue(name, out var state))
                    continue;

                if (!state.Consumers.Contains(consumerId))
                    continue;

                state.Consumers.Remove(consumerId);

                switch (state.Type)
                {
                    case SubscriptionType.Failover:
                        if (state.ActiveConsumer == consumerId)
                        {
                            state.ActiveConsumer = state.Consumers.Count > 0 ? state.Consumers[0] : null;
                        }
                        break;

                    case SubscriptionType.KeyShared:
                        state.HashRanges.Remove(consumerId);
                        RedistributeHashRanges(state);
                        break;
                }

                if (state.Consumers.Count == 0)
                {
                    _subscriptions.Remove(name);
                }
            }
        }
    }

    /// <summary>
    /// Get detailed information about a subscription.
    /// </summary>
    public SubscriptionInfo? DescribeSubscription(string subscriptionName)
    {
        lock (_lock)
        {
            if (!_subscriptions.TryGetValue(subscriptionName, out var state))
                return null;

            return ToInfo(state);
        }
    }

    /// <summary>
    /// List all active subscriptions.
    /// </summary>
    public List<SubscriptionInfo> ListSubscriptions()
    {
        lock (_lock)
        {
            return _subscriptions.Values.Select(ToInfo).ToList();
        }
    }

    private static SubscriptionResult SubscribeStandard(SubscriptionState state, string consumerId)
    {
        state.Consumers.Add(consumerId);
        return new SubscriptionResult { ErrorCode = 0, IsActive = true };
    }

    private static SubscriptionResult SubscribeExclusive(SubscriptionState state, string consumerId)
    {
        if (state.Consumers.Count > 0)
        {
            return new SubscriptionResult
            {
                ErrorCode = 50,
                ErrorMessage = $"Exclusive subscription '{state.Name}' is already bound to consumer '{state.Consumers[0]}'.",
                IsActive = false
            };
        }

        state.Consumers.Add(consumerId);
        return new SubscriptionResult { ErrorCode = 0, IsActive = true };
    }

    private static SubscriptionResult SubscribeShared(SubscriptionState state, string consumerId)
    {
        state.Consumers.Add(consumerId);
        return new SubscriptionResult { ErrorCode = 0, IsActive = true };
    }

    private static SubscriptionResult SubscribeFailover(SubscriptionState state, string consumerId)
    {
        state.Consumers.Add(consumerId);

        if (state.ActiveConsumer is null)
        {
            // First consumer becomes active
            state.ActiveConsumer = consumerId;
            return new SubscriptionResult { ErrorCode = 0, IsActive = true };
        }

        // Subsequent consumers are standby
        return new SubscriptionResult { ErrorCode = 0, IsActive = false };
    }

    private static SubscriptionResult SubscribeKeyShared(SubscriptionState state, string consumerId)
    {
        state.Consumers.Add(consumerId);
        RedistributeHashRanges(state);
        return new SubscriptionResult { ErrorCode = 0, IsActive = true };
    }

    private static string GetSharedTarget(SubscriptionState state)
    {
        var index = (int)(state.DispatchCounter % state.Consumers.Count);
        state.DispatchCounter++;
        return state.Consumers[index];
    }

    private static string? GetKeySharedTarget(SubscriptionState state, byte[]? messageKey)
    {
        if (state.Consumers.Count == 0)
            return null;

        if (messageKey is null || messageKey.Length == 0)
        {
            // No key — fall back to round-robin like Shared
            return GetSharedTarget(state);
        }

        var hashSlot = ComputeHashSlot(messageKey);

        foreach (var (consumer, (start, end)) in state.HashRanges)
        {
            if (hashSlot >= start && hashSlot <= end)
                return consumer;
        }

        // Fallback: shouldn't happen if ranges are properly distributed
        return state.Consumers[0];
    }

    /// <summary>
    /// Compute a hash slot (0..65535) from a message key using XxHash32.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int ComputeHashSlot(byte[] key)
    {
        var hash = XxHash32.HashToUInt32(key);
        return (int)(hash % HashSlotCount);
    }

    /// <summary>
    /// Evenly distribute hash ranges across all consumers in a KeyShared subscription.
    /// Each consumer gets a contiguous range of slots.
    /// </summary>
    private static void RedistributeHashRanges(SubscriptionState state)
    {
        state.HashRanges.Clear();

        if (state.Consumers.Count == 0)
            return;

        var consumerCount = state.Consumers.Count;
        var slotsPerConsumer = HashSlotCount / consumerCount;
        var remainder = HashSlotCount % consumerCount;

        var start = 0;
        for (var i = 0; i < consumerCount; i++)
        {
            // Distribute remainder slots to first consumers (one extra each)
            var rangeSize = slotsPerConsumer + (i < remainder ? 1 : 0);
            var end = start + rangeSize - 1;
            state.HashRanges[state.Consumers[i]] = (start, end);
            start = end + 1;
        }
    }

    private static SubscriptionResult CreateSuccessResult(SubscriptionState state, string consumerId)
    {
        var isActive = state.Type switch
        {
            SubscriptionType.Failover => state.ActiveConsumer == consumerId,
            _ => true
        };

        return new SubscriptionResult { ErrorCode = 0, IsActive = isActive };
    }

    private static SubscriptionInfo ToInfo(SubscriptionState state) => new()
    {
        Name = state.Name,
        Type = state.Type,
        ConsumerCount = state.Consumers.Count,
        ActiveConsumer = state.Type == SubscriptionType.Failover ? state.ActiveConsumer : null,
        Consumers = [.. state.Consumers]
    };
}
