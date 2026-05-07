using System.Text;
using Kuestenlogik.Surgewave.Broker.Native;
using Kuestenlogik.Surgewave.Broker.Native.Coordination;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// Unit tests for subscription types (Exclusive, Shared, Failover, KeyShared).
/// Tests the SubscriptionManager independently and via NativeGroupCoordinator integration.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class SubscriptionTypeTests
{
    private readonly SubscriptionManager _manager = new();

    #region Exclusive Tests

    [Fact]
    public void Exclusive_SingleConsumer_Accepted()
    {
        var result = _manager.Subscribe("my-sub", SubscriptionType.Exclusive, "consumer-1");

        Assert.Equal(0, result.ErrorCode);
        Assert.True(result.IsActive);
        Assert.Null(result.ErrorMessage);
    }

    [Fact]
    public void Exclusive_SecondConsumer_Rejected()
    {
        _manager.Subscribe("my-sub", SubscriptionType.Exclusive, "consumer-1");

        var result = _manager.Subscribe("my-sub", SubscriptionType.Exclusive, "consumer-2");

        Assert.Equal(50, result.ErrorCode);
        Assert.False(result.IsActive);
        Assert.Contains("already bound", result.ErrorMessage);
    }

    [Fact]
    public void Exclusive_ConsumerLeaves_NewConsumerAccepted()
    {
        _manager.Subscribe("my-sub", SubscriptionType.Exclusive, "consumer-1");
        _manager.Unsubscribe("my-sub", "consumer-1");

        var result = _manager.Subscribe("my-sub", SubscriptionType.Exclusive, "consumer-2");

        Assert.Equal(0, result.ErrorCode);
        Assert.True(result.IsActive);
    }

    #endregion

    #region Shared Tests

    [Fact]
    public void Shared_MultipleConsumers_RoundRobinDispatch()
    {
        _manager.Subscribe("shared-sub", SubscriptionType.Shared, "consumer-1");
        _manager.Subscribe("shared-sub", SubscriptionType.Shared, "consumer-2");
        _manager.Subscribe("shared-sub", SubscriptionType.Shared, "consumer-3");

        // Round-robin should cycle through consumers
        var target1 = _manager.GetTargetConsumer("shared-sub", null);
        var target2 = _manager.GetTargetConsumer("shared-sub", null);
        var target3 = _manager.GetTargetConsumer("shared-sub", null);
        var target4 = _manager.GetTargetConsumer("shared-sub", null);

        Assert.Equal("consumer-1", target1);
        Assert.Equal("consumer-2", target2);
        Assert.Equal("consumer-3", target3);
        Assert.Equal("consumer-1", target4); // Wraps around
    }

    [Fact]
    public void Shared_ConsumerLeaves_SkippedInRotation()
    {
        _manager.Subscribe("shared-sub", SubscriptionType.Shared, "consumer-1");
        _manager.Subscribe("shared-sub", SubscriptionType.Shared, "consumer-2");
        _manager.Subscribe("shared-sub", SubscriptionType.Shared, "consumer-3");

        // Consume one message to advance counter to 1
        _manager.GetTargetConsumer("shared-sub", null); // consumer-1 (counter=0)

        // Remove consumer-2
        _manager.Unsubscribe("shared-sub", "consumer-2");

        // Now only consumer-1 and consumer-3 remain
        // Counter is at 1, 1 % 2 = 1 => consumer-3
        var target1 = _manager.GetTargetConsumer("shared-sub", null);
        // Counter is at 2, 2 % 2 = 0 => consumer-1
        var target2 = _manager.GetTargetConsumer("shared-sub", null);

        Assert.Equal("consumer-3", target1);
        Assert.Equal("consumer-1", target2);
    }

    #endregion

    #region Failover Tests

    [Fact]
    public void Failover_FirstConsumer_IsActive()
    {
        var result = _manager.Subscribe("failover-sub", SubscriptionType.Failover, "consumer-1");

        Assert.Equal(0, result.ErrorCode);
        Assert.True(result.IsActive);

        var target = _manager.GetTargetConsumer("failover-sub", null);
        Assert.Equal("consumer-1", target);
    }

    [Fact]
    public void Failover_ActiveDies_StandbyPromoted()
    {
        _manager.Subscribe("failover-sub", SubscriptionType.Failover, "consumer-1");
        var result2 = _manager.Subscribe("failover-sub", SubscriptionType.Failover, "consumer-2");
        Assert.False(result2.IsActive); // consumer-2 is standby

        // Active consumer dies
        _manager.HandleConsumerFailure("consumer-1");

        // consumer-2 should be promoted
        var target = _manager.GetTargetConsumer("failover-sub", null);
        Assert.Equal("consumer-2", target);

        var info = _manager.DescribeSubscription("failover-sub");
        Assert.NotNull(info);
        Assert.Equal("consumer-2", info.ActiveConsumer);
    }

    [Fact]
    public void Failover_MultipleStandby_FirstPromoted()
    {
        _manager.Subscribe("failover-sub", SubscriptionType.Failover, "consumer-1");
        _manager.Subscribe("failover-sub", SubscriptionType.Failover, "consumer-2");
        _manager.Subscribe("failover-sub", SubscriptionType.Failover, "consumer-3");

        // Kill active
        _manager.HandleConsumerFailure("consumer-1");

        // consumer-2 should be promoted (first in standby list)
        var target = _manager.GetTargetConsumer("failover-sub", null);
        Assert.Equal("consumer-2", target);

        // Kill the newly active
        _manager.HandleConsumerFailure("consumer-2");

        // consumer-3 should be promoted
        target = _manager.GetTargetConsumer("failover-sub", null);
        Assert.Equal("consumer-3", target);
    }

    #endregion

    #region KeyShared Tests

    [Fact]
    public void KeyShared_SameKey_SameConsumer()
    {
        _manager.Subscribe("ks-sub", SubscriptionType.KeyShared, "consumer-1");
        _manager.Subscribe("ks-sub", SubscriptionType.KeyShared, "consumer-2");

        var key = Encoding.UTF8.GetBytes("order-12345");

        // Same key should always map to the same consumer
        var target1 = _manager.GetTargetConsumer("ks-sub", key);
        var target2 = _manager.GetTargetConsumer("ks-sub", key);
        var target3 = _manager.GetTargetConsumer("ks-sub", key);

        Assert.Equal(target1, target2);
        Assert.Equal(target2, target3);
    }

    [Fact]
    public void KeyShared_DifferentKeys_Distributed()
    {
        _manager.Subscribe("ks-sub", SubscriptionType.KeyShared, "consumer-1");
        _manager.Subscribe("ks-sub", SubscriptionType.KeyShared, "consumer-2");

        // Generate many different keys and verify they distribute across consumers
        var consumerHits = new Dictionary<string, int>();
        for (var i = 0; i < 1000; i++)
        {
            var key = Encoding.UTF8.GetBytes($"key-{i}");
            var target = _manager.GetTargetConsumer("ks-sub", key);
            Assert.NotNull(target);

            if (!consumerHits.TryGetValue(target!, out var count))
                count = 0;
            consumerHits[target!] = count + 1;
        }

        // Both consumers should receive some messages (not all to one)
        Assert.Equal(2, consumerHits.Count);
        Assert.True(consumerHits["consumer-1"] > 100, $"consumer-1 got {consumerHits["consumer-1"]} messages, expected > 100");
        Assert.True(consumerHits["consumer-2"] > 100, $"consumer-2 got {consumerHits["consumer-2"]} messages, expected > 100");
    }

    [Fact]
    public void KeyShared_ConsumerLeaves_HashRangesRedistributed()
    {
        _manager.Subscribe("ks-sub", SubscriptionType.KeyShared, "consumer-1");
        _manager.Subscribe("ks-sub", SubscriptionType.KeyShared, "consumer-2");
        _manager.Subscribe("ks-sub", SubscriptionType.KeyShared, "consumer-3");

        // Remove middle consumer
        _manager.Unsubscribe("ks-sub", "consumer-2");

        var info = _manager.DescribeSubscription("ks-sub");
        Assert.NotNull(info);
        Assert.Equal(2, info.ConsumerCount);
        Assert.DoesNotContain("consumer-2", info.Consumers);

        // All keys should still be routable
        for (var i = 0; i < 100; i++)
        {
            var key = Encoding.UTF8.GetBytes($"key-{i}");
            var target = _manager.GetTargetConsumer("ks-sub", key);
            Assert.NotNull(target);
            Assert.True(target == "consumer-1" || target == "consumer-3",
                $"Expected consumer-1 or consumer-3, got {target}");
        }
    }

    #endregion

    #region Standard Tests

    [Fact]
    public void Standard_BehavesLikeNormalGroup()
    {
        // Standard subscription type should allow multiple consumers
        var result1 = _manager.Subscribe("std-sub", SubscriptionType.Standard, "consumer-1");
        var result2 = _manager.Subscribe("std-sub", SubscriptionType.Standard, "consumer-2");

        Assert.Equal(0, result1.ErrorCode);
        Assert.True(result1.IsActive);
        Assert.Equal(0, result2.ErrorCode);
        Assert.True(result2.IsActive);

        // GetTargetConsumer returns null for Standard (use normal partition assignment)
        var target = _manager.GetTargetConsumer("std-sub", null);
        Assert.Null(target);
    }

    #endregion

    #region Subscription Lifecycle Tests

    [Fact]
    public void DescribeSubscription_ReturnsCorrectInfo()
    {
        _manager.Subscribe("desc-sub", SubscriptionType.Failover, "consumer-1");
        _manager.Subscribe("desc-sub", SubscriptionType.Failover, "consumer-2");

        var info = _manager.DescribeSubscription("desc-sub");

        Assert.NotNull(info);
        Assert.Equal("desc-sub", info.Name);
        Assert.Equal(SubscriptionType.Failover, info.Type);
        Assert.Equal(2, info.ConsumerCount);
        Assert.Equal("consumer-1", info.ActiveConsumer);
        Assert.Equal(["consumer-1", "consumer-2"], info.Consumers);
    }

    [Fact]
    public void DescribeSubscription_UnknownSubscription_ReturnsNull()
    {
        var info = _manager.DescribeSubscription("nonexistent");
        Assert.Null(info);
    }

    [Fact]
    public void ListSubscriptions_ReturnsAll()
    {
        _manager.Subscribe("sub-a", SubscriptionType.Exclusive, "consumer-1");
        _manager.Subscribe("sub-b", SubscriptionType.Shared, "consumer-2");

        var list = _manager.ListSubscriptions();

        Assert.Equal(2, list.Count);
        Assert.Contains(list, s => s.Name == "sub-a" && s.Type == SubscriptionType.Exclusive);
        Assert.Contains(list, s => s.Name == "sub-b" && s.Type == SubscriptionType.Shared);
    }

    [Fact]
    public void Subscribe_TypeMismatch_Rejected()
    {
        _manager.Subscribe("typed-sub", SubscriptionType.Exclusive, "consumer-1");

        // Try to subscribe with a different type
        var result = _manager.Subscribe("typed-sub", SubscriptionType.Shared, "consumer-2");

        Assert.Equal(51, result.ErrorCode);
        Assert.False(result.IsActive);
        Assert.Contains("already exists with type", result.ErrorMessage);
    }

    [Fact]
    public void Subscribe_DuplicateConsumer_Idempotent()
    {
        _manager.Subscribe("dup-sub", SubscriptionType.Shared, "consumer-1");

        // Re-subscribe same consumer
        var result = _manager.Subscribe("dup-sub", SubscriptionType.Shared, "consumer-1");

        Assert.Equal(0, result.ErrorCode);
        Assert.True(result.IsActive);

        // Should still only have one consumer
        var info = _manager.DescribeSubscription("dup-sub");
        Assert.NotNull(info);
        Assert.Equal(1, info.ConsumerCount);
    }

    [Fact]
    public void Unsubscribe_LastConsumer_RemovesSubscription()
    {
        _manager.Subscribe("cleanup-sub", SubscriptionType.Shared, "consumer-1");
        _manager.Unsubscribe("cleanup-sub", "consumer-1");

        var info = _manager.DescribeSubscription("cleanup-sub");
        Assert.Null(info);
    }

    [Fact]
    public void GetTargetConsumer_UnknownSubscription_ReturnsNull()
    {
        var target = _manager.GetTargetConsumer("nonexistent", null);
        Assert.Null(target);
    }

    #endregion

    #region NativeGroupCoordinator Integration Tests

    [Fact]
    public void Coordinator_ExclusiveSubscription_RejectsSecondJoin()
    {
        var coordinator = new NativeGroupCoordinator(NullLogger<NativeGroupCoordinator>.Instance);
        var protocols = new List<GroupProtocol> { new("range", []) };

        var result1 = coordinator.JoinGroup(
            "exclusive-group", null, null, "client-1", "consumer",
            10000, 30000, protocols, SubscriptionType.Exclusive);

        Assert.Equal(0, result1.ErrorCode);

        var result2 = coordinator.JoinGroup(
            "exclusive-group", null, null, "client-2", "consumer",
            10000, 30000, protocols, SubscriptionType.Exclusive);

        Assert.Equal(50, result2.ErrorCode);
    }

    [Fact]
    public void Coordinator_StandardSubscription_FallsThrough()
    {
        var coordinator = new NativeGroupCoordinator(NullLogger<NativeGroupCoordinator>.Instance);
        var protocols = new List<GroupProtocol> { new("range", []) };

        var result = coordinator.JoinGroup(
            "standard-group", null, null, "client-1", "consumer",
            10000, 30000, protocols, SubscriptionType.Standard);

        Assert.Equal(0, result.ErrorCode);
        Assert.NotEmpty(result.MemberId);
    }

    [Fact]
    public void Coordinator_HandleConsumerDisconnect_TriggersFailover()
    {
        var coordinator = new NativeGroupCoordinator(NullLogger<NativeGroupCoordinator>.Instance);
        var sm = coordinator.SubscriptionManager;

        sm.Subscribe("failover-group", SubscriptionType.Failover, "member-1");
        sm.Subscribe("failover-group", SubscriptionType.Failover, "member-2");

        Assert.Equal("member-1", sm.GetTargetConsumer("failover-group", null));

        coordinator.HandleConsumerDisconnect("member-1");

        Assert.Equal("member-2", sm.GetTargetConsumer("failover-group", null));
    }

    #endregion

    #region HashSlot Tests

    [Fact]
    public void ComputeHashSlot_Deterministic()
    {
        var key = Encoding.UTF8.GetBytes("test-key");
        var slot1 = SubscriptionManager.ComputeHashSlot(key);
        var slot2 = SubscriptionManager.ComputeHashSlot(key);

        Assert.Equal(slot1, slot2);
        Assert.InRange(slot1, 0, 65535);
    }

    [Fact]
    public void ComputeHashSlot_DifferentKeys_DifferentSlots()
    {
        var slot1 = SubscriptionManager.ComputeHashSlot(Encoding.UTF8.GetBytes("key-a"));
        var slot2 = SubscriptionManager.ComputeHashSlot(Encoding.UTF8.GetBytes("key-b"));

        // Statistically extremely unlikely to collide
        Assert.NotEqual(slot1, slot2);
    }

    #endregion
}
