using Kuestenlogik.Surgewave.Clustering.Bundles;
using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests;

[Trait("Category", TestCategories.Unit)]
public class TopicBundleTests
{
    private static ClusterState CreateClusterWithBrokers(params int[] brokerIds)
    {
        var state = new ClusterState();
        foreach (var id in brokerIds)
        {
            state.AddBroker(new BrokerNode { BrokerId = id, Host = "localhost", Port = 9092 + id });
        }
        return state;
    }

    private static BundleManager CreateManager(ClusterState? state = null, BundleConfig? config = null)
    {
        state ??= new ClusterState();
        return new BundleManager(
            NullLogger<BundleManager>.Instance,
            state,
            config);
    }

    [Fact]
    public void BundleManager_Initialize_CreatesEqualBundles()
    {
        var manager = CreateManager();
        manager.Initialize(4);

        var bundles = manager.AllBundles;

        Assert.Equal(4, bundles.Count);

        // Verify they cover the entire uint32 range without gaps
        Assert.Equal(0u, bundles[0].HashRangeStart);

        for (int i = 1; i < bundles.Count; i++)
        {
            // Each bundle starts where the previous one ended
            Assert.Equal(bundles[i - 1].HashRangeEnd, bundles[i].HashRangeStart);
        }

        // Last bundle should cover through the end (sentinel 0)
        Assert.Equal(0u, bundles[^1].HashRangeEnd);

        // All unassigned
        foreach (var bundle in bundles)
        {
            Assert.Equal(-1, bundle.OwnerBrokerId);
        }
    }

    [Fact]
    public void BundleManager_GetBundleForTopic_ConsistentMapping()
    {
        var manager = CreateManager();
        manager.Initialize(8);

        // Same topic should always map to same bundle
        var bundle1 = manager.GetBundleForTopic("orders");
        var bundle2 = manager.GetBundleForTopic("orders");

        Assert.NotNull(bundle1);
        Assert.NotNull(bundle2);
        Assert.Equal(bundle1.BundleId, bundle2.BundleId);

        // Different topics may map to different bundles (at least some should differ with 8 bundles)
        var topics = new[] { "orders", "users", "payments", "inventory", "logs", "metrics", "events", "alerts" };
        var mappedBundles = topics.Select(t => manager.GetBundleForTopic(t)!.BundleId).Distinct().ToList();

        // With 8 topics and 8 bundles, we expect some distribution (at least 2 different bundles)
        Assert.True(mappedBundles.Count >= 2,
            $"Expected topics to map to multiple bundles, got {mappedBundles.Count}");
    }

    [Fact]
    public void BundleManager_SplitBundle_CreatesTwoHalves()
    {
        var manager = CreateManager();
        manager.Initialize(2);

        var original = manager.AllBundles[0];
        string originalId = original.BundleId;
        uint originalStart = original.HashRangeStart;
        uint originalEnd = original.HashRangeEnd;

        var (lower, upper) = manager.SplitBundle(originalId);

        // Should now have 3 bundles (2 original - 1 split + 2 new = 3)
        Assert.Equal(3, manager.AllBundles.Count);

        // Lower starts at original start
        Assert.Equal(originalStart, lower.HashRangeStart);

        // Upper ends at original end
        Assert.Equal(originalEnd, upper.HashRangeEnd);

        // Lower end equals upper start (contiguous)
        Assert.Equal(lower.HashRangeEnd, upper.HashRangeStart);

        // Midpoint should be roughly in the middle
        Assert.True(lower.HashRangeEnd > originalStart);
    }

    [Fact]
    public void BundleManager_SplitBundle_TopicsMoveCorrectly()
    {
        var manager = CreateManager();
        manager.Initialize(1); // One big bundle

        var originalBundle = manager.AllBundles[0];
        string originalId = originalBundle.BundleId;

        // Get a topic that maps to this bundle
        string testTopic = "test-topic";
        var bundleBefore = manager.GetBundleForTopic(testTopic);
        Assert.NotNull(bundleBefore);
        Assert.Equal(originalId, bundleBefore.BundleId);

        // Split
        var (lower, upper) = manager.SplitBundle(originalId);

        // Topic should now map to one of the two halves
        var bundleAfter = manager.GetBundleForTopic(testTopic);
        Assert.NotNull(bundleAfter);
        Assert.True(
            bundleAfter.BundleId == lower.BundleId || bundleAfter.BundleId == upper.BundleId,
            "Topic should map to one of the split halves");

        // Verify the hash is actually in the chosen bundle's range
        uint hash = TopicBundle.HashTopic(testTopic);
        if (bundleAfter.HashRangeEnd == 0)
        {
            Assert.True(hash >= bundleAfter.HashRangeStart);
        }
        else
        {
            Assert.True(bundleAfter.ContainsHash(hash));
        }
    }

    [Fact]
    public void BundleManager_UnloadBundle_SetsOwnerToNegativeOne()
    {
        var manager = CreateManager();
        manager.Initialize(2);

        var bundle = manager.AllBundles[0];
        manager.AssignBundle(bundle.BundleId, 42);

        Assert.Equal(42, manager.AllBundles[0].OwnerBrokerId);

        manager.UnloadBundle(bundle.BundleId);

        Assert.Equal(-1, manager.AllBundles[0].OwnerBrokerId);
    }

    [Fact]
    public void BundleManager_AssignBundle_UpdatesOwner()
    {
        var manager = CreateManager();
        manager.Initialize(4);

        var bundle = manager.AllBundles[1];
        Assert.Equal(-1, bundle.OwnerBrokerId);

        manager.AssignBundle(bundle.BundleId, 3);

        // Re-read from AllBundles since it returns a snapshot
        Assert.Equal(3, manager.AllBundles[1].OwnerBrokerId);

        // Reassign to different broker
        manager.AssignBundle(bundle.BundleId, 7);
        Assert.Equal(7, manager.AllBundles[1].OwnerBrokerId);
    }

    [Fact]
    public void BundleRebalancer_FindBundlesToSplit_DetectsHotBundles()
    {
        var state = new ClusterState();

        // Add enough topics to exceed the threshold
        var config = new BundleConfig { MaxTopicsPerBundle = 2 };
        var manager = CreateManager(state, config);
        manager.Initialize(2);

        // Add 5 topics — at least one bundle should get > 2 topics
        for (int i = 0; i < 10; i++)
        {
            state.AddTopic(new TopicMetadata
            {
                Name = $"topic-{i}",
                TopicId = Guid.NewGuid(),
                PartitionCount = 1,
                ReplicationFactor = 1,
                Config = [],
                CreatedAt = DateTime.UtcNow
            });
        }

        var rebalancer = new BundleRebalancer(
            NullLogger<BundleRebalancer>.Instance,
            manager, state, config);

        var toSplit = rebalancer.FindBundlesToSplit();

        // At least one bundle should have > 2 topics and be flagged for split
        Assert.True(toSplit.Count > 0,
            "Should detect at least one hot bundle with > 2 topics");
    }

    [Fact]
    public void BundleRebalancer_GenerateTransferPlan_BalancesLoad()
    {
        var state = CreateClusterWithBrokers(0, 1, 2);
        var manager = CreateManager(state);
        manager.Initialize(6);

        // Assign all 6 bundles to broker 0 — heavily imbalanced
        foreach (var bundle in manager.AllBundles)
        {
            manager.AssignBundle(bundle.BundleId, 0);
        }

        var rebalancer = new BundleRebalancer(
            NullLogger<BundleRebalancer>.Instance,
            manager, state);

        var transfers = rebalancer.GenerateTransferPlan();

        // Should propose moving bundles away from broker 0
        Assert.True(transfers.Count > 0, "Should generate transfers to balance load");

        // All transfers should be FROM broker 0
        Assert.All(transfers, t => Assert.Equal(0, t.FromBrokerId));

        // Transfers should go to brokers 1 and/or 2
        Assert.All(transfers, t => Assert.True(t.ToBrokerId == 1 || t.ToBrokerId == 2));
    }

    [Fact]
    public void TopicBundle_HashRange_ContainsCorrectTopics()
    {
        // Create a bundle with a known range
        var bundle = new TopicBundle
        {
            BundleId = "test-bundle",
            HashRangeStart = 100,
            HashRangeEnd = 200
        };

        // Values in range
        Assert.True(bundle.ContainsHash(100)); // inclusive start
        Assert.True(bundle.ContainsHash(150));
        Assert.True(bundle.ContainsHash(199));

        // Values out of range
        Assert.False(bundle.ContainsHash(99));
        Assert.False(bundle.ContainsHash(200)); // exclusive end
        Assert.False(bundle.ContainsHash(201));
        Assert.False(bundle.ContainsHash(0));

        // HashTopic is deterministic
        uint hash1 = TopicBundle.HashTopic("my-topic");
        uint hash2 = TopicBundle.HashTopic("my-topic");
        Assert.Equal(hash1, hash2);

        // Different topics produce different hashes (extremely likely)
        uint hashA = TopicBundle.HashTopic("topic-a");
        uint hashB = TopicBundle.HashTopic("topic-b");
        Assert.NotEqual(hashA, hashB);
    }

    [Fact]
    public void BundleConfig_DefaultValues_Correct()
    {
        var config = new BundleConfig();

        Assert.Equal(4, config.InitialBundleCount);
        Assert.Equal(128, config.MaxBundlesPerNamespace);
        Assert.Equal(1000, config.MaxTopicsPerBundle);
        Assert.Equal(30_000, config.MaxMessageRatePerBundle);
        Assert.Equal(100, config.MaxBandwidthMbPerBundle);
        Assert.True(config.AutoSplitEnabled);
        Assert.True(config.AutoUnloadAfterSplit);
    }

    [Fact]
    public void BundleManager_GetBundlesForBroker_ReturnsCorrectBundles()
    {
        var manager = CreateManager();
        manager.Initialize(4);

        manager.AssignBundle(manager.AllBundles[0].BundleId, 1);
        manager.AssignBundle(manager.AllBundles[1].BundleId, 1);
        manager.AssignBundle(manager.AllBundles[2].BundleId, 2);

        var broker1Bundles = manager.GetBundlesForBroker(1);
        var broker2Bundles = manager.GetBundlesForBroker(2);
        var unassigned = manager.GetBundlesForBroker(-1);

        Assert.Equal(2, broker1Bundles.Count);
        Assert.Single(broker2Bundles);
        Assert.Single(unassigned); // 4th bundle still unassigned
    }

    [Fact]
    public void BundleManager_SplitBundle_InheritsOwner()
    {
        var manager = CreateManager();
        manager.Initialize(2);

        var bundle = manager.AllBundles[0];
        manager.AssignBundle(bundle.BundleId, 5);

        var (lower, upper) = manager.SplitBundle(bundle.BundleId);

        Assert.Equal(5, lower.OwnerBrokerId);
        Assert.Equal(5, upper.OwnerBrokerId);
    }

    [Fact]
    public void BundleManager_SplitBundle_RespectsMaxLimit()
    {
        var config = new BundleConfig { MaxBundlesPerNamespace = 4 };
        var manager = CreateManager(config: config);
        manager.Initialize(4);

        // Already at max — split should throw
        var ex = Assert.Throws<InvalidOperationException>(
            () => manager.SplitBundle(manager.AllBundles[0].BundleId));

        Assert.Contains("maximum bundle count", ex.Message);
    }

    [Fact]
    public void BundleRebalancer_MaybeRebalanceBundles_SplitsAndRedistributes()
    {
        var state = CreateClusterWithBrokers(0, 1);

        var config = new BundleConfig
        {
            MaxTopicsPerBundle = 1,
            AutoSplitEnabled = true,
            AutoUnloadAfterSplit = true,
            MaxBundlesPerNamespace = 128
        };
        var manager = CreateManager(state, config);
        manager.Initialize(2);

        // Assign both bundles to broker 0
        foreach (var b in manager.AllBundles)
        {
            manager.AssignBundle(b.BundleId, 0);
        }

        // Add topics to trigger splits
        for (int i = 0; i < 4; i++)
        {
            state.AddTopic(new TopicMetadata
            {
                Name = $"topic-{i}",
                TopicId = Guid.NewGuid(),
                PartitionCount = 1,
                ReplicationFactor = 1,
                Config = [],
                CreatedAt = DateTime.UtcNow
            });
        }

        var rebalancer = new BundleRebalancer(
            NullLogger<BundleRebalancer>.Instance,
            manager, state, config);

        rebalancer.MaybeRebalanceBundles();

        // After split + unload, bundles should have been split (more than 2 now)
        Assert.True(manager.AllBundles.Count > 2,
            "Bundles should have been split");
    }

    [Fact]
    public void BundleManager_GetLoadReport_CountsTopicsCorrectly()
    {
        var state = new ClusterState();
        var manager = CreateManager(state);
        manager.Initialize(2);

        // Add some topics
        for (int i = 0; i < 5; i++)
        {
            state.AddTopic(new TopicMetadata
            {
                Name = $"topic-{i}",
                TopicId = Guid.NewGuid(),
                PartitionCount = 1,
                ReplicationFactor = 1,
                Config = [],
                CreatedAt = DateTime.UtcNow
            });
        }

        var report = manager.GetLoadReport();

        Assert.Equal(2, report.Bundles.Count);

        // Total topic count across all bundles should equal 5
        int totalTopics = report.Bundles.Sum(b => b.TopicCount);
        Assert.Equal(5, totalTopics);

        // Should identify hottest bundle
        Assert.NotNull(report.HottestBundleId);
    }

    [Fact]
    public void BundleManager_AssignBundle_ThrowsForUnknownBundle()
    {
        var manager = CreateManager();
        manager.Initialize(2);

        Assert.Throws<InvalidOperationException>(
            () => manager.AssignBundle("nonexistent-bundle", 1));
    }

    [Fact]
    public void BundleManager_UnloadBundle_ThrowsForUnknownBundle()
    {
        var manager = CreateManager();
        manager.Initialize(2);

        Assert.Throws<InvalidOperationException>(
            () => manager.UnloadBundle("nonexistent-bundle"));
    }
}
