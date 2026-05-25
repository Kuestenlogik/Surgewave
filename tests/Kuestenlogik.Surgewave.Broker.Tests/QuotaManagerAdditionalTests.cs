using Kuestenlogik.Surgewave.Broker.AutoTuning;
using Kuestenlogik.Surgewave.Broker.CruiseControl;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// Additional tests for QuotaManager (token-bucket), AutoTuning edge cases,
/// and CruiseControl subsystem helpers not covered in existing tests.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class QuotaManagerAdditionalTests
{
    // ────────────────────────── QuotaManager ──────────────────────────────────

    [Fact]
    public void QuotaConfig_Defaults_AreCorrect()
    {
        var config = new QuotaConfig();

        Assert.False(config.Enabled);
        Assert.Equal(-1, config.ProducerBytesPerSecond);
        Assert.Equal(-1, config.ConsumerBytesPerSecond);
        Assert.Equal(30_000, config.MaxThrottleTimeMs);
        Assert.Equal(10, config.ClientInactivityTimeoutMinutes);
        Assert.Equal(104_857_600L, config.ProducerBurstBytes); // 100 MB
        Assert.Equal(104_857_600L, config.ConsumerBurstBytes);
    }

    [Fact]
    public void QuotaConfig_StaticDefault_IsDisabled()
    {
        var config = QuotaConfig.Default;
        Assert.False(config.Enabled);
    }

    [Fact]
    public void QuotaConfig_StaticDisabled_IsDisabled()
    {
        var config = QuotaConfig.Disabled;
        Assert.False(config.Enabled);
    }

    [Fact]
    public void QuotaManager_ProduceThrottle_WhenOverLimit()
    {
        var config = new QuotaConfig
        {
            Enabled = true,
            ProducerBytesPerSecond = 1_000,      // 1 KB/s
            ProducerBurstBytes = 1_000,           // 1 KB burst
            MaxThrottleTimeMs = 30_000
        };
        using var manager = new QuotaManager(config, NullLogger<QuotaManager>.Instance);

        // Exhaust the burst bucket
        var throttle = manager.CheckProduceQuota("client-1", 5_000); // 5 KB >> 1 KB limit

        Assert.True(throttle > 0, $"Expected throttle but got {throttle}ms");
        Assert.True(throttle <= 30_000, $"Throttle capped at MaxThrottleTimeMs but got {throttle}ms");
    }

    [Fact]
    public void QuotaManager_FetchThrottle_WhenOverLimit()
    {
        var config = new QuotaConfig
        {
            Enabled = true,
            ConsumerBytesPerSecond = 500,
            ConsumerBurstBytes = 500,
            MaxThrottleTimeMs = 30_000
        };
        using var manager = new QuotaManager(config, NullLogger<QuotaManager>.Instance);

        var throttle = manager.CheckFetchQuota("consumer-1", 10_000);

        Assert.True(throttle > 0);
    }

    [Fact]
    public void QuotaManager_ProduceNoThrottle_WhenBucketHasTokens()
    {
        var config = new QuotaConfig
        {
            Enabled = true,
            ProducerBytesPerSecond = 100_000_000, // 100 MB/s
            ProducerBurstBytes = 100_000_000
        };
        using var manager = new QuotaManager(config, NullLogger<QuotaManager>.Instance);

        var throttle = manager.CheckProduceQuota("client-1", 1_000);
        Assert.Equal(0, throttle);
    }

    [Fact]
    public void QuotaManager_ThrottleIsCapped_AtMaxThrottleTimeMs()
    {
        var config = new QuotaConfig
        {
            Enabled = true,
            ProducerBytesPerSecond = 1,
            ProducerBurstBytes = 1,
            MaxThrottleTimeMs = 5_000
        };
        using var manager = new QuotaManager(config, NullLogger<QuotaManager>.Instance);

        // Request an absurd amount to cause maximum throttle
        var throttle = manager.CheckProduceQuota("client-1", 1_000_000_000);

        Assert.Equal(5_000, throttle);
    }

    [Fact]
    public void QuotaManager_RecordProducedBytes_TracksStatistics()
    {
        var config = new QuotaConfig { Enabled = true };
        using var manager = new QuotaManager(config, NullLogger<QuotaManager>.Instance);

        manager.RecordProducedBytes("client-1", 1_000);
        manager.RecordProducedBytes("client-1", 2_000);

        var stats = manager.GetClientStats("client-1");
        Assert.NotNull(stats);
        Assert.Equal(3_000L, stats.TotalProducedBytes);
    }

    [Fact]
    public void QuotaManager_RecordFetchedBytes_TracksStatistics()
    {
        var config = new QuotaConfig { Enabled = true };
        using var manager = new QuotaManager(config, NullLogger<QuotaManager>.Instance);

        manager.RecordFetchedBytes("consumer-1", 500);
        manager.RecordFetchedBytes("consumer-1", 750);

        var stats = manager.GetClientStats("consumer-1");
        Assert.NotNull(stats);
        Assert.Equal(1_250L, stats.TotalFetchedBytes);
    }

    [Fact]
    public void QuotaManager_GetClientStats_UnknownClient_ReturnsNull()
    {
        var config = new QuotaConfig { Enabled = true };
        using var manager = new QuotaManager(config, NullLogger<QuotaManager>.Instance);

        var stats = manager.GetClientStats("never-seen-client");
        Assert.Null(stats);
    }

    [Fact]
    public void QuotaManager_GetAllClientStats_ReturnsAllClients()
    {
        var config = new QuotaConfig { Enabled = true };
        using var manager = new QuotaManager(config, NullLogger<QuotaManager>.Instance);

        manager.RecordProducedBytes("client-a", 100);
        manager.RecordProducedBytes("client-b", 200);

        var all = manager.GetAllClientStats().ToList();
        Assert.Equal(2, all.Count);
        Assert.Contains(all, x => x.ClientId == "client-a");
        Assert.Contains(all, x => x.ClientId == "client-b");
    }

    [Fact]
    public void QuotaManager_UpdateConfig_ChangesConfiguration()
    {
        var config = new QuotaConfig { Enabled = false, ProducerBytesPerSecond = 1_000 };
        using var manager = new QuotaManager(config, NullLogger<QuotaManager>.Instance);

        manager.UpdateConfig(enabled: true, producerBytesPerSecond: 5_000);

        var current = manager.Config;
        Assert.True(current.Enabled);
        Assert.Equal(5_000, current.ProducerBytesPerSecond);
    }

    [Fact]
    public void QuotaManager_UpdateConfig_NullArguments_NoChange()
    {
        var config = new QuotaConfig { Enabled = true, ProducerBytesPerSecond = 2_000 };
        using var manager = new QuotaManager(config, NullLogger<QuotaManager>.Instance);

        // Passing all nulls should not change anything
        manager.UpdateConfig();

        var current = manager.Config;
        Assert.True(current.Enabled);
        Assert.Equal(2_000, current.ProducerBytesPerSecond);
    }

    [Fact]
    public void QuotaManager_Disabled_RecordBytesIsNoOp()
    {
        var config = new QuotaConfig { Enabled = false };
        using var manager = new QuotaManager(config, NullLogger<QuotaManager>.Instance);

        // Should not throw and client state should not be created
        manager.RecordProducedBytes("client-1", 5_000);
        manager.RecordFetchedBytes("client-1", 5_000);

        var stats = manager.GetClientStats("client-1");
        Assert.Null(stats);
    }

    [Fact]
    public void QuotaManager_MultipleClients_IndependentBuckets()
    {
        var config = new QuotaConfig
        {
            Enabled = true,
            ProducerBytesPerSecond = 1_000,
            ProducerBurstBytes = 1_000
        };
        using var manager = new QuotaManager(config, NullLogger<QuotaManager>.Instance);

        // Exhaust client-1's bucket
        var throttle1 = manager.CheckProduceQuota("client-1", 5_000);
        // client-2 has its own bucket – should not be throttled for small request
        var throttle2 = manager.CheckProduceQuota("client-2", 100);

        Assert.True(throttle1 > 0, "client-1 should be throttled");
        Assert.Equal(0, throttle2); // client-2 starts fresh
    }

    // ────────────────────────── AutoTuning Edge Cases ─────────────────────────

    [Fact]
    public void AutoTuningRecommendation_Properties_AreCorrect()
    {
        var now = DateTimeOffset.UtcNow;
        var rec = new AutoTuningRecommendation
        {
            RuleId = "batch-size",
            Description = "Increase batch size for better throughput",
            ConfigKey = "producer.batch.size",
            CurrentValue = "8192",
            SuggestedValue = "16384",
            Reason = "Batch size is below optimal threshold",
            WasAutoApplied = false,
            Timestamp = now
        };

        Assert.Equal("batch-size", rec.RuleId);
        Assert.Equal("producer.batch.size", rec.ConfigKey);
        Assert.Equal("8192", rec.CurrentValue);
        Assert.Equal("16384", rec.SuggestedValue);
        Assert.False(rec.WasAutoApplied);
        Assert.Equal(now, rec.Timestamp);
    }

    [Fact]
    public void AutoTuningConfig_DisabledRules_EmptyByDefault()
    {
        var config = new AutoTuningConfig();
        Assert.Empty(config.DisabledRules);
    }

    [Fact]
    public void AutoTuningConfig_AllProperties_Settable()
    {
        var config = new AutoTuningConfig
        {
            Enabled = true,
            Mode = AutoTuningMode.AutoApply,
            AnalysisIntervalSeconds = 60,
            DisabledRules = ["batch-size"]
        };

        Assert.True(config.Enabled);
        Assert.Equal(AutoTuningMode.AutoApply, config.Mode);
        Assert.Equal(60, config.AnalysisIntervalSeconds);
        Assert.Single(config.DisabledRules);
        Assert.Contains("batch-size", config.DisabledRules);
    }

    [Fact]
    public void AutoTuningMode_AllValues_Present()
    {
        var values = Enum.GetValues<AutoTuningMode>();
        Assert.Contains(AutoTuningMode.SuggestOnly, values);
        Assert.Contains(AutoTuningMode.AutoApply, values);
    }

    // ────────────────────── CruiseControl Additional Tests ────────────────────

    [Fact]
    public void BalanceCalculator_SingleBroker_FullScore()
    {
        var calculator = new BalanceCalculator();
        var loads = new List<BrokerLoadSnapshot>
        {
            new() { BrokerId = 0, PartitionCount = 10, LeaderCount = 10, DiskUsageBytes = 1_000_000 }
        };

        var score = calculator.Calculate(loads);

        // Single broker → perfectly balanced (nothing to compare against)
        Assert.Equal(100, score.OverallScore);
    }

    [Fact]
    public void BalanceCalculator_EmptyLoads_ReturnsFullScore()
    {
        var calculator = new BalanceCalculator();
        var score = calculator.Calculate(new List<BrokerLoadSnapshot>());

        Assert.Equal(100, score.OverallScore);
    }

    [Fact]
    public void BalanceCalculator_NetworkImbalance_LowersNetworkScore()
    {
        var calculator = new BalanceCalculator();
        var loads = new List<BrokerLoadSnapshot>
        {
            new() { BrokerId = 0, PartitionCount = 10, LeaderCount = 5, DiskUsageBytes = 1000,
                    ProduceRateBytesPerSec = 100_000_000, ConsumeRateBytesPerSec = 100_000_000 },
            new() { BrokerId = 1, PartitionCount = 10, LeaderCount = 5, DiskUsageBytes = 1000,
                    ProduceRateBytesPerSec = 1000, ConsumeRateBytesPerSec = 1000 },
        };

        var score = calculator.Calculate(loads);

        Assert.True(score.NetworkBalance < 80,
            $"Expected network balance < 80, got {score.NetworkBalance}");
    }

    [Fact]
    public void BalanceCalculator_DetectsLeaderImbalance()
    {
        var calculator = new BalanceCalculator();
        var goals = new BalanceGoals { MaxLeaderImbalancePercent = 10.0 };
        var loads = new List<BrokerLoadSnapshot>
        {
            new() { BrokerId = 0, PartitionCount = 10, LeaderCount = 50, DiskUsageBytes = 1000 },
            new() { BrokerId = 1, PartitionCount = 10, LeaderCount = 1, DiskUsageBytes = 1000 },
        };

        var imbalances = calculator.DetectImbalances(loads, goals);

        Assert.Contains(imbalances, i => i.Metric == ImbalanceMetric.Leaders);
    }

    [Fact]
    public void BalanceCalculator_DetectsDiskImbalance()
    {
        var calculator = new BalanceCalculator();
        var goals = new BalanceGoals { MaxDiskImbalancePercent = 10.0 };
        var loads = new List<BrokerLoadSnapshot>
        {
            new() { BrokerId = 0, PartitionCount = 5, LeaderCount = 5, DiskUsageBytes = 100_000_000_000 },
            new() { BrokerId = 1, PartitionCount = 5, LeaderCount = 5, DiskUsageBytes = 1_000 },
        };

        var imbalances = calculator.DetectImbalances(loads, goals);

        Assert.Contains(imbalances, i => i.Metric == ImbalanceMetric.Disk);
    }

    [Fact]
    public void CruiseControlConfig_GoalsAreInitialized_ByDefault()
    {
        var config = new CruiseControlConfig();

        Assert.NotNull(config.Goals);
        Assert.Equal(20.0, config.Goals.MaxPartitionImbalancePercent);
        Assert.Equal(25.0, config.Goals.MaxDiskImbalancePercent);
        Assert.Equal(15.0, config.Goals.MaxLeaderImbalancePercent);
    }

    [Fact]
    public void BalanceGoals_MinPartitionsToRebalance_DefaultIsThree()
    {
        var goals = new BalanceGoals();
        Assert.Equal(3, goals.MinPartitionsToRebalance);
    }

    [Fact]
    public void ImbalanceMetric_AllValues_Present()
    {
        var values = Enum.GetValues<ImbalanceMetric>();
        Assert.Contains(ImbalanceMetric.Partitions, values);
        Assert.Contains(ImbalanceMetric.Leaders, values);
        Assert.Contains(ImbalanceMetric.Disk, values);
        Assert.Contains(ImbalanceMetric.Network, values);
    }

    [Fact]
    public void BalanceScore_AllDimensions_Tracked()
    {
        var score = new BalanceScore
        {
            PartitionBalance = 90,
            LeaderBalance = 95,
            DiskBalance = 85,
            NetworkBalance = 88,
            OverallScore = 89
        };

        Assert.Equal(90, score.PartitionBalance);
        Assert.Equal(95, score.LeaderBalance);
        Assert.Equal(85, score.DiskBalance);
        Assert.Equal(88, score.NetworkBalance);
        Assert.Equal(89, score.OverallScore);
    }
}
