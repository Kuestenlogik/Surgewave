using Kuestenlogik.Surgewave.Broker.Quotas;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// Tests for the bandwidth quota system: SlidingWindowCounter, BandwidthTracker, BandwidthQuotaManager.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class BandwidthQuotaTests
{
    [Fact]
    public void SlidingWindowCounter_RecordsBytes()
    {
        // Arrange
        var counter = new SlidingWindowCounter(windowMs: 1000, bucketCount: 10);

        // Act
        counter.Record(1000);
        counter.Record(2000);

        // Assert
        var total = counter.GetWindowTotal();
        Assert.Equal(3000, total);
    }

    [Fact]
    public void SlidingWindowCounter_Rate()
    {
        // Arrange - 1 second window
        var counter = new SlidingWindowCounter(windowMs: 1000, bucketCount: 10);

        // Act
        counter.Record(5000);

        // Assert - rate should be approximately 5000 bytes/sec (within the same window)
        var rate = counter.GetCurrentRate();
        Assert.True(rate >= 4000, $"Expected rate >= 4000 but got {rate}");
        Assert.True(rate <= 10000, $"Expected rate <= 10000 but got {rate}");
    }

    [Fact]
    public void SlidingWindowCounter_Reset_ClearsAllBuckets()
    {
        // Arrange
        var counter = new SlidingWindowCounter(windowMs: 1000, bucketCount: 10);
        counter.Record(5000);

        // Act
        counter.Reset();

        // Assert
        Assert.Equal(0, counter.GetWindowTotal());
        Assert.Equal(0, counter.GetCurrentRate());
    }

    [Fact]
    public async Task SlidingWindowCounter_WindowExpiry()
    {
        // Arrange - short window for testing
        var counter = new SlidingWindowCounter(windowMs: 200, bucketCount: 4);

        // Act
        counter.Record(1000);
        Assert.True(counter.GetWindowTotal() > 0);

        // Wait for window to expire
        await Task.Delay(300);

        // Assert - old data should have expired
        var totalAfterExpiry = counter.GetWindowTotal();
        Assert.Equal(0, totalAfterExpiry);
    }

    [Fact]
    public void BandwidthTracker_RecordAndCheck_UnderLimit()
    {
        // Arrange
        var tracker = new BandwidthTracker(windowMs: 1000);

        // Act - record some bytes, check under limit
        tracker.RecordProduce("client-1", 5000);
        var result = tracker.CheckProduce("client-1", 1000, limitBytesPerSec: 100_000, delayFactor: 1.5);

        // Assert
        Assert.False(result.Throttled);
        Assert.Null(result.Delay);
        Assert.Equal(100_000, result.LimitBytesPerSec);
    }

    [Fact]
    public void BandwidthTracker_Throttled_WhenOverLimit()
    {
        // Arrange
        var tracker = new BandwidthTracker(windowMs: 1000);

        // Act - record way more than the limit, then check
        tracker.RecordProduce("client-1", 200_000);
        var result = tracker.CheckProduce("client-1", 50_000, limitBytesPerSec: 100_000, delayFactor: 1.5);

        // Assert
        Assert.True(result.Throttled);
        Assert.NotNull(result.Delay);
        Assert.True(result.Delay.Value.TotalMilliseconds > 0);
        Assert.Equal(100_000, result.LimitBytesPerSec);
    }

    [Fact]
    public void BandwidthTracker_NotThrottled_WhenUnderLimit()
    {
        // Arrange
        var tracker = new BandwidthTracker(windowMs: 1000);

        // Act - record a small amount
        tracker.RecordConsume("client-1", 100);
        var result = tracker.CheckConsume("client-1", 100, limitBytesPerSec: 1_000_000, delayFactor: 1.5);

        // Assert
        Assert.False(result.Throttled);
        Assert.Null(result.Delay);
    }

    [Fact]
    public void BandwidthTracker_Unlimited_WhenZeroLimit()
    {
        // Arrange
        var tracker = new BandwidthTracker(windowMs: 1000);

        // Act - zero limit = unlimited
        tracker.RecordProduce("client-1", 999_999_999);
        var result = tracker.CheckProduce("client-1", 999_999_999, limitBytesPerSec: 0, delayFactor: 1.5);

        // Assert
        Assert.False(result.Throttled);
        Assert.Null(result.Delay);
        Assert.Equal(0, result.LimitBytesPerSec);
    }

    [Fact]
    public void BandwidthQuotaManager_DefaultQuota_WhenNoOverrides()
    {
        // Arrange
        var config = new BandwidthQuotaConfig
        {
            Enabled = true,
            DefaultProduceBytesPerSec = 1_000_000,
            DefaultConsumeBytesPerSec = 500_000
        };
        using var manager = new BandwidthQuotaManager(config, NullLogger<BandwidthQuotaManager>.Instance);

        // Act
        var quota = manager.GetQuota("unknown-client");

        // Assert
        Assert.Equal(1_000_000, quota.ProduceBytesPerSec);
        Assert.Equal(500_000, quota.ConsumeBytesPerSec);
    }

    [Fact]
    public void BandwidthQuotaManager_ClientOverride_TakesPrecedence()
    {
        // Arrange
        var config = new BandwidthQuotaConfig
        {
            Enabled = true,
            DefaultProduceBytesPerSec = 1_000_000,
            DefaultConsumeBytesPerSec = 500_000
        };
        using var manager = new BandwidthQuotaManager(config, NullLogger<BandwidthQuotaManager>.Instance);

        // Set a client-specific override
        manager.SetClientQuota("client-1", new ClientBandwidthQuota
        {
            ProduceBytesPerSec = 2_000_000,
            ConsumeBytesPerSec = 1_000_000
        });

        // Act
        var quota = manager.GetQuota("client-1");
        var defaultQuota = manager.GetQuota("other-client");

        // Assert
        Assert.Equal(2_000_000, quota.ProduceBytesPerSec);
        Assert.Equal(1_000_000, quota.ConsumeBytesPerSec);
        Assert.Equal(1_000_000, defaultQuota.ProduceBytesPerSec); // other client gets default
    }

    [Fact]
    public void BandwidthQuotaManager_UserOverride_OverridesClientOverride()
    {
        // Arrange
        var config = new BandwidthQuotaConfig
        {
            Enabled = true,
            DefaultProduceBytesPerSec = 1_000_000
        };
        using var manager = new BandwidthQuotaManager(config, NullLogger<BandwidthQuotaManager>.Instance);

        // Set client override
        manager.SetClientQuota("client-1", new ClientBandwidthQuota { ProduceBytesPerSec = 2_000_000 });

        // Set user override (should take precedence)
        manager.SetUserQuota("admin", new ClientBandwidthQuota { ProduceBytesPerSec = 5_000_000 });

        // Act
        var quotaWithUser = manager.GetQuota("client-1", user: "admin");
        var quotaWithoutUser = manager.GetQuota("client-1");

        // Assert
        Assert.Equal(5_000_000, quotaWithUser.ProduceBytesPerSec); // user override wins
        Assert.Equal(2_000_000, quotaWithoutUser.ProduceBytesPerSec); // client override without user
    }

    [Fact]
    public void BandwidthQuotaConfig_Defaults()
    {
        // Arrange & Act
        var config = new BandwidthQuotaConfig();

        // Assert
        Assert.False(config.Enabled);
        Assert.Equal(0, config.DefaultProduceBytesPerSec);
        Assert.Equal(0, config.DefaultConsumeBytesPerSec);
        Assert.Equal(1000, config.EnforcementWindowMs);
        Assert.Equal(1.5, config.ThrottleDelayFactor);
        Assert.Empty(config.ClientOverrides);
        Assert.Empty(config.UserOverrides);
    }

    [Fact]
    public void BandwidthUsage_Utilization_Calculation()
    {
        // Arrange
        var usage = new BandwidthUsage
        {
            ClientId = "test",
            ProduceBytesPerSec = 750_000,
            ConsumeBytesPerSec = 250_000,
            ProduceLimitBytesPerSec = 1_000_000,
            ConsumeLimitBytesPerSec = 500_000,
            ProduceUtilizationPercent = 75.0,
            ConsumeUtilizationPercent = 50.0,
            IsThrottled = false,
            LastActivityAt = DateTimeOffset.UtcNow
        };

        // Assert
        Assert.Equal(75.0, usage.ProduceUtilizationPercent);
        Assert.Equal(50.0, usage.ConsumeUtilizationPercent);
        Assert.False(usage.IsThrottled);
    }

    [Fact]
    public void ThrottleResult_NotThrottled_HasNoDelay()
    {
        // Arrange & Act
        var result = new ThrottleResult(false, null, 5000, 10000);

        // Assert
        Assert.False(result.Throttled);
        Assert.Null(result.Delay);
        Assert.Equal(5000, result.CurrentBytesPerSec);
        Assert.Equal(10000, result.LimitBytesPerSec);
    }

    [Fact]
    public void ThrottleResult_Throttled_HasDelay()
    {
        // Arrange & Act
        var delay = TimeSpan.FromMilliseconds(500);
        var result = new ThrottleResult(true, delay, 15000, 10000);

        // Assert
        Assert.True(result.Throttled);
        Assert.NotNull(result.Delay);
        Assert.Equal(500, result.Delay.Value.TotalMilliseconds);
    }

    [Fact]
    public void BandwidthQuotaManager_Disabled_NeverThrottles()
    {
        // Arrange
        var config = new BandwidthQuotaConfig
        {
            Enabled = false,
            DefaultProduceBytesPerSec = 100 // would throttle if enabled
        };
        using var manager = new BandwidthQuotaManager(config, NullLogger<BandwidthQuotaManager>.Instance);

        // Act
        var result = manager.CheckAndRecordProduce("client-1", null, 999_999_999);

        // Assert
        Assert.False(result.Throttled);
    }

    [Fact]
    public void BandwidthQuotaManager_Metrics_TracksThrottledEvents()
    {
        // Arrange
        var config = new BandwidthQuotaConfig
        {
            Enabled = true,
            DefaultProduceBytesPerSec = 100, // very low limit
            EnforcementWindowMs = 1000
        };
        using var manager = new BandwidthQuotaManager(config, NullLogger<BandwidthQuotaManager>.Instance);

        // Act - produce way over the limit to trigger throttling
        manager.CheckAndRecordProduce("client-1", null, 1_000_000);

        // Assert
        var metrics = manager.GetMetrics();
        Assert.True(metrics.TotalClientsTracked >= 1);
        // The throttle event counter should have been incremented
        Assert.True(metrics.TotalThrottleEvents >= 1);
    }

    [Fact]
    public void BandwidthQuotaManager_RemoveClientQuota_ReturnsToDefault()
    {
        // Arrange
        var config = new BandwidthQuotaConfig
        {
            Enabled = true,
            DefaultProduceBytesPerSec = 1_000_000
        };
        using var manager = new BandwidthQuotaManager(config, NullLogger<BandwidthQuotaManager>.Instance);

        manager.SetClientQuota("client-1", new ClientBandwidthQuota { ProduceBytesPerSec = 5_000_000 });
        Assert.Equal(5_000_000, manager.GetQuota("client-1").ProduceBytesPerSec);

        // Act
        var removed = manager.RemoveClientQuota("client-1");

        // Assert
        Assert.True(removed);
        Assert.Equal(1_000_000, manager.GetQuota("client-1").ProduceBytesPerSec); // back to default
    }

    [Fact]
    public void BandwidthQuotaManager_ConfigOverrides_LoadedFromConfig()
    {
        // Arrange
        var config = new BandwidthQuotaConfig
        {
            Enabled = true,
            DefaultProduceBytesPerSec = 1_000_000,
            ClientOverrides = new Dictionary<string, ClientBandwidthQuota>
            {
                ["client-vip"] = new() { ProduceBytesPerSec = 10_000_000, ConsumeBytesPerSec = 10_000_000 }
            },
            UserOverrides = new Dictionary<string, ClientBandwidthQuota>
            {
                ["admin"] = new() { ProduceBytesPerSec = 0, ConsumeBytesPerSec = 0 } // unlimited
            }
        };

        // Act
        using var manager = new BandwidthQuotaManager(config, NullLogger<BandwidthQuotaManager>.Instance);

        // Assert
        Assert.Equal(10_000_000, manager.GetQuota("client-vip").ProduceBytesPerSec);
        Assert.Equal(0, manager.GetQuota("any-client", user: "admin").ProduceBytesPerSec); // unlimited
    }
}
