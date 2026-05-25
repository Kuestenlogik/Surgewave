using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// Tests for the QuotaManager class.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public class QuotaManagerTests
{
    [Fact]
    public void QuotaManager_DisabledByDefault_NoThrottle()
    {
        // Arrange
        var config = QuotaConfig.Disabled;
        using var manager = new QuotaManager(config, NullLogger<QuotaManager>.Instance);

        // Act
        var produceThrottle = manager.CheckProduceQuota("client1", 10_000_000);
        var fetchThrottle = manager.CheckFetchQuota("client1", 10_000_000);

        // Assert
        Assert.Equal(0, produceThrottle);
        Assert.Equal(0, fetchThrottle);
    }

    [Fact]
    public void QuotaManager_UnlimitedQuota_NoThrottle()
    {
        // Arrange - enabled but unlimited (-1)
        var config = new QuotaConfig
        {
            Enabled = true,
            ProducerBytesPerSecond = -1,
            ConsumerBytesPerSecond = -1
        };
        using var manager = new QuotaManager(config, NullLogger<QuotaManager>.Instance);

        // Act
        var produceThrottle = manager.CheckProduceQuota("client1", 10_000_000);
        var fetchThrottle = manager.CheckFetchQuota("client1", 10_000_000);

        // Assert
        Assert.Equal(0, produceThrottle);
        Assert.Equal(0, fetchThrottle);
    }

    [Fact]
    public void QuotaManager_WithinBurst_NoThrottle()
    {
        // Arrange
        var config = new QuotaConfig
        {
            Enabled = true,
            ProducerBytesPerSecond = 1_000_000,  // 1 MB/s
            ProducerBurstBytes = 10_000_000,     // 10 MB burst
            ConsumerBytesPerSecond = 1_000_000,
            ConsumerBurstBytes = 10_000_000
        };
        using var manager = new QuotaManager(config, NullLogger<QuotaManager>.Instance);

        // Act - request within burst capacity
        var produceThrottle = manager.CheckProduceQuota("client1", 5_000_000); // 5 MB
        var fetchThrottle = manager.CheckFetchQuota("client1", 5_000_000);

        // Assert
        Assert.Equal(0, produceThrottle);
        Assert.Equal(0, fetchThrottle);
    }

    [Fact]
    public void QuotaManager_ExceedsBurst_ReturnsThrottleTime()
    {
        // Arrange
        var config = new QuotaConfig
        {
            Enabled = true,
            ProducerBytesPerSecond = 1_000_000,  // 1 MB/s
            ProducerBurstBytes = 5_000_000,      // 5 MB burst
            MaxThrottleTimeMs = 30000
        };
        using var manager = new QuotaManager(config, NullLogger<QuotaManager>.Instance);

        // Act - request exceeds burst capacity
        var throttle = manager.CheckProduceQuota("client1", 10_000_000); // 10 MB > 5 MB burst

        // Assert - should return throttle time (10MB - 5MB = 5MB deficit, at 1MB/s = 5 seconds = 5000ms)
        Assert.True(throttle > 0);
        Assert.True(throttle <= 30000); // capped at max throttle
    }

    [Fact]
    public void QuotaManager_ConsecutiveRequests_TokensDeplete()
    {
        // Arrange
        var config = new QuotaConfig
        {
            Enabled = true,
            ProducerBytesPerSecond = 1_000_000,  // 1 MB/s
            ProducerBurstBytes = 10_000_000      // 10 MB burst
        };
        using var manager = new QuotaManager(config, NullLogger<QuotaManager>.Instance);

        // Act - first request should pass
        var throttle1 = manager.CheckProduceQuota("client1", 8_000_000); // 8 MB

        // Second request should need throttling (only 2 MB tokens left)
        var throttle2 = manager.CheckProduceQuota("client1", 5_000_000); // 5 MB

        // Assert
        Assert.Equal(0, throttle1);
        Assert.True(throttle2 > 0); // should be throttled
    }

    [Fact]
    public void QuotaManager_DifferentClients_IndependentQuotas()
    {
        // Arrange
        var config = new QuotaConfig
        {
            Enabled = true,
            ProducerBytesPerSecond = 1_000_000,
            ProducerBurstBytes = 10_000_000
        };
        using var manager = new QuotaManager(config, NullLogger<QuotaManager>.Instance);

        // Act - deplete client1's quota
        manager.CheckProduceQuota("client1", 10_000_000);

        // client2 should still have full quota
        var throttle = manager.CheckProduceQuota("client2", 5_000_000);

        // Assert
        Assert.Equal(0, throttle);
    }

    [Fact]
    public void QuotaManager_RecordBytes_UpdatesStats()
    {
        // Arrange
        var config = new QuotaConfig { Enabled = true };
        using var manager = new QuotaManager(config, NullLogger<QuotaManager>.Instance);

        // Act
        manager.CheckProduceQuota("client1", 1000); // creates client state
        manager.RecordProducedBytes("client1", 5000);
        manager.RecordFetchedBytes("client1", 3000);

        // Assert
        var stats = manager.GetClientStats("client1");
        Assert.NotNull(stats);
        Assert.Equal(5000, stats.TotalProducedBytes);
        Assert.Equal(3000, stats.TotalFetchedBytes);
    }

    [Fact]
    public void QuotaManager_GetClientStats_ReturnsNullForUnknownClient()
    {
        // Arrange
        var config = new QuotaConfig { Enabled = true };
        using var manager = new QuotaManager(config, NullLogger<QuotaManager>.Instance);

        // Act
        var stats = manager.GetClientStats("unknown-client");

        // Assert
        Assert.Null(stats);
    }

    [Fact]
    public void QuotaManager_GetAllClientStats_ReturnsAllClients()
    {
        // Arrange - quota must be enabled with limits to create client states
        var config = new QuotaConfig
        {
            Enabled = true,
            ProducerBytesPerSecond = 1_000_000,
            ConsumerBytesPerSecond = 1_000_000
        };
        using var manager = new QuotaManager(config, NullLogger<QuotaManager>.Instance);

        // Create states for multiple clients
        manager.CheckProduceQuota("client1", 1000);
        manager.CheckProduceQuota("client2", 1000);
        manager.CheckFetchQuota("client3", 1000);

        // Act
        var allStats = manager.GetAllClientStats().ToList();

        // Assert
        Assert.Equal(3, allStats.Count);
        Assert.Contains(allStats, s => s.ClientId == "client1");
        Assert.Contains(allStats, s => s.ClientId == "client2");
        Assert.Contains(allStats, s => s.ClientId == "client3");
    }

    [Fact]
    public void QuotaManager_MaxThrottleTime_CapsThrottle()
    {
        // Arrange
        var config = new QuotaConfig
        {
            Enabled = true,
            ProducerBytesPerSecond = 100,  // Very slow: 100 bytes/s
            ProducerBurstBytes = 100,
            MaxThrottleTimeMs = 1000       // Cap at 1 second
        };
        using var manager = new QuotaManager(config, NullLogger<QuotaManager>.Instance);

        // Act - request would require 100 seconds but should be capped
        var throttle = manager.CheckProduceQuota("client1", 10_000); // 10KB at 100 B/s = 100s

        // Assert - throttle should be capped at MaxThrottleTimeMs
        Assert.Equal(1000, throttle);
    }

    [Fact]
    public void QuotaConfig_DefaultValues()
    {
        // Arrange & Act
        var config = new QuotaConfig();

        // Assert
        Assert.False(config.Enabled);
        Assert.Equal(-1, config.ProducerBytesPerSecond);
        Assert.Equal(-1, config.ConsumerBytesPerSecond);
        Assert.Equal(104857600, config.ProducerBurstBytes); // 100 MB
        Assert.Equal(104857600, config.ConsumerBurstBytes);
        Assert.Equal(30000, config.MaxThrottleTimeMs);
        Assert.Equal(10, config.ClientInactivityTimeoutMinutes);
    }
}
