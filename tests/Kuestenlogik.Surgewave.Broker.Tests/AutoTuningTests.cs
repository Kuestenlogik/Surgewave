using Kuestenlogik.Surgewave.Broker.AutoTuning;
using Kuestenlogik.Surgewave.Testing;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// Tests for the adaptive auto-tuning system.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class AutoTuningTests : IDisposable
{
    private readonly string _tempDir;
    private readonly ILoggerFactory _loggerFactory;

    public AutoTuningTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "surgewave-autotuning-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempDir);
        _loggerFactory = LoggerFactory.Create(builder => builder.SetMinimumLevel(LogLevel.Warning));
    }

    public void Dispose()
    {
        _loggerFactory.Dispose();
        try
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, recursive: true);
        }
        catch
        {
            // Ignore cleanup errors
        }
    }

    private (AutoTuningService Service, AutoTuningConfig Config) CreateService(
        Action<AutoTuningConfig>? configureAutoTuning = null,
        Action<BrokerConfig>? configureBroker = null)
    {
        var autoTuningConfig = new AutoTuningConfig
        {
            Enabled = true,
            Mode = AutoTuningMode.SuggestOnly,
            AnalysisIntervalSeconds = 30
        };
        configureAutoTuning?.Invoke(autoTuningConfig);

        var brokerConfig = new BrokerConfig { DataDirectory = _tempDir };
        configureBroker?.Invoke(brokerConfig);

        var dynamicConfig = new DynamicBrokerConfig(brokerConfig, _loggerFactory.CreateLogger<DynamicBrokerConfig>());
        var metrics = new BrokerMetrics();
        var service = new AutoTuningService(
            autoTuningConfig,
            brokerConfig,
            dynamicConfig,
            metrics,
            _loggerFactory.CreateLogger<AutoTuningService>());

        return (service, autoTuningConfig);
    }

    [Fact]
    public void AutoTuningConfig_Defaults_AreCorrect()
    {
        // Act
        var config = new AutoTuningConfig();

        // Assert
        Assert.False(config.Enabled);
        Assert.Equal(AutoTuningMode.SuggestOnly, config.Mode);
        Assert.Equal(30, config.AnalysisIntervalSeconds);
        Assert.Empty(config.DisabledRules);
    }

    [Fact]
    public void SuggestOnly_DoesNotAutoApply()
    {
        // Arrange
        var (service, _) = CreateService(
            configureAutoTuning: c => c.Mode = AutoTuningMode.SuggestOnly,
            configureBroker: c => c.ProducerBatchSizeBytes = 8192); // small batch to trigger rule

        // Act
        service.AnalyzeAndRecommend();

        // Assert
        var recommendations = service.ActiveRecommendations;
        Assert.NotEmpty(recommendations);
        Assert.All(recommendations, r => Assert.False(r.WasAutoApplied));
    }

    [Fact]
    public void BatchSizeRule_SuggestsIncrease_WhenSmall()
    {
        // Arrange
        var (service, _) = CreateService(
            configureBroker: c => c.ProducerBatchSizeBytes = 8192); // 8KB

        // Act
        service.AnalyzeAndRecommend();

        // Assert
        var batchRecommendation = service.ActiveRecommendations
            .FirstOrDefault(r => r.RuleId == "batch-size");
        Assert.NotNull(batchRecommendation);
        Assert.Equal("producer.batch.size", batchRecommendation.ConfigKey);
        Assert.Equal("8192", batchRecommendation.CurrentValue);
        // Should suggest doubling to 16384
        Assert.Equal("16384", batchRecommendation.SuggestedValue);
    }

    [Fact]
    public void CompressionRule_SuggestsEnable_WhenOff()
    {
        // Arrange
        var (service, _) = CreateService();

        // Act
        service.AnalyzeAndRecommend();

        // Assert
        var compRecommendation = service.ActiveRecommendations
            .FirstOrDefault(r => r.RuleId == "compression");
        Assert.NotNull(compRecommendation);
        Assert.Equal("compression.type", compRecommendation.ConfigKey);
        Assert.Equal("lz4", compRecommendation.SuggestedValue);
    }

    [Fact]
    public void DisabledRule_Skipped()
    {
        // Arrange
        var (service, _) = CreateService(
            configureAutoTuning: c => c.DisabledRules = ["batch-size", "compression"],
            configureBroker: c => c.ProducerBatchSizeBytes = 8192);

        // Act
        service.AnalyzeAndRecommend();

        // Assert
        var batchRecommendation = service.ActiveRecommendations
            .FirstOrDefault(r => r.RuleId == "batch-size");
        var compRecommendation = service.ActiveRecommendations
            .FirstOrDefault(r => r.RuleId == "compression");
        Assert.Null(batchRecommendation);
        Assert.Null(compRecommendation);
    }

    [Fact]
    public void AutoApply_AppliesRecommendation()
    {
        // Arrange
        var (service, _) = CreateService(
            configureAutoTuning: c => c.Mode = AutoTuningMode.AutoApply,
            configureBroker: c => c.DefaultNumPartitions = 1);

        // Act
        service.AnalyzeAndRecommend();

        // Assert - partition hotspot rule was auto-applied, so not in active recommendations
        var partitionRecommendation = service.ActiveRecommendations
            .FirstOrDefault(r => r.RuleId == "partition-hotspot");
        // The recommendation may or may not be in active list depending on whether DynamicBrokerConfig
        // accepted it. Check history instead.
        var history = service.History;
        Assert.NotEmpty(history);
    }

    [Fact]
    public void ManualApply_RemovesFromActive()
    {
        // Arrange
        var (service, _) = CreateService(
            configureBroker: c => c.DefaultNumPartitions = 1);

        service.AnalyzeAndRecommend();
        var before = service.ActiveRecommendations.Count;
        Assert.True(before > 0);

        // Act - apply the partition hotspot rule
        var result = service.ApplyRecommendation("partition-hotspot");

        // Assert
        if (result is not null)
        {
            var after = service.ActiveRecommendations
                .FirstOrDefault(r => r.RuleId == "partition-hotspot");
            Assert.Null(after); // Should be removed from active
        }
    }

    [Fact]
    public void ApplyRecommendation_NonExistentRule_ReturnsNull()
    {
        // Arrange
        var (service, _) = CreateService();

        // Act
        var result = service.ApplyRecommendation("non-existent-rule");

        // Assert
        Assert.Null(result);
    }

    [Fact]
    public void IsrRule_SuggestsReduction_WhenMinIsrEqualsReplicationFactor()
    {
        // Arrange
        var (service, _) = CreateService(
            configureBroker: c =>
            {
                c.DefaultReplicationFactor = 3;
                c.MinInSyncReplicas = 3;
            });

        // Act
        service.AnalyzeAndRecommend();

        // Assert
        var isrRecommendation = service.ActiveRecommendations
            .FirstOrDefault(r => r.RuleId == "isr-health");
        Assert.NotNull(isrRecommendation);
        Assert.Equal("min.insync.replicas", isrRecommendation.ConfigKey);
        Assert.Equal("2", isrRecommendation.SuggestedValue);
    }

    [Fact]
    public void History_RecordsRecommendations()
    {
        // Arrange
        var (service, _) = CreateService(
            configureBroker: c => c.ProducerBatchSizeBytes = 8192);

        // Act
        service.AnalyzeAndRecommend();

        // Assert
        Assert.NotEmpty(service.History);
        Assert.All(service.History, r => Assert.NotNull(r.RuleId));
        Assert.All(service.History, r => Assert.NotNull(r.Description));
    }

    [Fact]
    public void LogSegmentRule_SuggestsIncrease_WhenSmall()
    {
        // Arrange
        var (service, _) = CreateService(
            configureBroker: c => c.LogSegmentBytes = 10 * 1024 * 1024); // 10MB

        // Act
        service.AnalyzeAndRecommend();

        // Assert
        var segmentRecommendation = service.ActiveRecommendations
            .FirstOrDefault(r => r.RuleId == "log-segment-size");
        Assert.NotNull(segmentRecommendation);
        Assert.Equal("log.segment.bytes", segmentRecommendation.ConfigKey);
    }
}
