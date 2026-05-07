using Kuestenlogik.Surgewave.Broker.CruiseControl;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests.CruiseControl;

/// <summary>
/// Tests for the Cruise Control (auto-balance) system.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class CruiseControlTests
{
    [Fact]
    public void CruiseControlConfig_Defaults()
    {
        // Act
        var config = new CruiseControlConfig();

        // Assert
        Assert.False(config.Enabled);
        Assert.Equal(300, config.AnalysisIntervalSeconds);
        Assert.Equal(CruiseControlMode.SuggestOnly, config.Mode);
        Assert.Equal(50_000_000, config.ThrottleRateBytesPerSec);
        Assert.Equal(30, config.CooldownMinutes);
        Assert.NotNull(config.Goals);
    }

    [Fact]
    public void BalanceGoals_Defaults()
    {
        // Act
        var goals = new BalanceGoals();

        // Assert
        Assert.Equal(20.0, goals.MaxPartitionImbalancePercent);
        Assert.Equal(25.0, goals.MaxDiskImbalancePercent);
        Assert.Equal(15.0, goals.MaxLeaderImbalancePercent);
        Assert.Equal(30.0, goals.MaxNetworkImbalancePercent);
        Assert.Equal(3, goals.MinPartitionsToRebalance);
    }

    [Fact]
    public void BalanceCalculator_PerfectlyBalanced()
    {
        // Arrange
        var calculator = new BalanceCalculator();
        var loads = new List<BrokerLoadSnapshot>
        {
            new() { BrokerId = 0, PartitionCount = 10, LeaderCount = 5, DiskUsageBytes = 1000, ProduceRateBytesPerSec = 100, ConsumeRateBytesPerSec = 100 },
            new() { BrokerId = 1, PartitionCount = 10, LeaderCount = 5, DiskUsageBytes = 1000, ProduceRateBytesPerSec = 100, ConsumeRateBytesPerSec = 100 },
            new() { BrokerId = 2, PartitionCount = 10, LeaderCount = 5, DiskUsageBytes = 1000, ProduceRateBytesPerSec = 100, ConsumeRateBytesPerSec = 100 },
        };

        // Act
        var score = calculator.Calculate(loads);

        // Assert
        Assert.Equal(100, score.PartitionBalance);
        Assert.Equal(100, score.LeaderBalance);
        Assert.Equal(100, score.DiskBalance);
        Assert.Equal(100, score.NetworkBalance);
        Assert.Equal(100, score.OverallScore);
    }

    [Fact]
    public void BalanceCalculator_Imbalanced_PartitionSkew()
    {
        // Arrange
        var calculator = new BalanceCalculator();
        var loads = new List<BrokerLoadSnapshot>
        {
            new() { BrokerId = 0, PartitionCount = 30, LeaderCount = 10, DiskUsageBytes = 1000 },
            new() { BrokerId = 1, PartitionCount = 5, LeaderCount = 10, DiskUsageBytes = 1000 },
            new() { BrokerId = 2, PartitionCount = 10, LeaderCount = 10, DiskUsageBytes = 1000 },
        };

        // Act
        var score = calculator.Calculate(loads);

        // Assert - partition balance should be low due to skew
        Assert.True(score.PartitionBalance < 80, $"Expected partition balance < 80, got {score.PartitionBalance}");
        Assert.Equal(100, score.LeaderBalance);
        Assert.Equal(100, score.DiskBalance);
    }

    [Fact]
    public void BalanceCalculator_Imbalanced_LeaderSkew()
    {
        // Arrange
        var calculator = new BalanceCalculator();
        var loads = new List<BrokerLoadSnapshot>
        {
            new() { BrokerId = 0, PartitionCount = 10, LeaderCount = 20, DiskUsageBytes = 1000 },
            new() { BrokerId = 1, PartitionCount = 10, LeaderCount = 2, DiskUsageBytes = 1000 },
            new() { BrokerId = 2, PartitionCount = 10, LeaderCount = 8, DiskUsageBytes = 1000 },
        };

        // Act
        var score = calculator.Calculate(loads);

        // Assert
        Assert.True(score.LeaderBalance < 80, $"Expected leader balance < 80, got {score.LeaderBalance}");
        Assert.Equal(100, score.PartitionBalance);
    }

    [Fact]
    public void BalanceCalculator_Imbalanced_DiskSkew()
    {
        // Arrange
        var calculator = new BalanceCalculator();
        var loads = new List<BrokerLoadSnapshot>
        {
            new() { BrokerId = 0, PartitionCount = 10, LeaderCount = 5, DiskUsageBytes = 10_000_000_000 },
            new() { BrokerId = 1, PartitionCount = 10, LeaderCount = 5, DiskUsageBytes = 1_000_000_000 },
            new() { BrokerId = 2, PartitionCount = 10, LeaderCount = 5, DiskUsageBytes = 2_000_000_000 },
        };

        // Act
        var score = calculator.Calculate(loads);

        // Assert
        Assert.True(score.DiskBalance < 80, $"Expected disk balance < 80, got {score.DiskBalance}");
        Assert.Equal(100, score.PartitionBalance);
    }

    [Fact]
    public void BalanceCalculator_DetectsImbalance_OverThreshold()
    {
        // Arrange
        var calculator = new BalanceCalculator();
        var goals = new BalanceGoals { MaxPartitionImbalancePercent = 20.0 };
        var loads = new List<BrokerLoadSnapshot>
        {
            new() { BrokerId = 0, PartitionCount = 30, LeaderCount = 10, DiskUsageBytes = 1000 },
            new() { BrokerId = 1, PartitionCount = 5, LeaderCount = 10, DiskUsageBytes = 1000 },
            new() { BrokerId = 2, PartitionCount = 10, LeaderCount = 10, DiskUsageBytes = 1000 },
        };

        // Act
        var imbalances = calculator.DetectImbalances(loads, goals);

        // Assert
        Assert.Contains(imbalances, i => i.Metric == ImbalanceMetric.Partitions);
        var partitionImbalance = imbalances.First(i => i.Metric == ImbalanceMetric.Partitions);
        Assert.Equal(0, partitionImbalance.OverloadedBrokerId);
        Assert.Equal(1, partitionImbalance.UnderloadedBrokerId);
        Assert.True(partitionImbalance.ImbalancePercent > 20.0);
    }

    [Fact]
    public void BalanceCalculator_NoImbalance_UnderThreshold()
    {
        // Arrange
        var calculator = new BalanceCalculator();
        var goals = new BalanceGoals { MaxPartitionImbalancePercent = 50.0 }; // very lenient
        var loads = new List<BrokerLoadSnapshot>
        {
            new() { BrokerId = 0, PartitionCount = 11, LeaderCount = 10, DiskUsageBytes = 1000 },
            new() { BrokerId = 1, PartitionCount = 9, LeaderCount = 10, DiskUsageBytes = 1000 },
            new() { BrokerId = 2, PartitionCount = 10, LeaderCount = 10, DiskUsageBytes = 1000 },
        };

        // Act
        var imbalances = calculator.DetectImbalances(loads, goals);

        // Assert - with lenient threshold, slight differences should not trigger
        Assert.DoesNotContain(imbalances, i => i.Metric == ImbalanceMetric.Partitions);
    }

    [Fact]
    public void BalanceScore_WeightedAverage()
    {
        // Arrange
        var calculator = new BalanceCalculator();
        // Create loads where only partitions are imbalanced
        var loads = new List<BrokerLoadSnapshot>
        {
            new() { BrokerId = 0, PartitionCount = 100, LeaderCount = 10, DiskUsageBytes = 1000 },
            new() { BrokerId = 1, PartitionCount = 1, LeaderCount = 10, DiskUsageBytes = 1000 },
        };

        // Act
        var score = calculator.Calculate(loads);

        // Assert - overall score should be a weighted average, not just partition balance
        Assert.True(score.PartitionBalance < 50, $"Partition balance should be low, got {score.PartitionBalance}");
        Assert.Equal(100, score.LeaderBalance);
        Assert.Equal(100, score.DiskBalance);
        Assert.Equal(100, score.NetworkBalance);
        // Overall = partition * 0.3 + leader * 0.25 + disk * 0.25 + network * 0.20
        // = low * 0.3 + 100 * 0.7 = should be between partition and 100
        Assert.True(score.OverallScore > score.PartitionBalance, "Overall should be higher than worst metric");
        Assert.True(score.OverallScore < 100, "Overall should be less than perfect since partitions are imbalanced");
    }

    [Fact]
    public void BrokerLoadSnapshot_Properties()
    {
        // Act
        var snapshot = new BrokerLoadSnapshot
        {
            BrokerId = 42,
            PartitionCount = 100,
            LeaderCount = 50,
            DiskUsageBytes = 1_000_000_000,
            ProduceRateBytesPerSec = 500_000,
            ConsumeRateBytesPerSec = 300_000,
            CpuPercent = 65.5,
            NetworkUtilizationPercent = 45.2
        };

        // Assert
        Assert.Equal(42, snapshot.BrokerId);
        Assert.Equal(100, snapshot.PartitionCount);
        Assert.Equal(50, snapshot.LeaderCount);
        Assert.Equal(1_000_000_000, snapshot.DiskUsageBytes);
        Assert.Equal(500_000, snapshot.ProduceRateBytesPerSec);
        Assert.Equal(300_000, snapshot.ConsumeRateBytesPerSec);
        Assert.Equal(65.5, snapshot.CpuPercent);
        Assert.Equal(45.2, snapshot.NetworkUtilizationPercent);
        Assert.True(snapshot.Timestamp <= DateTimeOffset.UtcNow);
    }

    [Fact]
    public void ClusterBalanceReport_IsBalanced()
    {
        // Act
        var report = new ClusterBalanceReport
        {
            IsBalanced = true,
            Score = new BalanceScore
            {
                PartitionBalance = 100,
                LeaderBalance = 100,
                DiskBalance = 100,
                NetworkBalance = 100,
                OverallScore = 100
            }
        };

        // Assert
        Assert.True(report.IsBalanced);
        Assert.Equal(100, report.Score.OverallScore);
        Assert.Empty(report.Imbalances);
        Assert.Null(report.SuggestedPlan);
        Assert.NotEmpty(report.Timestamp.ToString());
    }

    [Fact]
    public void CruiseControlMode_AllValues()
    {
        // Assert
        var values = Enum.GetValues<CruiseControlMode>();
        Assert.Equal(2, values.Length);
        Assert.Contains(CruiseControlMode.SuggestOnly, values);
        Assert.Contains(CruiseControlMode.AutoRebalance, values);
    }

    [Fact]
    public void ImbalanceDetail_Properties()
    {
        // Act
        var detail = new ImbalanceDetail
        {
            Metric = ImbalanceMetric.Partitions,
            OverloadedBrokerId = 0,
            UnderloadedBrokerId = 2,
            ImbalancePercent = 35.7,
            Description = "Broker 0 has significantly more partitions than broker 2"
        };

        // Assert
        Assert.Equal(ImbalanceMetric.Partitions, detail.Metric);
        Assert.Equal(0, detail.OverloadedBrokerId);
        Assert.Equal(2, detail.UnderloadedBrokerId);
        Assert.Equal(35.7, detail.ImbalancePercent);
        Assert.Contains("Broker 0", detail.Description);
    }
}
