using Kuestenlogik.Surgewave.Clustering.Upgrades;
using Xunit;

namespace Kuestenlogik.Surgewave.Clustering.Tests.Upgrades;

public class RollingUpgradeTests
{
    [Fact]
    public void RollingUpgradeState_Defaults()
    {
        var state = new RollingUpgradeState();

        Assert.False(state.InProgress);
        Assert.Null(state.TargetVersion);
        Assert.Empty(state.Brokers);
        Assert.Null(state.StartedAt);
        Assert.Equal(UpgradePhase.NotStarted, state.Phase);
        Assert.Null(state.CompletedAt);
    }

    [Fact]
    public void RollingUpgradeState_Phases()
    {
        var state = new RollingUpgradeState
        {
            InProgress = true,
            TargetVersion = BrokerVersion.Parse("1.1.0"),
            StartedAt = DateTimeOffset.UtcNow,
            Phase = UpgradePhase.PreCheck,
        };

        Assert.True(state.InProgress);
        Assert.Equal(UpgradePhase.PreCheck, state.Phase);

        state.Phase = UpgradePhase.InProgress;
        Assert.Equal(UpgradePhase.InProgress, state.Phase);

        state.Phase = UpgradePhase.Verifying;
        Assert.Equal(UpgradePhase.Verifying, state.Phase);

        state.Phase = UpgradePhase.Completed;
        state.InProgress = false;
        state.CompletedAt = DateTimeOffset.UtcNow;

        Assert.False(state.InProgress);
        Assert.Equal(UpgradePhase.Completed, state.Phase);
        Assert.NotNull(state.CompletedAt);
    }

    [Fact]
    public void BrokerUpgradeStatus_DefaultState()
    {
        var status = new BrokerUpgradeStatus
        {
            BrokerId = 1,
            Version = BrokerVersion.Parse("1.0.0"),
        };

        Assert.Equal(1, status.BrokerId);
        Assert.Equal(BrokerUpgradeState.Pending, status.State);
        Assert.Null(status.UpgradedAt);
    }

    [Fact]
    public void BrokerUpgradeStatus_StateTransitions()
    {
        var status = new BrokerUpgradeStatus
        {
            BrokerId = 2,
            Version = BrokerVersion.Parse("1.0.0"),
        };

        status.State = BrokerUpgradeState.ShuttingDown;
        Assert.Equal(BrokerUpgradeState.ShuttingDown, status.State);

        status.State = BrokerUpgradeState.Upgrading;
        Assert.Equal(BrokerUpgradeState.Upgrading, status.State);

        status.State = BrokerUpgradeState.Restarting;
        Assert.Equal(BrokerUpgradeState.Restarting, status.State);

        status.State = BrokerUpgradeState.Verified;
        status.UpgradedAt = DateTimeOffset.UtcNow;
        Assert.Equal(BrokerUpgradeState.Verified, status.State);
        Assert.NotNull(status.UpgradedAt);
    }

    [Fact]
    public void BrokerUpgradeStatus_FailedState()
    {
        var status = new BrokerUpgradeStatus
        {
            BrokerId = 3,
            Version = BrokerVersion.Parse("1.0.0"),
        };

        status.State = BrokerUpgradeState.Failed;
        Assert.Equal(BrokerUpgradeState.Failed, status.State);
    }

    [Fact]
    public void RollingUpgradeConfig_Defaults()
    {
        var config = new RollingUpgradeConfig();

        Assert.Equal(TimeSpan.FromSeconds(60), config.GracefulShutdownTimeout);
        Assert.Equal(TimeSpan.FromSeconds(30), config.LeaderTransferTimeout);
        Assert.Equal(TimeSpan.FromMinutes(5), config.IsrRecoveryTimeout);
        Assert.True(config.RequireFullIsr);
        Assert.Equal(1, config.MaxConcurrentUpgrades);
    }

    [Fact]
    public void RollingUpgradeConfig_CustomValues()
    {
        var config = new RollingUpgradeConfig
        {
            GracefulShutdownTimeout = TimeSpan.FromSeconds(120),
            LeaderTransferTimeout = TimeSpan.FromSeconds(60),
            IsrRecoveryTimeout = TimeSpan.FromMinutes(10),
            RequireFullIsr = false,
            MaxConcurrentUpgrades = 2,
        };

        Assert.Equal(TimeSpan.FromSeconds(120), config.GracefulShutdownTimeout);
        Assert.Equal(TimeSpan.FromSeconds(60), config.LeaderTransferTimeout);
        Assert.Equal(TimeSpan.FromMinutes(10), config.IsrRecoveryTimeout);
        Assert.False(config.RequireFullIsr);
        Assert.Equal(2, config.MaxConcurrentUpgrades);
    }

    [Fact]
    public void TransferResult_Tracking()
    {
        var result = new TransferResult(
            TotalPartitions: 10,
            Transferred: 8,
            Failed: 2,
            FailedPartitions: ["topic1-3", "topic2-1"]);

        Assert.Equal(10, result.TotalPartitions);
        Assert.Equal(8, result.Transferred);
        Assert.Equal(2, result.Failed);
        Assert.Equal(2, result.FailedPartitions.Count);
        Assert.Contains("topic1-3", result.FailedPartitions);
        Assert.Contains("topic2-1", result.FailedPartitions);
    }

    [Fact]
    public void TransferResult_AllSuccessful()
    {
        var result = new TransferResult(5, 5, 0, []);

        Assert.Equal(5, result.TotalPartitions);
        Assert.Equal(5, result.Transferred);
        Assert.Equal(0, result.Failed);
        Assert.Empty(result.FailedPartitions);
    }

    [Fact]
    public void ShutdownResult_Success()
    {
        var result = new ShutdownResult(
            Success: true,
            PartitionsTransferred: 12,
            ConnectionsClosed: 5,
            Duration: TimeSpan.FromSeconds(3.5),
            Warnings: []);

        Assert.True(result.Success);
        Assert.Equal(12, result.PartitionsTransferred);
        Assert.Equal(5, result.ConnectionsClosed);
        Assert.Equal(TimeSpan.FromSeconds(3.5), result.Duration);
        Assert.Empty(result.Warnings);
    }

    [Fact]
    public void ShutdownResult_WithWarnings()
    {
        var result = new ShutdownResult(
            Success: false,
            PartitionsTransferred: 8,
            ConnectionsClosed: 3,
            Duration: TimeSpan.FromSeconds(60),
            Warnings: ["Some transfers timed out"]);

        Assert.False(result.Success);
        Assert.Single(result.Warnings);
    }

    [Fact]
    public void RollingUpgradeState_BrokerTracking()
    {
        var state = new RollingUpgradeState
        {
            InProgress = true,
            TargetVersion = BrokerVersion.Parse("1.1.0"),
            Phase = UpgradePhase.InProgress,
            StartedAt = DateTimeOffset.UtcNow,
            Brokers =
            [
                new BrokerUpgradeStatus
                {
                    BrokerId = 0,
                    Version = BrokerVersion.Parse("1.0.0"),
                    State = BrokerUpgradeState.Verified,
                    UpgradedAt = DateTimeOffset.UtcNow,
                },
                new BrokerUpgradeStatus
                {
                    BrokerId = 1,
                    Version = BrokerVersion.Parse("1.0.0"),
                    State = BrokerUpgradeState.ShuttingDown,
                },
                new BrokerUpgradeStatus
                {
                    BrokerId = 2,
                    Version = BrokerVersion.Parse("1.0.0"),
                    State = BrokerUpgradeState.Pending,
                },
            ],
        };

        Assert.Equal(3, state.Brokers.Count);
        Assert.Equal(BrokerUpgradeState.Verified, state.Brokers[0].State);
        Assert.Equal(BrokerUpgradeState.ShuttingDown, state.Brokers[1].State);
        Assert.Equal(BrokerUpgradeState.Pending, state.Brokers[2].State);
    }

    [Fact]
    public void UpgradePhase_AllValues()
    {
        var allPhases = Enum.GetValues<UpgradePhase>();
        Assert.Equal(6, allPhases.Length);
        Assert.Contains(UpgradePhase.NotStarted, allPhases);
        Assert.Contains(UpgradePhase.PreCheck, allPhases);
        Assert.Contains(UpgradePhase.InProgress, allPhases);
        Assert.Contains(UpgradePhase.Verifying, allPhases);
        Assert.Contains(UpgradePhase.Completed, allPhases);
        Assert.Contains(UpgradePhase.Failed, allPhases);
    }

    [Fact]
    public void BrokerUpgradeState_AllValues()
    {
        var allStates = Enum.GetValues<BrokerUpgradeState>();
        Assert.Equal(6, allStates.Length);
        Assert.Contains(BrokerUpgradeState.Pending, allStates);
        Assert.Contains(BrokerUpgradeState.ShuttingDown, allStates);
        Assert.Contains(BrokerUpgradeState.Upgrading, allStates);
        Assert.Contains(BrokerUpgradeState.Restarting, allStates);
        Assert.Contains(BrokerUpgradeState.Verified, allStates);
        Assert.Contains(BrokerUpgradeState.Failed, allStates);
    }
}
