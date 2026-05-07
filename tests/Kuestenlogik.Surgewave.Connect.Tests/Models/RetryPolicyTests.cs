using Kuestenlogik.Surgewave.Connect.Pipelines;

namespace Kuestenlogik.Surgewave.Connect.Tests.Models;

/// <summary>
/// Tests for RetryPolicy and ScheduleConfig records.
/// </summary>
public sealed class RetryPolicyTests
{
    [Fact]
    public void RetryPolicy_Defaults_AreCorrect()
    {
        var policy = new RetryPolicy();

        Assert.Equal(3, policy.MaxRetries);
        Assert.Equal(1000, policy.BackoffMs);
        Assert.Equal(2.0, policy.BackoffMultiplier);
        Assert.Equal(30000, policy.MaxBackoffMs);
        Assert.True(policy.Enabled);
    }

    [Fact]
    public void RetryPolicy_CustomValues_ArePreserved()
    {
        var policy = new RetryPolicy(
            MaxRetries: 10,
            BackoffMs: 500,
            BackoffMultiplier: 1.5,
            MaxBackoffMs: 60000,
            Enabled: false);

        Assert.Equal(10, policy.MaxRetries);
        Assert.Equal(500, policy.BackoffMs);
        Assert.Equal(1.5, policy.BackoffMultiplier);
        Assert.Equal(60000, policy.MaxBackoffMs);
        Assert.False(policy.Enabled);
    }

    [Fact]
    public void RetryPolicy_RecordEquality()
    {
        var p1 = new RetryPolicy(MaxRetries: 5, BackoffMs: 1000);
        var p2 = new RetryPolicy(MaxRetries: 5, BackoffMs: 1000);

        Assert.Equal(p1, p2);
    }

    [Fact]
    public void ScheduleConfig_Defaults_AreCorrect()
    {
        var config = new ScheduleConfig();

        Assert.Null(config.CronExpression);
        Assert.Equal("UTC", config.Timezone);
        Assert.False(config.Enabled);
        Assert.Null(config.MaxRunDurationMinutes);
        Assert.Null(config.LastRunAt);
        Assert.Null(config.NextRunAt);
        Assert.Null(config.LastCompletedAt);
    }

    [Fact]
    public void ScheduleConfig_AllProperties_AreSet()
    {
        var lastRun = DateTimeOffset.UtcNow.AddHours(-1);
        var nextRun = DateTimeOffset.UtcNow.AddHours(1);
        var lastComplete = DateTimeOffset.UtcNow.AddMinutes(-30);

        var config = new ScheduleConfig
        {
            CronExpression = "*/5 * * * *",
            Timezone = "America/New_York",
            Enabled = true,
            MaxRunDurationMinutes = 120,
            LastRunAt = lastRun,
            NextRunAt = nextRun,
            LastCompletedAt = lastComplete
        };

        Assert.Equal("*/5 * * * *", config.CronExpression);
        Assert.Equal("America/New_York", config.Timezone);
        Assert.True(config.Enabled);
        Assert.Equal(120, config.MaxRunDurationMinutes);
        Assert.Equal(lastRun, config.LastRunAt);
        Assert.Equal(nextRun, config.NextRunAt);
        Assert.Equal(lastComplete, config.LastCompletedAt);
    }
}
