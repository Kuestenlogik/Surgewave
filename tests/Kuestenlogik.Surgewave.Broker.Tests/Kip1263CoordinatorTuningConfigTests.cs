using System.ComponentModel.DataAnnotations;
using System.Reflection;
using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// KIP-1263 — coordinator tuning configs:
/// <list type="bullet">
///   <item><c>group.coordinator.background.threads</c> — default 2, lower bound 1 (was hard-wired to 1)</item>
///   <item><c>group.consumer.assignment.interval.ms</c> — default 1000, lower bound 0 (was effectively 0)</item>
///   <item><c>group.share.assignment.interval.ms</c> — default 1000, lower bound 0</item>
///   <item><c>group.streams.assignment.interval.ms</c> — default 1000, lower bound 0</item>
/// </list>
///
/// Surgewave captures all four on <see cref="BrokerConfig"/> with the
/// upstream defaults. The `TargetAssignmentComputer` already gates on
/// material change (Surgewave is less prone to the upstream thrash that
/// motivated KIP-1263), so the time-based assignment-interval gate is a
/// documented follow-up rather than a critical gap. Background-thread
/// pool wiring for regex-subscription updates is also a follow-up — the
/// configs are externally visible today so admins can tune them.
///
/// Tests go via reflection so a future rename can't silently disconnect
/// them from the KIP — the property MUST exist by its KIP-1263 name on
/// <see cref="BrokerConfig"/>. This was the lesson learnt from the
/// Kip1196 test pass.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class Kip1263CoordinatorTuningConfigTests
{
    private static PropertyInfo Property(string name) =>
        typeof(BrokerConfig).GetProperty(name)
        ?? throw new InvalidOperationException(
            $"BrokerConfig.{name} is missing — KIP-1263 expects this exact property name on BrokerConfig.");

    public static IEnumerable<object[]> IntervalProps =>
    [
        ["ConsumerGroupAssignmentIntervalMs"],
        ["ShareGroupAssignmentIntervalMs"],
        ["StreamsGroupAssignmentIntervalMs"],
    ];

    [Fact]
    public void GroupCoordinatorBackgroundThreads_DefaultIsTwo_LowerBoundIsOne()
    {
        var prop = Property(nameof(BrokerConfig.GroupCoordinatorBackgroundThreads));
        var config = new BrokerConfig();
        Assert.Equal(2, (int)prop.GetValue(config)!);

        var range = prop.GetCustomAttribute<RangeAttribute>();
        Assert.NotNull(range);
        Assert.Equal(1, (int)range.Minimum);
    }

    [Theory]
    [MemberData(nameof(IntervalProps))]
    public void AssignmentInterval_DefaultIs1000ms_LowerBoundIsZero(string propertyName)
    {
        var prop = Property(propertyName);
        var config = new BrokerConfig();
        Assert.Equal(1000, (int)prop.GetValue(config)!);

        var range = prop.GetCustomAttribute<RangeAttribute>();
        Assert.NotNull(range);
        Assert.Equal(0, (int)range.Minimum);
    }

    [Theory]
    [MemberData(nameof(IntervalProps))]
    public void AssignmentInterval_NegativeValue_FailsValidation(string propertyName)
    {
        var prop = Property(propertyName);
        var config = new BrokerConfig();
        prop.SetValue(config, -1);
        var results = new List<ValidationResult>();
        var ctx = new ValidationContext(config) { MemberName = propertyName };
        var valid = Validator.TryValidateProperty(prop.GetValue(config)!, ctx, results);
        Assert.False(valid);
    }

    [Fact]
    public void GroupCoordinatorBackgroundThreads_Zero_FailsValidation()
    {
        // Upstream uses atLeast(1) — a thread pool of size 0 deadlocks the
        // regex-subscription path, so the validator must reject zero.
        var prop = Property(nameof(BrokerConfig.GroupCoordinatorBackgroundThreads));
        var config = new BrokerConfig();
        prop.SetValue(config, 0);
        var results = new List<ValidationResult>();
        var ctx = new ValidationContext(config) { MemberName = prop.Name };
        var valid = Validator.TryValidateProperty(prop.GetValue(config)!, ctx, results);
        Assert.False(valid);
    }

    [Theory]
    [MemberData(nameof(IntervalProps))]
    public void AssignmentInterval_CustomValue_RoundTrips(string propertyName)
    {
        var prop = Property(propertyName);
        var config = new BrokerConfig();
        prop.SetValue(config, 5000);
        Assert.Equal(5000, (int)prop.GetValue(config)!);
    }
}
