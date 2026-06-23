using System.ComponentModel.DataAnnotations;
using System.Reflection;
using Kuestenlogik.Surgewave.Broker;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// KIP-1196 — two new broker-level configs let admins cap the buffer size
/// each coordinator retains for reuse:
/// <list type="bullet">
///   <item><c>group.coordinator.cached.buffer.max.bytes</c></item>
///   <item><c>share.coordinator.cached.buffer.max.bytes</c></item>
/// </list>
///
/// Both default to 1 MiB + 12-byte log-record overhead with a lower bound
/// of 512 KiB upstream. Surgewave captures the values on
/// <see cref="BrokerConfig"/> with the same defaults and a matching
/// <see cref="RangeAttribute"/> so misconfiguration fails validation at
/// config-bind time. The actual buffer-reuse pool wiring inside the
/// coordinators is a documented follow-up (see kips.md).
///
/// Tests go via reflection so a future rename/refactor of the property
/// names doesn't silently disconnect the test from the KIP — the property
/// MUST exist by its KIP-1196 name on <see cref="BrokerConfig"/>.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class Kip1196CoordinatorBufferConfigTests
{
    private const int UpstreamDefault = 1024 * 1024 + 12; // 1 MiB + LOG_OVERHEAD
    private const int UpstreamMin = 512 * 1024;

    private static PropertyInfo Property(string name) =>
        typeof(BrokerConfig).GetProperty(name)
        ?? throw new InvalidOperationException(
            $"BrokerConfig.{name} is missing — KIP-1196 expects this exact property name on BrokerConfig.");

    [Theory]
    [InlineData("GroupCoordinatorCachedBufferMaxBytes")]
    [InlineData("ShareCoordinatorCachedBufferMaxBytes")]
    public void BrokerConfig_Defaults_MatchUpstreamKafkaKip1196Values(string propertyName)
    {
        var prop = Property(propertyName);
        var config = new BrokerConfig();
        var value = (int)prop.GetValue(config)!;
        Assert.Equal(UpstreamDefault, value);
    }

    [Theory]
    [InlineData("GroupCoordinatorCachedBufferMaxBytes")]
    [InlineData("ShareCoordinatorCachedBufferMaxBytes")]
    public void BrokerConfig_HasRangeAttribute_WithUpstreamLowerBound(string propertyName)
    {
        var range = Property(propertyName).GetCustomAttribute<RangeAttribute>();
        Assert.NotNull(range);
        Assert.Equal(UpstreamMin, (int)range.Minimum);
    }

    [Theory]
    [InlineData("GroupCoordinatorCachedBufferMaxBytes")]
    [InlineData("ShareCoordinatorCachedBufferMaxBytes")]
    public void BrokerConfig_AcceptsCustomValues_AboveDefault(string propertyName)
    {
        // Admins may bump the cap to retain larger writes — common when a
        // deployment ships unusually large group-metadata batches.
        var prop = Property(propertyName);
        var config = new BrokerConfig();
        prop.SetValue(config, 4 * 1024 * 1024);
        Assert.Equal(4 * 1024 * 1024, (int)prop.GetValue(config)!);
    }

    [Theory]
    [InlineData("GroupCoordinatorCachedBufferMaxBytes")]
    [InlineData("ShareCoordinatorCachedBufferMaxBytes")]
    public void BrokerConfig_BelowLowerBound_FailsValidation(string propertyName)
    {
        var prop = Property(propertyName);
        var config = new BrokerConfig();
        prop.SetValue(config, UpstreamMin - 1);
        var results = new List<ValidationResult>();
        var ctx = new ValidationContext(config) { MemberName = propertyName };
        var valid = Validator.TryValidateProperty(prop.GetValue(config)!, ctx, results);
        Assert.False(valid);
    }
}
