using Kuestenlogik.Surgewave.Core.Configuration;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Core.Tests;

/// <summary>
/// Startup refuses an invalid configuration (#170).
/// </summary>
/// <remarks>
/// <para>
/// <see cref="ConfigValidator.ThrowIfInvalid"/> existed and had no callers, so every rule written
/// as a startup gate was reachable only through the <c>/api/config/validate</c> endpoint. A setting
/// whose wrong value looks like a cluster problem rather than a config problem stayed silent until
/// somebody thought to call it.
/// </para>
/// <para>
/// What is pinned here is the aggregation and the message, because that is what an operator acts
/// on: one exception naming every mistake, not the first one and then the next after a restart.
/// </para>
/// </remarks>
[Trait("Category", TestCategories.Unit)]
public sealed class StartupConfigValidationTests
{
    [Fact]
    public void AValidConfigurationPassesQuietly()
    {
        // The load-bearing case: every correct deployment goes through here on every start.
        ConfigValidator.ThrowIfAnyInvalid(new StubConfig(), new StubConfig());
    }

    [Fact]
    public void NoConfigurationsIsNotAFailure()
    {
        ConfigValidator.ThrowIfAnyInvalid();
    }

    [Fact]
    public void ANullConfigurationIsSkipped()
    {
        // Lets a caller pass a config that only exists when a feature is on, without branching.
        ConfigValidator.ThrowIfAnyInvalid(new StubConfig(), null);
    }

    [Fact]
    public void AnInvalidConfigurationStopsStartup()
    {
        var error = Assert.Throws<ConfigValidationException>(
            () => ConfigValidator.ThrowIfAnyInvalid(new StubConfig("Port must be positive.")));

        Assert.Contains("Port must be positive.", error.Message);
        Assert.Equal(typeof(StubConfig), error.ConfigType);
    }

    [Fact]
    public void EveryMistakeIsReportedAtOnce()
    {
        // The reason this aggregates rather than throwing on the first: an operator fixing a
        // deployment file should see all of it, not discover the next error on the next restart.
        var error = Assert.Throws<ConfigValidationException>(() => ConfigValidator.ThrowIfAnyInvalid(
            new StubConfig("first problem"),
            new StubConfig(),
            new OtherStubConfig("second problem", "third problem")));

        Assert.Equal(3, error.Errors.Count);
        Assert.Contains("first problem", error.Message);
        Assert.Contains("second problem", error.Message);
        Assert.Contains("third problem", error.Message);
    }

    [Fact]
    public void EachErrorNamesTheConfigurationItCameFrom()
    {
        // Two configs can both complain about "Port"; without the type an operator cannot tell
        // which file section to open.
        var error = Assert.Throws<ConfigValidationException>(() => ConfigValidator.ThrowIfAnyInvalid(
            new StubConfig("Port is wrong"),
            new OtherStubConfig("Port is wrong")));

        Assert.Contains($"{nameof(StubConfig)}: Port is wrong", error.Errors);
        Assert.Contains($"{nameof(OtherStubConfig)}: Port is wrong", error.Errors);
        Assert.Equal([typeof(StubConfig), typeof(OtherStubConfig)], error.ConfigTypes);
    }

    private sealed class StubConfig(params string[] errors) : IValidatableConfig
    {
        public IReadOnlyList<string> Validate() => errors;
    }

    private sealed class OtherStubConfig(params string[] errors) : IValidatableConfig
    {
        public IReadOnlyList<string> Validate() => errors;
    }
}
