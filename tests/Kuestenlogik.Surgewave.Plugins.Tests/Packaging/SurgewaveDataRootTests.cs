using Kuestenlogik.Surgewave.Plugins.Packaging;

namespace Kuestenlogik.Surgewave.Plugins.Tests.Packaging;

/// <summary>
/// The data-root rules from #158. These run against process-wide static state, so
/// the collection is serialised and every test restores what it changed.
/// </summary>
[Collection("SurgewaveDataRoot")]
public sealed class SurgewaveDataRootTests : IDisposable
{
    private readonly string? _root = Environment.GetEnvironmentVariable(SurgewaveDataRoot.OverrideVariable);
    private readonly string? _instance = Environment.GetEnvironmentVariable(SurgewaveDataRoot.InstanceVariable);

    public void Dispose()
    {
        Environment.SetEnvironmentVariable(SurgewaveDataRoot.OverrideVariable, _root);
        Environment.SetEnvironmentVariable(SurgewaveDataRoot.InstanceVariable, _instance);
        SurgewaveDataRoot.Configure();
    }

    [Fact]
    public void MachineAndUserAreDifferentDirectories()
    {
        Environment.SetEnvironmentVariable(SurgewaveDataRoot.OverrideVariable, null);
        SurgewaveDataRoot.Configure();

        // The whole point of the separation: a broker running as a service account
        // must not resolve to the profile of whoever installed a plugin.
        Assert.NotEqual(SurgewaveDataRoot.Machine, SurgewaveDataRoot.User);
    }

    [Fact]
    public void MachineScopeIsNotUnderTheUserProfile()
    {
        Environment.SetEnvironmentVariable(SurgewaveDataRoot.OverrideVariable, null);
        SurgewaveDataRoot.Configure();

        var home = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
        Assert.False(SurgewaveDataRoot.Machine.StartsWith(home, StringComparison.OrdinalIgnoreCase),
            $"machine scope resolved to {SurgewaveDataRoot.Machine}, which is inside {home}");
    }

    [Fact]
    public void NoInstanceMeansTheRootItself()
    {
        Environment.SetEnvironmentVariable(SurgewaveDataRoot.InstanceVariable, null);
        SurgewaveDataRoot.Configure(dataRoot: Path.Combine(Path.GetTempPath(), "sw-root"));

        Assert.Null(SurgewaveDataRoot.Instance);
        Assert.Equal(Path.Combine(Path.GetTempPath(), "sw-root"), SurgewaveDataRoot.Machine);
    }

    [Fact]
    public void InstanceAddsOneSegmentToEveryScope()
    {
        var root = Path.Combine(Path.GetTempPath(), "sw-root");
        SurgewaveDataRoot.Configure(dataRoot: root, instance: "beta");

        Assert.Equal(Path.Combine(root, "beta"), SurgewaveDataRoot.Machine);
        Assert.Equal(Path.Combine(root, "beta"), SurgewaveDataRoot.User);
    }

    [Fact]
    public void TwoInstancesDoNotShareState()
    {
        var root = Path.Combine(Path.GetTempPath(), "sw-root");

        SurgewaveDataRoot.Configure(dataRoot: root, instance: "alpha");
        var alpha = SurgewavePluginDirectories.Machine;

        SurgewaveDataRoot.Configure(dataRoot: root, instance: "beta");
        var beta = SurgewavePluginDirectories.Machine;

        Assert.NotEqual(alpha, beta);
    }

    [Fact]
    public void ConfigurationWinsOverTheEnvironment()
    {
        Environment.SetEnvironmentVariable(SurgewaveDataRoot.InstanceVariable, "from-env");
        SurgewaveDataRoot.Configure(instance: "from-config");

        Assert.Equal("from-config", SurgewaveDataRoot.Instance);
    }

    [Fact]
    public void EnvironmentAppliesWhenNothingWasConfigured()
    {
        Environment.SetEnvironmentVariable(SurgewaveDataRoot.InstanceVariable, "from-env");
        SurgewaveDataRoot.Configure();

        Assert.Equal("from-env", SurgewaveDataRoot.Instance);
    }

    [Theory]
    [InlineData("../escape")]
    [InlineData("nested/name")]
    [InlineData("..")]
    public void AnInstanceNameThatIsAPathIsRejected(string name)
    {
        // Otherwise SURGEWAVE_INSTANCE would be a second, undocumented way of
        // relocating the data root, with traversal thrown in.
        Assert.Throws<InvalidOperationException>(() => SurgewaveDataRoot.Configure(instance: name));
    }

    [Theory]
    [InlineData("plugins")]
    [InlineData("Connectors")]
    public void AnInstanceNamedAfterADirectoryTheRootOwnsIsRejected(string name)
    {
        // With no instance set the scope IS the root, so an instance called
        // "plugins" would keep its state in the unnamed instance's plugin directory.
        var ex = Assert.Throws<InvalidOperationException>(() => SurgewaveDataRoot.Configure(instance: name));
        Assert.Contains("Reserved names", ex.Message, StringComparison.Ordinal);
    }
}
