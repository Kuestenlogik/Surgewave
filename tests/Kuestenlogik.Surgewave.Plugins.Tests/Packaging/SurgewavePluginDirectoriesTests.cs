using Kuestenlogik.Surgewave.Plugins.Packaging;

namespace Kuestenlogik.Surgewave.Plugins.Tests.Packaging;

/// <summary>Search-order rules from #158.</summary>
[Collection("SurgewaveDataRoot")]
public sealed class SurgewavePluginDirectoriesTests : IDisposable
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
    public void AnExplicitDirectoryIsTheOnlyOneSearched()
    {
        // A container image mounts one directory and means it. Quietly adding the
        // host's scopes would make the image behave differently per host.
        var order = SurgewavePluginDirectories.SearchOrder("/mnt/plugins");

        Assert.Single(order);
        Assert.Equal(Path.GetFullPath("/mnt/plugins"), order[0]);
    }

    [Fact]
    public void PrecedenceRunsInstallationThenMachineThenUser()
    {
        Environment.SetEnvironmentVariable(SurgewaveDataRoot.OverrideVariable, null);
        SurgewaveDataRoot.Configure();

        var order = SurgewavePluginDirectories.SearchOrder();

        Assert.Equal(3, order.Count);
        Assert.Equal(Path.GetFullPath(SurgewavePluginDirectories.Installation), order[0]);
        Assert.Equal(Path.GetFullPath(SurgewavePluginDirectories.Machine), order[1]);
        Assert.Equal(Path.GetFullPath(SurgewavePluginDirectories.User), order[2]);
    }

    [Fact]
    public void ARedirectedRootDoesNotListTheSameDirectoryTwice()
    {
        // SURGEWAVE_DATA_DIR collapses machine and user onto one root. Without the
        // de-duplication every plugin in it would be reported as shadowing itself.
        SurgewaveDataRoot.Configure(dataRoot: Path.Combine(Path.GetTempPath(), "sw-order"));

        var order = SurgewavePluginDirectories.SearchOrder();

        Assert.Equal(order.Count, order.Distinct(StringComparer.OrdinalIgnoreCase).Count());
    }

    [Fact]
    public void TheUserScopeCanBeRefused()
    {
        Environment.SetEnvironmentVariable(SurgewaveDataRoot.OverrideVariable, null);
        SurgewaveDataRoot.Configure();

        // A plugin in a home directory is code the host runs that an unprivileged
        // account could place there; the machine scope needs elevation and a home
        // directory does not. An administrator deploying a service must be able to
        // refuse it.
        var order = SurgewavePluginDirectories.SearchOrder(allowUserScope: false);

        Assert.DoesNotContain(Path.GetFullPath(SurgewavePluginDirectories.User), order);
        Assert.Contains(Path.GetFullPath(SurgewavePluginDirectories.Machine), order);
        Assert.Contains(Path.GetFullPath(SurgewavePluginDirectories.Installation), order);
    }

    [Fact]
    public void RefusingTheUserScopeDoesNotAffectAnExplicitDirectory()
    {
        // An explicit directory is the administrator's own choice, so the switch
        // that protects them from a user's directory has nothing to say about it.
        var order = SurgewavePluginDirectories.SearchOrder("/mnt/plugins", allowUserScope: false);

        Assert.Single(order);
        Assert.Equal(Path.GetFullPath("/mnt/plugins"), order[0]);
    }

    [Fact]
    public void ResolveAgreesWithTheProperties()
    {
        Assert.Equal(SurgewavePluginDirectories.Installation, SurgewavePluginDirectories.Resolve(PluginScope.Installation));
        Assert.Equal(SurgewavePluginDirectories.Machine, SurgewavePluginDirectories.Resolve(PluginScope.Machine));
        Assert.Equal(SurgewavePluginDirectories.User, SurgewavePluginDirectories.Resolve(PluginScope.User));
    }
}
