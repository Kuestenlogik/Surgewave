using System.Reflection;
using Kuestenlogik.Surgewave.Plugins;

namespace Kuestenlogik.Surgewave.Plugins.Tests.Core;

/// <summary>
/// Tests fuer <see cref="PluginAssemblyScanner"/>. Verwendet die Test-Assembly selbst als
/// Scan-Target, so dass keine zusaetzlichen Plugin-DLLs gepackt werden muessen.
/// </summary>
public sealed class PluginAssemblyScannerTests
{
    public interface IFakeServiceForScanner
    {
        string Tag { get; }
    }

    public sealed class FakeImplA : IFakeServiceForScanner
    {
        public string Tag => "A";
    }

    public sealed class FakeImplB : IFakeServiceForScanner
    {
        public string Tag => "B";
    }

    public abstract class AbstractImpl : IFakeServiceForScanner
    {
        public string Tag => "abstract";
    }

    public sealed class CtorThrowsImpl : IFakeServiceForScanner
    {
        public CtorThrowsImpl() => throw new InvalidOperationException("intentional");
        public string Tag => "throws";
    }

    public sealed class NoParameterlessCtor : IFakeServiceForScanner
    {
        public NoParameterlessCtor(string _) { }
        public string Tag => "no-default-ctor";
    }

    [Fact]
    public void FindImplementations_FindsConcreteImpls_SkipsAbstractAndCtorIssues()
    {
        var assemblies = new[] { typeof(PluginAssemblyScannerTests).Assembly };

        var impls = PluginAssemblyScanner
            .FindImplementations<IFakeServiceForScanner>(assemblies)
            .ToList();

        var tags = impls.Select(i => i.Tag).ToHashSet();
        Assert.Contains("A", tags);
        Assert.Contains("B", tags);
        Assert.DoesNotContain("abstract", tags);
        Assert.DoesNotContain("throws", tags);
        Assert.DoesNotContain("no-default-ctor", tags);
    }

    [Fact]
    public void FindImplementations_EmptyAssemblyList_YieldsNothing()
    {
        var impls = PluginAssemblyScanner
            .FindImplementations<IFakeServiceForScanner>([])
            .ToList();

        Assert.Empty(impls);
    }

    [Fact]
    public void FindImplementations_NoMatchingImplementations_YieldsNothing()
    {
        // Use the System.Runtime assembly as a target; no implementations for our
        // local interface live there.
        var sysRuntime = typeof(object).Assembly;

        var impls = PluginAssemblyScanner
            .FindImplementations<IFakeServiceForScanner>([sysRuntime])
            .ToList();

        Assert.Empty(impls);
    }
}
