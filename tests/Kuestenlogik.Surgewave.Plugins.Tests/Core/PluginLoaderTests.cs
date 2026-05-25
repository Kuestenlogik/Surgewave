using Kuestenlogik.Surgewave.Plugins;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kuestenlogik.Surgewave.Plugins.Tests.Core;

/// <summary>
/// Tests fuer <see cref="PluginLoader"/>. Wir nehmen die Test-Assembly selbst als
/// "Plugin"-Assembly — dadurch koennen wir LoadPlugin/Unload/CreateInstance ohne
/// extra Plugin-DLL pruefen.
/// </summary>
public sealed class PluginLoaderTests
{
    public interface IFakeService
    {
        string Hello();
    }

    public sealed class FakeImpl : IFakeService
    {
        public string Hello() => "hi";
    }

    private static string TestAssemblyPath => typeof(PluginLoaderTests).Assembly.Location;

    [Fact]
    public void Constructor_Logger_DoesNotThrow()
    {
        using var loader = new PluginLoader(NullLogger<PluginLoader>.Instance);

        Assert.Empty(loader.GetLoadedAssemblies());
    }

    [Fact]
    public void LoadPlugin_ValidAssembly_ReturnsAssembly()
    {
        using var loader = new PluginLoader(NullLogger<PluginLoader>.Instance);

        var asm = loader.LoadPlugin(TestAssemblyPath);

        Assert.NotNull(asm);
        Assert.Contains(asm!, loader.GetLoadedAssemblies());
    }

    [Fact]
    public void LoadPlugin_SamePathTwice_ReturnsCachedInstance()
    {
        using var loader = new PluginLoader(NullLogger<PluginLoader>.Instance);

        var first = loader.LoadPlugin(TestAssemblyPath);
        var second = loader.LoadPlugin(TestAssemblyPath);

        Assert.Same(first, second);
        Assert.Single(loader.GetLoadedAssemblies());
    }

    [Fact]
    public void LoadPlugin_NonExistentPath_ReturnsNull()
    {
        using var loader = new PluginLoader(NullLogger<PluginLoader>.Instance);

        var asm = loader.LoadPlugin(Path.Combine(Path.GetTempPath(), "does-not-exist-asm.dll"));

        Assert.Null(asm);
        Assert.Empty(loader.GetLoadedAssemblies());
    }

    [Fact]
    public void UnloadPlugin_LoadedAssembly_ReturnsTrue()
    {
        using var loader = new PluginLoader(NullLogger<PluginLoader>.Instance);
        loader.LoadPlugin(TestAssemblyPath);

        var removed = loader.UnloadPlugin(TestAssemblyPath);

        Assert.True(removed);
        Assert.Empty(loader.GetLoadedAssemblies());
    }

    [Fact]
    public void UnloadPlugin_UnknownPath_ReturnsFalse()
    {
        using var loader = new PluginLoader(NullLogger<PluginLoader>.Instance);

        Assert.False(loader.UnloadPlugin("nothing.dll"));
    }

    [Fact]
    public void CreateInstance_UnknownType_ReturnsNull()
    {
        using var loader = new PluginLoader(NullLogger<PluginLoader>.Instance);
        loader.LoadPlugin(TestAssemblyPath);

        var instance = loader.CreateInstance<IFakeService>("Nothing.Here");

        Assert.Null(instance);
    }

    [Fact]
    public void CreateInstance_FallbackToTypeGetType_BuiltInType_Works()
    {
        // No assemblies loaded — falls back to Type.GetType. The built-in
        // List<string> has a public parameterless ctor and assignability to its
        // own interfaces, so the fallback path executes through to a successful
        // Activator.CreateInstance.
        using var loader = new PluginLoader(NullLogger<PluginLoader>.Instance);

        var list = loader.CreateInstance<System.Collections.IList>(
            "System.Collections.Generic.List`1[[System.String, System.Private.CoreLib]], System.Private.CoreLib");

        Assert.NotNull(list);
    }

    [Fact]
    public void Dispose_UnloadsAllAssemblies()
    {
        var loader = new PluginLoader(NullLogger<PluginLoader>.Instance);
        loader.LoadPlugin(TestAssemblyPath);
        Assert.NotEmpty(loader.GetLoadedAssemblies());

        loader.Dispose();

        Assert.Empty(loader.GetLoadedAssemblies());
    }
}
