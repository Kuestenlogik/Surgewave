using System.Reflection;
using Kuestenlogik.Surgewave.Plugins;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kuestenlogik.Surgewave.Plugins.Tests.Core;

/// <summary>
/// Tests fuer <see cref="PluginDiscovery"/> — Registrierung von Built-in-Assemblies,
/// Verzeichnis-Scan, Dedupe via GetAllPlugins, LoadPluginType-Fallback und die
/// internal-aber-rein-funktionalen Helpers (DeriveCategory, ImplementsPluginInterface,
/// DeterminePluginType).
/// </summary>
public sealed class PluginDiscoveryTests : IDisposable
{
    private readonly string _tempDir;

    public PluginDiscoveryTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"sw-discovery-tests-{Guid.NewGuid():N}");
    }

    private static PluginDiscovery CreateDiscovery()
    {
        var loader = new PluginLoader(NullLogger<PluginLoader>.Instance);
        return new PluginDiscovery(loader, NullLogger<PluginDiscovery>.Instance);
    }

    [Fact]
    public void NewDiscovery_HasNoPlugins()
    {
        var d = CreateDiscovery();

        Assert.Empty(d.GetDiscoveredPlugins());
    }

    [Fact]
    public void RegisterBuiltInAssembly_AssemblyOnce_DoesNotDuplicate()
    {
        var d = CreateDiscovery();
        var asm = typeof(PluginDiscoveryTests).Assembly;

        d.RegisterBuiltInAssembly(asm);
        d.RegisterBuiltInAssembly(asm);

        // No public way to count built-ins, but GetAllPlugins must not duplicate
        var all = d.GetAllPlugins();
        var distinctClasses = all.Select(p => p.Class).Distinct().Count();
        Assert.Equal(distinctClasses, all.Count);
    }

    [Fact]
    public void RegisterBuiltInAssembly_Generic_ResolvesToTypesAssembly()
    {
        var d = CreateDiscovery();

        // Should not throw — generic form should resolve typeof(T).Assembly internally.
        d.RegisterBuiltInAssembly<PluginDiscoveryTests>();
    }

    [Fact]
    public void DiscoverPlugins_NonExistentDirectory_NoCrash()
    {
        var d = CreateDiscovery();

        d.DiscoverPlugins(Path.Combine(_tempDir, "missing"));

        Assert.Empty(d.GetDiscoveredPlugins());
    }

    [Fact]
    public void DiscoverPlugins_EmptyDirectory_NoPlugins()
    {
        Directory.CreateDirectory(_tempDir);

        var d = CreateDiscovery();
        d.DiscoverPlugins(_tempDir);

        Assert.Empty(d.GetDiscoveredPlugins());
    }

    [Fact]
    public void GetPluginLoader_ReturnsLoaderPassedIn()
    {
        var loader = new PluginLoader(NullLogger<PluginLoader>.Instance);
        var d = new PluginDiscovery(loader, NullLogger<PluginDiscovery>.Instance);

        Assert.Same(loader, d.GetPluginLoader());
    }

    [Fact]
    public void LoadPluginType_UnknownClass_ReturnsNull()
    {
        var d = CreateDiscovery();

        var type = d.LoadPluginType("Nothing.Known.Here.Ever");

        Assert.Null(type);
    }

    [Fact]
    public void HotSwapPlugin_NonExistentDir_DoesNotThrow()
    {
        var d = CreateDiscovery();

        var added = d.HotSwapPlugin(Path.Combine(_tempDir, "no-such"));

        Assert.True(added >= 0);
    }

    // --- Internal helpers via reflection — they steer Discovery results so worth covering. ---

    private static T InvokeInternalStatic<T>(string methodName, params object[] args)
    {
        var m = typeof(PluginDiscovery).GetMethod(methodName,
            BindingFlags.NonPublic | BindingFlags.Static)!;
        return (T)m.Invoke(null, args)!;
    }

    [Theory]
    [InlineData("chat, bot", "Messaging")]
    [InlineData("messaging", "Messaging")]
    [InlineData("database", "Database")]
    [InlineData("sql", "Database")]
    [InlineData("cloud, aws", "Cloud")]
    [InlineData("azure", "Cloud")]
    [InlineData("ai, llm", "AI")]
    [InlineData("ml", "AI")]
    [InlineData("iot", "IoT")]
    [InlineData("smart-home", "IoT")]
    [InlineData("social", "Social")]
    [InlineData("streaming", "Streaming")]
    [InlineData("file, storage", "Storage")]
    [InlineData("queue", "Queue")]
    [InlineData("search", "Search")]
    [InlineData("graph", "Graph")]
    [InlineData("time-series", "TimeSeries")]
    [InlineData("protocol", "Transport")]
    [InlineData("logic", "Logic")]
    [InlineData("foo, bar, baz", "Integration")]
    [InlineData("", "Integration")]
    public void DeriveCategory_KnownTagSets_ReturnsExpectedCategory(string tags, string expected)
    {
        var result = InvokeInternalStatic<string>("DeriveCategory", tags);

        Assert.Equal(expected, result);
    }

    [Fact]
    public void ImplementsPluginInterface_TypeImplementingIPlugin_ReturnsTrue()
    {
        // FakeService implements IPlugin via FeatureId/DisplayName
        var result = InvokeInternalStatic<bool>("ImplementsPluginInterface", typeof(FakePluginNode));

        Assert.True(result);
    }

    [Fact]
    public void ImplementsPluginInterface_PlainPoco_ReturnsFalse()
    {
        var result = InvokeInternalStatic<bool>("ImplementsPluginInterface", typeof(string));

        Assert.False(result);
    }

    [Fact]
    public void DeterminePluginType_BySinkSinkInterface_ReturnsSink()
    {
        var result = InvokeInternalStatic<string>("DeterminePluginType", typeof(FakeSinkNode));

        Assert.Equal("sink", result);
    }

    [Theory]
    [InlineData("MyFooSource", "source")]
    [InlineData("MyBarSink", "sink")]
    [InlineData("MyProducer", "source")]
    [InlineData("MyConsumer", "sink")]
    [InlineData("MyWriter", "sink")]
    [InlineData("MyReader", "source")]
    public void DeterminePluginType_FallsBackToNameHeuristic(string typeName, string expected)
    {
        // Construct a fresh runtime type with the desired name via TypeBuilder.
        var asmName = new AssemblyName($"DynAsm_{Guid.NewGuid():N}");
        var asmBuilder = System.Reflection.Emit.AssemblyBuilder.DefineDynamicAssembly(
            asmName, System.Reflection.Emit.AssemblyBuilderAccess.Run);
        var modBuilder = asmBuilder.DefineDynamicModule("Mod");
        var typeBuilder = modBuilder.DefineType(typeName, TypeAttributes.Public);
        typeBuilder.AddInterfaceImplementation(typeof(IPlugin));
        // Implement properties to satisfy IPlugin shape (interface contract isn't enforced
        // by DeterminePluginType — it only inspects metadata).
        var fid = typeBuilder.DefineProperty("FeatureId", PropertyAttributes.None, typeof(string), null);
        var dn = typeBuilder.DefineProperty("DisplayName", PropertyAttributes.None, typeof(string), null);
        var getFid = typeBuilder.DefineMethod("get_FeatureId",
            MethodAttributes.Public | MethodAttributes.Virtual,
            typeof(string), Type.EmptyTypes);
        var il1 = getFid.GetILGenerator();
        il1.Emit(System.Reflection.Emit.OpCodes.Ldnull);
        il1.Emit(System.Reflection.Emit.OpCodes.Ret);
        var getDn = typeBuilder.DefineMethod("get_DisplayName",
            MethodAttributes.Public | MethodAttributes.Virtual,
            typeof(string), Type.EmptyTypes);
        var il2 = getDn.GetILGenerator();
        il2.Emit(System.Reflection.Emit.OpCodes.Ldnull);
        il2.Emit(System.Reflection.Emit.OpCodes.Ret);
        fid.SetGetMethod(getFid);
        dn.SetGetMethod(getDn);
        typeBuilder.DefineMethodOverride(getFid, typeof(IPlugin).GetMethod("get_FeatureId")!);
        typeBuilder.DefineMethodOverride(getDn, typeof(IPlugin).GetMethod("get_DisplayName")!);
        var dynType = typeBuilder.CreateType();

        var result = InvokeInternalStatic<string>("DeterminePluginType", dynType);

        Assert.Equal(expected, result);
    }

    public void Dispose()
    {
        if (Directory.Exists(_tempDir))
        {
            try { Directory.Delete(_tempDir, recursive: true); }
            catch { /* best effort */ }
        }
    }

    // Test-fixtures wired up so PluginDiscovery can find them via the Test-Assembly.
    public sealed class FakePluginNode : IPlugin
    {
        public string FeatureId => "fake";
        public string DisplayName => "Fake";
    }

    public sealed class FakeSinkNode : Kuestenlogik.Surgewave.Plugins.Pipeline.ISinkNode
    {
        public string FeatureId => "fake-sink";
        public string DisplayName => "Fake Sink";
        public int InputPorts => 1;
        public int OutputPorts => 0;
        public Kuestenlogik.Surgewave.Plugins.Configuration.ConfigDef Config { get; } = new();
        public string Version => "1.0.0";
    }
}
