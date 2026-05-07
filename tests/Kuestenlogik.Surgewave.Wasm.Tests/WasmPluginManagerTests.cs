using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kuestenlogik.Surgewave.Wasm.Tests;

public sealed class WasmPluginManagerTests : IAsyncDisposable
{
    private readonly string _tempDir;
    private readonly WasmPluginConfig _config;
    private readonly WasmRuntime _runtime;
    private readonly WasmPluginManager _manager;

    public WasmPluginManagerTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"surgewave-wasm-test-{Guid.NewGuid():N}");
        Directory.CreateDirectory(_tempDir);

        _config = new WasmPluginConfig
        {
            Enabled = true,
            WasmDirectory = _tempDir,
            EnableHotDeploy = false
        };

        var loggerFactory = NullLoggerFactory.Instance;
        _runtime = new WasmRuntime(_config, loggerFactory);
        _manager = new WasmPluginManager(_config, _runtime, loggerFactory.CreateLogger<WasmPluginManager>());
    }

    [Fact]
    public async Task DiscoverPlugins_EmptyDirectory_ReturnsEmpty()
    {
        var manifests = await _manager.DiscoverPluginsAsync();

        Assert.Empty(manifests);
    }

    [Fact]
    public async Task DiscoverPlugins_NonExistentDirectory_ReturnsEmpty()
    {
        var config = new WasmPluginConfig { WasmDirectory = "/nonexistent/path" };
        var loggerFactory = NullLoggerFactory.Instance;
        var runtime = new WasmRuntime(config, loggerFactory);
        var manager = new WasmPluginManager(config, runtime, loggerFactory.CreateLogger<WasmPluginManager>());

        var manifests = await manager.DiscoverPluginsAsync();

        Assert.Empty(manifests);

        await manager.DisposeAsync();
        runtime.Dispose();
    }

    [Fact]
    public async Task DiscoverPlugins_WithManifest_ReturnsManifest()
    {
        // Create a plugin subdirectory with a manifest
        var pluginDir = Path.Combine(_tempDir, "test-plugin");
        Directory.CreateDirectory(pluginDir);

        var manifestJson = """
        {
            "id": "test-plugin",
            "name": "Test Plugin",
            "version": "1.0.0",
            "type": "Transform"
        }
        """;
        File.WriteAllText(Path.Combine(pluginDir, "wasm-plugin.json"), manifestJson);

        var manifests = await _manager.DiscoverPluginsAsync();

        Assert.Single(manifests);
        Assert.Equal("test-plugin", manifests[0].Id);
        Assert.Equal("Test Plugin", manifests[0].Name);
        Assert.Equal("1.0.0", manifests[0].Version);
        Assert.Equal(WasmPluginType.Transform, manifests[0].Type);
    }

    [Fact]
    public async Task DiscoverPlugins_MultiplePlugins_ReturnsAll()
    {
        for (var i = 0; i < 3; i++)
        {
            var pluginDir = Path.Combine(_tempDir, $"plugin-{i}");
            Directory.CreateDirectory(pluginDir);

            var manifestJson = $$"""
            {
                "id": "plugin-{{i}}",
                "name": "Plugin {{i}}",
                "version": "1.0.0",
                "type": "Source"
            }
            """;
            File.WriteAllText(Path.Combine(pluginDir, "wasm-plugin.json"), manifestJson);
        }

        var manifests = await _manager.DiscoverPluginsAsync();

        Assert.Equal(3, manifests.Count);
    }

    [Fact]
    public void GetStatus_Empty_ReturnsEmptyList()
    {
        var status = _manager.GetStatus();

        Assert.Empty(status);
    }

    [Fact]
    public void GetPluginStatus_NotLoaded_ReturnsNull()
    {
        var status = _manager.GetPluginStatus("nonexistent");

        Assert.Null(status);
    }

    [Fact]
    public async Task LoadAndStart_NoPlugin_ThrowsFileNotFound()
    {
        await Assert.ThrowsAsync<FileNotFoundException>(
            () => _manager.LoadAndStartAsync("nonexistent-plugin"));
    }

    public async ValueTask DisposeAsync()
    {
        await _manager.DisposeAsync();
        _runtime.Dispose();

        try
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, recursive: true);
        }
        catch
        {
            // Cleanup best-effort
        }
    }
}
