using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Kuestenlogik.Surgewave.Wasm.Tests;

/// <summary>
/// Extended tests for WasmPluginManager lifecycle and discovery.
/// </summary>
public sealed class WasmPluginManagerExtendedTests : IAsyncDisposable
{
    private readonly string _tempDir;
    private readonly WasmPluginConfig _config;
    private readonly WasmRuntime _runtime;
    private readonly WasmPluginManager _manager;

    public WasmPluginManagerExtendedTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), $"surgewave-wasm-ext-{Guid.NewGuid():N}");
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
    public void GetPlugin_NotLoaded_ReturnsNull()
    {
        var plugin = _manager.GetPlugin("nonexistent");

        Assert.Null(plugin);
    }

    [Fact]
    public async Task StopAsync_NotLoaded_DoesNotThrow()
    {
        await _manager.StopAsync("nonexistent-plugin");
    }

    [Fact]
    public async Task DiscoverPlugins_InvalidManifestJson_SkipsPlugin()
    {
        var pluginDir = Path.Combine(_tempDir, "bad-plugin");
        Directory.CreateDirectory(pluginDir);
        File.WriteAllText(Path.Combine(pluginDir, "wasm-plugin.json"), "{ invalid json }}}");

        var manifests = await _manager.DiscoverPluginsAsync();

        Assert.Empty(manifests);
    }

    [Fact]
    public async Task DiscoverPlugins_SubdirWithoutManifest_SkipsIt()
    {
        var pluginDir = Path.Combine(_tempDir, "no-manifest");
        Directory.CreateDirectory(pluginDir);
        File.WriteAllText(Path.Combine(pluginDir, "readme.txt"), "No manifest here");

        var manifests = await _manager.DiscoverPluginsAsync();

        Assert.Empty(manifests);
    }

    [Fact]
    public async Task DiscoverPlugins_MixedValidAndInvalid_ReturnsOnlyValid()
    {
        var validDir = Path.Combine(_tempDir, "valid-plugin");
        Directory.CreateDirectory(validDir);
        File.WriteAllText(Path.Combine(validDir, "wasm-plugin.json"), """
        {
            "id": "valid",
            "name": "Valid Plugin",
            "version": "1.0.0",
            "type": "Sink"
        }
        """);

        var invalidDir = Path.Combine(_tempDir, "invalid-plugin");
        Directory.CreateDirectory(invalidDir);
        File.WriteAllText(Path.Combine(invalidDir, "wasm-plugin.json"), "not-json");

        var noManifestDir = Path.Combine(_tempDir, "empty-dir");
        Directory.CreateDirectory(noManifestDir);

        var manifests = await _manager.DiscoverPluginsAsync();

        Assert.Single(manifests);
        Assert.Equal("valid", manifests[0].Id);
    }

    [Fact]
    public async Task DisposeAsync_MultipleTimes_DoesNotThrow()
    {
        var config = new WasmPluginConfig { WasmDirectory = _tempDir };
        var loggerFactory = NullLoggerFactory.Instance;
        var runtime = new WasmRuntime(config, loggerFactory);
        var manager = new WasmPluginManager(config, runtime, loggerFactory.CreateLogger<WasmPluginManager>());

        await manager.DisposeAsync();
        await manager.DisposeAsync();

        runtime.Dispose();
    }

    [Fact]
    public async Task LoadAndStart_AfterDispose_ThrowsObjectDisposed()
    {
        var config = new WasmPluginConfig { WasmDirectory = _tempDir };
        var loggerFactory = NullLoggerFactory.Instance;
        var runtime = new WasmRuntime(config, loggerFactory);
        var manager = new WasmPluginManager(config, runtime, loggerFactory.CreateLogger<WasmPluginManager>());

        await manager.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            manager.LoadAndStartAsync("some-plugin"));

        runtime.Dispose();
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
