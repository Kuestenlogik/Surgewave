using Microsoft.Extensions.Logging.Abstractions;

namespace Kuestenlogik.Surgewave.Wasm.Tests;

/// <summary>
/// Tests for WasmRuntime configuration and lifecycle.
/// </summary>
public sealed class WasmRuntimeTests : IDisposable
{
    private readonly WasmRuntime _runtime;
    private readonly WasmPluginConfig _config;

    public WasmRuntimeTests()
    {
        _config = new WasmPluginConfig
        {
            Enabled = true,
            MaxMemoryBytes = 32 * 1024 * 1024,
            ExecutionTimeout = TimeSpan.FromSeconds(10)
        };
        _runtime = new WasmRuntime(_config, NullLoggerFactory.Instance);
    }

    [Fact]
    public void Constructor_CreatesEngine()
    {
        // If the constructor succeeded, the engine is created
        Assert.NotNull(_runtime);
    }

    [Fact]
    public void LoadPlugin_NonExistentPath_ThrowsFileNotFound()
    {
        var manifest = new WasmPluginManifest
        {
            Id = "test",
            Name = "Test",
            Version = "1.0.0",
            Type = WasmPluginType.Transform
        };

        Assert.Throws<FileNotFoundException>(() =>
            _runtime.LoadPlugin("/nonexistent/path/plugin.wasm", manifest));
    }

    [Fact]
    public void LoadPlugin_NullManifest_ThrowsArgumentNull()
    {
        Assert.Throws<ArgumentNullException>(() =>
            _runtime.LoadPlugin("some/path.wasm", null!));
    }

    [Fact]
    public void LoadPluginFromDirectory_NoManifest_ThrowsFileNotFound()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), $"surgewave-wasm-rt-{Guid.NewGuid():N}");
        Directory.CreateDirectory(tempDir);

        try
        {
            Assert.Throws<FileNotFoundException>(() =>
                _runtime.LoadPluginFromDirectory(tempDir));
        }
        finally
        {
            Directory.Delete(tempDir, recursive: true);
        }
    }

    [Fact]
    public void Dispose_MultipleTimes_DoesNotThrow()
    {
        var config = new WasmPluginConfig();
        var runtime = new WasmRuntime(config, NullLoggerFactory.Instance);

        runtime.Dispose();
        runtime.Dispose();
    }

    [Fact]
    public void LoadPlugin_AfterDispose_ThrowsObjectDisposed()
    {
        var config = new WasmPluginConfig();
        var runtime = new WasmRuntime(config, NullLoggerFactory.Instance);
        runtime.Dispose();

        var manifest = new WasmPluginManifest
        {
            Id = "test",
            Name = "Test",
            Version = "1.0.0",
            Type = WasmPluginType.Transform
        };

        Assert.Throws<ObjectDisposedException>(() =>
            runtime.LoadPlugin("test.wasm", manifest));
    }

    [Fact]
    public void Constructor_WithProduceCallback_DoesNotThrow()
    {
        var config = new WasmPluginConfig();
        using var runtime = new WasmRuntime(config, NullLoggerFactory.Instance,
            (topic, key, value) => { });

        Assert.NotNull(runtime);
    }

    public void Dispose()
    {
        _runtime.Dispose();
    }
}
