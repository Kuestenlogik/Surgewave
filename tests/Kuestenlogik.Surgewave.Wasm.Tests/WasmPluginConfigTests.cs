namespace Kuestenlogik.Surgewave.Wasm.Tests;

public sealed class WasmPluginConfigTests
{
    [Fact]
    public void Defaults_AreCorrect()
    {
        var config = new WasmPluginConfig();

        Assert.False(config.Enabled);
        Assert.Equal("wasm-plugins", config.WasmDirectory);
        Assert.Equal(64 * 1024 * 1024, config.MaxMemoryBytes);
        Assert.Equal(TimeSpan.FromSeconds(30), config.ExecutionTimeout);
        Assert.False(config.AllowFileAccess);
        Assert.False(config.AllowNetworkAccess);
        Assert.True(config.EnableHotDeploy);
        Assert.Equal(TimeSpan.FromSeconds(2), config.HotDeployDebounce);
    }

    [Fact]
    public void SectionName_IsCorrect()
    {
        Assert.Equal("Surgewave:Wasm", WasmPluginConfig.SectionName);
    }

    [Fact]
    public void Properties_AreSettable()
    {
        var config = new WasmPluginConfig
        {
            Enabled = true,
            WasmDirectory = "/custom/path",
            MaxMemoryBytes = 128 * 1024 * 1024,
            ExecutionTimeout = TimeSpan.FromMinutes(1),
            AllowFileAccess = true,
            AllowNetworkAccess = true,
            EnableHotDeploy = false,
            HotDeployDebounce = TimeSpan.FromSeconds(5)
        };

        Assert.True(config.Enabled);
        Assert.Equal("/custom/path", config.WasmDirectory);
        Assert.Equal(128 * 1024 * 1024, config.MaxMemoryBytes);
        Assert.Equal(TimeSpan.FromMinutes(1), config.ExecutionTimeout);
        Assert.True(config.AllowFileAccess);
        Assert.True(config.AllowNetworkAccess);
        Assert.False(config.EnableHotDeploy);
        Assert.Equal(TimeSpan.FromSeconds(5), config.HotDeployDebounce);
    }
}
