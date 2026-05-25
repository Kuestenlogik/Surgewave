using Kuestenlogik.Surgewave.Plugins.Packaging;

namespace Kuestenlogik.Surgewave.Plugins.Tests.Packaging;

public sealed class SurgewavePackageConventionsTests
{
    [Theory]
    [InlineData("Kuestenlogik.Surgewave.Core")]
    [InlineData("Kuestenlogik.Surgewave.Client")]
    [InlineData("Kuestenlogik.Surgewave.Storage.Engine.Memory")]
    [InlineData("kuestenlogik.surgewave.lowercase")] // case-insensitive
    public void IsSurgewaveHostAssembly_PrefixMatches(string name)
    {
        Assert.True(SurgewavePackageConventions.IsSurgewaveHostAssembly(name));
    }

    [Theory]
    [InlineData("surgewave-broker")]
    [InlineData("surgewave-control")]
    [InlineData("surgewave-gateway")]
    [InlineData("SURGEWAVE-BROKER")] // case-insensitive
    public void IsSurgewaveHostAssembly_ExecutablePrefixMatches(string name)
    {
        Assert.True(SurgewavePackageConventions.IsSurgewaveHostAssembly(name));
    }

    [Theory]
    [InlineData("Newtonsoft.Json")]
    [InlineData("MQTTnet")]
    [InlineData("System.Text.Json")]
    [InlineData("")]
    [InlineData("Surgewave.Other")]  // missing Kuestenlogik prefix
    public void IsSurgewaveHostAssembly_ThirdParty_ReturnsFalse(string name)
    {
        Assert.False(SurgewavePackageConventions.IsSurgewaveHostAssembly(name));
    }

    [Fact]
    public void IsSurgewaveHostAssembly_Null_ReturnsFalse()
    {
        Assert.False(SurgewavePackageConventions.IsSurgewaveHostAssembly(null));
    }

    [Fact]
    public void ManifestFileName_IsPluginJson()
    {
        Assert.Equal("plugin.json", SurgewavePackageConventions.ManifestFileName);
    }

    [Fact]
    public void PluginSettingsFileName_IsPluginSettingsJson()
    {
        Assert.Equal("pluginsettings.json", SurgewavePackageConventions.PluginSettingsFileName);
    }

    [Fact]
    public void HostAssemblyPrefix_IsKuestenlogikSurgewave()
    {
        Assert.Equal("Kuestenlogik.Surgewave.", SurgewavePackageConventions.HostAssemblyPrefix);
    }

    [Fact]
    public void ExecutableAssemblyPrefix_IsSurgewaveKebab()
    {
        Assert.Equal("surgewave-", SurgewavePackageConventions.ExecutableAssemblyPrefix);
    }
}
