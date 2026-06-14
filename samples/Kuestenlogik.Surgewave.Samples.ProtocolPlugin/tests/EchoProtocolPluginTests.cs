using Microsoft.Extensions.Configuration;
using Xunit;

namespace Kuestenlogik.Surgewave.Samples.ProtocolPlugin.Tests;

public sealed class EchoProtocolPluginTests
{
    [Fact]
    public void Plugin_class_is_sealed()
    {
        Assert.True(typeof(EchoProtocolPlugin).IsSealed);
    }

    [Fact]
    public void Plugin_has_parameterless_constructor()
    {
        Assert.NotNull(typeof(EchoProtocolPlugin).GetConstructor(Type.EmptyTypes));
    }

    [Fact]
    public void DefaultPort_is_zero_signalling_HTTP_host_sharing()
    {
        var plugin = new EchoProtocolPlugin();
        Assert.Equal(0, plugin.DefaultPort);
    }

    [Fact]
    public void IsConfigEnabled_defaults_to_true()
    {
        var plugin = new EchoProtocolPlugin();
        var config = new ConfigurationBuilder().AddInMemoryCollection().Build();
        Assert.True(plugin.IsConfigEnabled(config));
    }
}
