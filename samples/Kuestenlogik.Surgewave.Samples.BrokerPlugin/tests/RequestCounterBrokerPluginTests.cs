using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Kuestenlogik.Surgewave.Samples.BrokerPlugin.Tests;

public sealed class RequestCounterBrokerPluginTests
{
    [Fact]
    public void Plugin_class_is_sealed()
    {
        Assert.True(typeof(RequestCounterBrokerPlugin).IsSealed);
    }

    [Fact]
    public void Plugin_has_parameterless_constructor()
    {
        var ctor = typeof(RequestCounterBrokerPlugin).GetConstructor(Type.EmptyTypes);
        Assert.NotNull(ctor);
    }

    [Fact]
    public void IsConfigEnabled_defaults_to_true()
    {
        var plugin = new RequestCounterBrokerPlugin();
        var config = new ConfigurationBuilder().AddInMemoryCollection().Build();

        Assert.True(plugin.IsConfigEnabled(config));
    }

    [Fact]
    public void IsConfigEnabled_respects_Enabled_false()
    {
        var plugin = new RequestCounterBrokerPlugin();
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(new[] { new KeyValuePair<string, string?>("SampleBrokerPlugin:Enabled", "false") })
            .Build();

        Assert.False(plugin.IsConfigEnabled(config));
    }

    [Fact]
    public void ConfigureServices_registers_RequestCounter_as_singleton()
    {
        var plugin = new RequestCounterBrokerPlugin();
        var services = new ServiceCollection();
        var config = new ConfigurationBuilder().Build();

        plugin.ConfigureServices(services, config);

        var provider = services.BuildServiceProvider();
        var counter1 = provider.GetRequiredService<RequestCounter>();
        var counter2 = provider.GetRequiredService<RequestCounter>();

        Assert.Same(counter1, counter2);
        counter1.Increment();
        Assert.Equal(1, counter2.Count);
    }
}
