using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// The broker's in-process components (the Connect worker's loopback client) connect to the broker's own
/// listener. With Host set to an interface address the listener binds there only, and the fixed
/// "localhost" they used was refused — the broker did not start.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class LocalListenerAddressTests
{
    [Theory]
    [InlineData("localhost", "localhost:19092")]
    [InlineData("192.168.1.99", "192.168.1.99:19092")]
    [InlineData("0.0.0.0", "localhost:19092")]
    [InlineData("::", "localhost:19092")]
    [InlineData("::1", "[::1]:19092")]
    public void IsTheListenerAddress_OrLoopbackForAWildcard(string host, string expected)
    {
        var config = new BrokerConfig { Host = host, Port = 19092 };

        Assert.Equal(expected, config.LocalListenerAddress());
    }
}
