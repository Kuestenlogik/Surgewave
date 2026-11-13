using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Broker.Tests;

/// <summary>
/// A broker in a container or a pod is known by a DNS name and binds no fixed address. Host was both the address the
/// client listener binds and the one clients are told, so a name there did not start ('An invalid IP address was
/// specified') and a wildcard there was what clients were told to connect to. BindAddress says what to bind and
/// leaves Host as the name.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class BindAddressTests
{
    [Fact]
    public void ANameInHostWithoutABindAddress_IsRefusedWithWhatToSet()
    {
        var config = new BrokerConfig { Host = "broker-1.brokers.example" };

        var error = Assert.Single(config.Validate(), e => e.Contains(nameof(BrokerConfig.BindAddress), StringComparison.Ordinal));
        Assert.Contains("broker-1.brokers.example", error, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("localhost", null)]
    [InlineData("192.168.1.99", null)]
    [InlineData("::1", null)]
    [InlineData("broker-1.brokers.example", "0.0.0.0")]
    [InlineData("broker-1.brokers.example", "::")]
    public void AnAddressToBind_IsAccepted(string host, string? bindAddress)
    {
        var config = new BrokerConfig { Host = host, BindAddress = bindAddress };

        Assert.DoesNotContain(config.Validate(), e => e.Contains(nameof(BrokerConfig.BindAddress), StringComparison.Ordinal));
    }

    [Fact]
    public void ABindAddressThatIsNoAddress_IsRefused()
    {
        var config = new BrokerConfig { Host = "broker-1.brokers.example", BindAddress = "eth0" };

        Assert.Contains(config.Validate(), e => e.Contains("'eth0' is not an IP address", StringComparison.Ordinal));
    }

    [Fact]
    public void AReplicationBindAddressThatIsNoAddress_IsRefused()
    {
        var config = new BrokerConfig { ReplicationBindAddress = "eth0" };

        var error = Assert.Single(config.Validate(), e => e.Contains(nameof(BrokerConfig.ReplicationBindAddress), StringComparison.Ordinal));
        Assert.Contains("'eth0' is not an IP address", error, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("127.0.0.1")]
    [InlineData("::1")]
    public void AReplicationBindAddressUnsetOrAnAddress_IsValid(string? address)
    {
        var config = new BrokerConfig { ReplicationBindAddress = address };

        Assert.DoesNotContain(config.Validate(), e => e.Contains(nameof(BrokerConfig.ReplicationBindAddress), StringComparison.Ordinal));
    }
}
