using Kuestenlogik.Surgewave.Core.Util;
using Kuestenlogik.Surgewave.Testing;
using Xunit;

namespace Kuestenlogik.Surgewave.Core.Tests;

/// <summary>
/// Tests for BrokerAddress parsing and formatting.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class BrokerAddressTests
{
    #region Parse Tests

    [Fact]
    public void Parse_HostAndPort_ParsesCorrectly()
    {
        var addr = BrokerAddress.Parse("broker1:9092");

        Assert.Equal("broker1", addr.Host);
        Assert.Equal(9092, addr.Port);
    }

    [Fact]
    public void Parse_CustomPort_ParsesCorrectly()
    {
        var addr = BrokerAddress.Parse("localhost:19092");

        Assert.Equal("localhost", addr.Host);
        Assert.Equal(19092, addr.Port);
    }

    [Fact]
    public void Parse_HostOnly_UsesDefaultPort()
    {
        var addr = BrokerAddress.Parse("broker1");

        Assert.Equal("broker1", addr.Host);
        Assert.Equal(BrokerAddress.DefaultPort, addr.Port);
    }

    [Fact]
    public void Parse_IpAddress_ParsesCorrectly()
    {
        var addr = BrokerAddress.Parse("192.168.1.100:9092");

        Assert.Equal("192.168.1.100", addr.Host);
        Assert.Equal(9092, addr.Port);
    }

    [Fact]
    public void Parse_NullOrEmpty_Throws()
    {
        Assert.ThrowsAny<ArgumentException>(() => BrokerAddress.Parse(null!));
        Assert.ThrowsAny<ArgumentException>(() => BrokerAddress.Parse(""));
        Assert.ThrowsAny<ArgumentException>(() => BrokerAddress.Parse("  "));
    }

    #endregion

    #region ParseFirst Tests

    [Fact]
    public void ParseFirst_SingleBroker_ReturnsIt()
    {
        var addr = BrokerAddress.ParseFirst("broker1:9092");

        Assert.Equal("broker1", addr.Host);
        Assert.Equal(9092, addr.Port);
    }

    [Fact]
    public void ParseFirst_MultipleBrokers_ReturnsFirstOnly()
    {
        var addr = BrokerAddress.ParseFirst("broker1:9092,broker2:9093,broker3:9094");

        Assert.Equal("broker1", addr.Host);
        Assert.Equal(9092, addr.Port);
    }

    [Fact]
    public void ParseFirst_NullOrEmpty_Throws()
    {
        Assert.ThrowsAny<ArgumentException>(() => BrokerAddress.ParseFirst(null!));
        Assert.ThrowsAny<ArgumentException>(() => BrokerAddress.ParseFirst(""));
    }

    #endregion

    #region ParseAll Tests

    [Fact]
    public void ParseAll_SingleBroker_ReturnsOne()
    {
        var addresses = BrokerAddress.ParseAll("broker1:9092");

        Assert.Single(addresses);
        Assert.Equal("broker1", addresses[0].Host);
        Assert.Equal(9092, addresses[0].Port);
    }

    [Fact]
    public void ParseAll_MultipleBrokers_ReturnsAll()
    {
        var addresses = BrokerAddress.ParseAll("broker1:9092,broker2:9093,broker3:9094");

        Assert.Equal(3, addresses.Count);
        Assert.Equal("broker1", addresses[0].Host);
        Assert.Equal(9092, addresses[0].Port);
        Assert.Equal("broker2", addresses[1].Host);
        Assert.Equal(9093, addresses[1].Port);
        Assert.Equal("broker3", addresses[2].Host);
        Assert.Equal(9094, addresses[2].Port);
    }

    [Fact]
    public void ParseAll_WithWhitespace_TrimsCorrectly()
    {
        var addresses = BrokerAddress.ParseAll("  broker1:9092 , broker2:9093 ");

        Assert.Equal(2, addresses.Count);
        Assert.Equal("broker1", addresses[0].Host);
        Assert.Equal("broker2", addresses[1].Host);
    }

    [Fact]
    public void ParseAll_NullOrEmpty_Throws()
    {
        Assert.ThrowsAny<ArgumentException>(() => BrokerAddress.ParseAll(null!));
        Assert.ThrowsAny<ArgumentException>(() => BrokerAddress.ParseAll(""));
    }

    #endregion

    #region ToString Tests

    [Fact]
    public void ToString_FormatsAsHostColonPort()
    {
        var addr = new BrokerAddress("myhost", 9092);
        Assert.Equal("myhost:9092", addr.ToString());
    }

    #endregion

    #region Equality Tests

    [Fact]
    public void Equality_SameValues_AreEqual()
    {
        var a = new BrokerAddress("host", 9092);
        var b = new BrokerAddress("host", 9092);

        Assert.Equal(a, b);
        Assert.True(a == b);
    }

    [Fact]
    public void Equality_DifferentHost_NotEqual()
    {
        var a = new BrokerAddress("host1", 9092);
        var b = new BrokerAddress("host2", 9092);

        Assert.NotEqual(a, b);
    }

    [Fact]
    public void Equality_DifferentPort_NotEqual()
    {
        var a = new BrokerAddress("host", 9092);
        var b = new BrokerAddress("host", 9093);

        Assert.NotEqual(a, b);
    }

    [Fact]
    public void DefaultPort_IsKafkaPort()
    {
        Assert.Equal(KafkaConstants.Ports.Kafka, BrokerAddress.DefaultPort);
    }

    #endregion
}
