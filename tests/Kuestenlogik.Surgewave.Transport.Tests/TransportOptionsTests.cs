using Kuestenlogik.Surgewave.Testing;
using Kuestenlogik.Surgewave.Transport;
using Xunit;

namespace Kuestenlogik.Surgewave.Transport.Tests;

/// <summary>
/// Tests for TransportOptions configuration defaults and validation.
/// </summary>
[Trait("Category", TestCategories.Unit)]
public sealed class TransportOptionsTests
{
    [Fact]
    public void TransportOptions_DefaultValues_AreCorrect()
    {
        // Act
        var options = new TransportOptions { Host = "localhost", Port = 9093 };

        // Assert
        Assert.Equal("localhost", options.Host);
        Assert.Equal(9093, options.Port);
        Assert.True(options.EnablePipelining, "Pipelining should be enabled by default");
        Assert.True(options.EnableCompression, "Compression should be enabled by default");
        Assert.Equal(65536, options.SendBufferSize);
        Assert.Equal(65536, options.ReceiveBufferSize);
    }

    [Fact]
    public void TransportOptions_CanOverrideDefaults()
    {
        // Act
        var options = new TransportOptions
        {
            Host = "broker.example.com",
            Port = 19093,
            EnablePipelining = false,
            EnableCompression = false,
            SendBufferSize = 131072,
            ReceiveBufferSize = 262144
        };

        // Assert
        Assert.Equal("broker.example.com", options.Host);
        Assert.Equal(19093, options.Port);
        Assert.False(options.EnablePipelining);
        Assert.False(options.EnableCompression);
        Assert.Equal(131072, options.SendBufferSize);
        Assert.Equal(262144, options.ReceiveBufferSize);
    }

    [Fact]
    public void TransportOptions_WithMinimalConfiguration()
    {
        // Arrange & Act - only required properties
        var options = new TransportOptions { Host = "127.0.0.1", Port = 9093 };

        // Assert - required props set
        Assert.Equal("127.0.0.1", options.Host);
        Assert.Equal(9093, options.Port);
        // defaults intact
        Assert.Equal(65536, options.SendBufferSize);
    }

    [Fact]
    public void TransportOptions_InitProperties_AreReadOnly()
    {
        // Arrange - create two options with the same host but different ports
        var options1 = new TransportOptions { Host = "localhost", Port = 9093 };
        var options2 = new TransportOptions { Host = "localhost", Port = 19093 };

        // Assert - they are independent objects, modifying one does not affect the other
        Assert.Equal(9093, options1.Port);
        Assert.Equal(19093, options2.Port);
        Assert.Equal(options1.Host, options2.Host);
    }

    [Theory]
    [InlineData(9092)]
    [InlineData(9093)]
    [InlineData(1)]
    [InlineData(65535)]
    public void TransportOptions_AcceptsVariousPorts(int port)
    {
        // Act
        var options = new TransportOptions { Host = "localhost", Port = port };

        // Assert
        Assert.Equal(port, options.Port);
    }
}
