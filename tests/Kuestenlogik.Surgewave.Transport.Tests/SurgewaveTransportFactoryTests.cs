using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Testing;
using Kuestenlogik.Surgewave.Transport;
using NSubstitute;
using Xunit;

namespace Kuestenlogik.Surgewave.Transport.Tests;

/// <summary>
/// Tests for SurgewaveTransportFactory registration and creation logic.
/// </summary>
[Trait("Category", TestCategories.Unit)]
[Collection("TransportFactory")]
public sealed class SurgewaveTransportFactoryTests
{
    private static TransportOptions DefaultOptions => new() { Host = "localhost", Port = 9093 };

    [Fact]
    public void RegisterTcpTransport_ThenCreateTcp_ReturnsMockTransport()
    {
        // Arrange
        var mockTransport = Substitute.For<ISurgewaveTransport>();
        mockTransport.TransportType.Returns(SurgewaveTransportType.Tcp);
        SurgewaveTransportFactory.RegisterTcpTransport(_ => mockTransport);

        // Act
        var transport = SurgewaveTransportFactory.CreateTcpTransport(DefaultOptions);

        // Assert
        Assert.NotNull(transport);
        Assert.Equal(SurgewaveTransportType.Tcp, transport.TransportType);
    }

    [Fact]
    public void RegisterSharedMemoryTransport_ThenCreateSharedMemory_ReturnsMockTransport()
    {
        // Arrange
        var mockTransport = Substitute.For<ISurgewaveTransport>();
        mockTransport.TransportType.Returns(SurgewaveTransportType.SharedMemory);
        SurgewaveTransportFactory.RegisterSharedMemoryTransport(_ => mockTransport);

        // Act
        var transport = SurgewaveTransportFactory.CreateSharedMemoryTransport(DefaultOptions);

        // Assert
        Assert.NotNull(transport);
        Assert.Equal(SurgewaveTransportType.SharedMemory, transport.TransportType);
    }

    [Fact]
    public async Task CreateAsync_WithTcpType_ReturnsTcpTransport()
    {
        // Arrange
        var mockTransport = Substitute.For<ISurgewaveTransport>();
        mockTransport.TransportType.Returns(SurgewaveTransportType.Tcp);
        SurgewaveTransportFactory.RegisterTcpTransport(_ => mockTransport);

        // Act
        var transport = await SurgewaveTransportFactory.CreateAsync(DefaultOptions, SurgewaveTransportType.Tcp);

        // Assert
        Assert.Equal(SurgewaveTransportType.Tcp, transport.TransportType);
    }

    [Fact]
    public async Task CreateAsync_WithSharedMemoryType_ReturnsSharedMemoryTransport()
    {
        // Arrange
        var mockTransport = Substitute.For<ISurgewaveTransport>();
        mockTransport.TransportType.Returns(SurgewaveTransportType.SharedMemory);
        SurgewaveTransportFactory.RegisterSharedMemoryTransport(_ => mockTransport);

        // Act
        var transport = await SurgewaveTransportFactory.CreateAsync(DefaultOptions, SurgewaveTransportType.SharedMemory);

        // Assert
        Assert.Equal(SurgewaveTransportType.SharedMemory, transport.TransportType);
    }

    [Fact]
    public async Task CreateAsync_WithUnknownType_ThrowsArgumentOutOfRangeException()
    {
        // Act & Assert
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () =>
            await SurgewaveTransportFactory.CreateAsync(DefaultOptions, (SurgewaveTransportType)99));
    }

    [Fact]
    public async Task CreateAutoTransportAsync_WhenBrokerIsLocal_UsesSharedMemory()
    {
        // Arrange
        var sharedMemTransport = Substitute.For<ISurgewaveTransport>();
        sharedMemTransport.TransportType.Returns(SurgewaveTransportType.SharedMemory);

        SurgewaveTransportFactory.RegisterSharedMemoryTransport(_ => sharedMemTransport);
        SurgewaveTransportFactory.RegisterLocalBrokerDetector(_ => ValueTask.FromResult(true));

        // Act
        var transport = await SurgewaveTransportFactory.CreateAutoTransportAsync(DefaultOptions);

        // Assert
        Assert.Equal(SurgewaveTransportType.SharedMemory, transport.TransportType);
    }

    [Fact]
    public async Task CreateAutoTransportAsync_WhenBrokerIsRemote_UsesTcp()
    {
        // Arrange
        var tcpTransport = Substitute.For<ISurgewaveTransport>();
        tcpTransport.TransportType.Returns(SurgewaveTransportType.Tcp);

        SurgewaveTransportFactory.RegisterTcpTransport(_ => tcpTransport);
        SurgewaveTransportFactory.RegisterLocalBrokerDetector(_ => ValueTask.FromResult(false));

        // Act
        var transport = await SurgewaveTransportFactory.CreateAutoTransportAsync(DefaultOptions);

        // Assert
        Assert.Equal(SurgewaveTransportType.Tcp, transport.TransportType);
    }

    [Fact]
    public async Task CreateAutoTransportAsync_WithAutoEnum_Uses_AutoDetection()
    {
        // Arrange
        var tcpTransport = Substitute.For<ISurgewaveTransport>();
        tcpTransport.TransportType.Returns(SurgewaveTransportType.Tcp);
        SurgewaveTransportFactory.RegisterTcpTransport(_ => tcpTransport);
        SurgewaveTransportFactory.RegisterLocalBrokerDetector(_ => ValueTask.FromResult(false));

        // Act
        var transport = await SurgewaveTransportFactory.CreateAsync(DefaultOptions, SurgewaveTransportType.Auto);

        // Assert - Should fall back to TCP since broker is remote
        Assert.Equal(SurgewaveTransportType.Tcp, transport.TransportType);
    }

    [Fact]
    public void RegisterTcpTransport_PassesOptionsToFactory()
    {
        // Arrange
        TransportOptions? capturedOptions = null;
        var mockTransport = Substitute.For<ISurgewaveTransport>();
        SurgewaveTransportFactory.RegisterTcpTransport(opts =>
        {
            capturedOptions = opts;
            return mockTransport;
        });

        var expectedOptions = new TransportOptions { Host = "specific-host", Port = 12345 };

        // Act
        SurgewaveTransportFactory.CreateTcpTransport(expectedOptions);

        // Assert
        Assert.NotNull(capturedOptions);
        Assert.Equal("specific-host", capturedOptions.Host);
        Assert.Equal(12345, capturedOptions.Port);
    }
}
