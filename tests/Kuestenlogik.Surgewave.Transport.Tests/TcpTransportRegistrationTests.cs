using Kuestenlogik.Surgewave.Testing;
using Kuestenlogik.Surgewave.Transport;
using Kuestenlogik.Surgewave.Transport.Tcp;
using Xunit;

namespace Kuestenlogik.Surgewave.Transport.Tests;

/// <summary>
/// Tests for TcpTransportRegistration - the module-level auto-registration.
/// </summary>
/// <remarks>
/// This class is in its own collection to prevent parallel test execution from causing
/// interference via the shared static SurgewaveTransportFactory state.
/// </remarks>
[Trait("Category", TestCategories.Unit)]
[Collection("TransportFactory")]
public sealed class TcpTransportRegistrationTests
{
    [Fact]
    public void Register_IsIdempotent_DoesNotThrow()
    {
        // Act - calling Register multiple times should be safe (no exception thrown)
        TcpTransportRegistration.Register();
        TcpTransportRegistration.Register();
        TcpTransportRegistration.Register();

        // Assert - idempotency: registering more than once does not throw; the factory is still callable.
        // Note: SurgewaveTransportFactory uses shared static state; other tests may replace the TCP factory
        // with mocks. We verify only that the factory is callable (not null), not the concrete return type.
        var options = new TransportOptions { Host = "localhost", Port = 9093 };
        var transport = SurgewaveTransportFactory.CreateTcpTransport(options);
        Assert.NotNull(transport);
    }

    [Fact]
    public void CreateTcpTransport_AfterRegistration_ReturnsISurgewaveTransport()
    {
        // Arrange - directly register the real TCP factory to ensure we get a TcpTransport
        // (bypasses the idempotency guard on TcpTransportRegistration which only runs once per AppDomain)
        SurgewaveTransportFactory.RegisterTcpTransport(opts => new TcpTransport(opts));
        var options = new TransportOptions { Host = "localhost", Port = 9093 };

        // Act
        var transport = SurgewaveTransportFactory.CreateTcpTransport(options);

        // Assert - the real TcpTransport implements ISurgewaveTransport with TransportType = Tcp
        Assert.IsType<TcpTransport>(transport);
        Assert.Equal(SurgewaveTransportType.Tcp, transport.TransportType);
    }

    [Fact]
    public async Task TcpTransport_TransportType_IsTcp()
    {
        // Arrange — bypass the idempotent Register() guard so we get the
        // real TcpTransport regardless of any prior test that swapped the
        // static factory for a mock.
        SurgewaveTransportFactory.RegisterTcpTransport(opts => new TcpTransport(opts));
        var options = new TransportOptions { Host = "localhost", Port = 9093 };

        // Act
        await using var transport = SurgewaveTransportFactory.CreateTcpTransport(options) as Kuestenlogik.Surgewave.Transport.Tcp.TcpTransport;

        // Assert
        Assert.NotNull(transport);
        Assert.Equal(SurgewaveTransportType.Tcp, transport.TransportType);
    }

    [Fact]
    public void TcpTransport_IsNotConnected_BeforeConnect()
    {
        // Arrange
        TcpTransportRegistration.Register();
        var options = new TransportOptions { Host = "localhost", Port = 9093 };

        // Act
        var transport = SurgewaveTransportFactory.CreateTcpTransport(options);

        // Assert - before connecting, IsConnected should be false
        Assert.False(transport.IsConnected);
    }

    [Fact]
    public void TcpTransport_ServerSupportsCompression_FalseBeforeConnect()
    {
        // Arrange
        TcpTransportRegistration.Register();
        var options = new TransportOptions { Host = "localhost", Port = 9093 };

        // Act
        var transport = SurgewaveTransportFactory.CreateTcpTransport(options);

        // Assert - before handshake, compression support is unknown (false)
        Assert.False(transport.ServerSupportsCompression);
    }

    [Fact]
    public async Task TcpTransport_DisposeAsync_CanBeCalledWithoutConnecting()
    {
        // Arrange
        TcpTransportRegistration.Register();
        var options = new TransportOptions { Host = "localhost", Port = 9093 };
        var transport = SurgewaveTransportFactory.CreateTcpTransport(options);

        // Act & Assert - should not throw when disposing without connecting
        await transport.DisposeAsync();
    }

    [Fact]
    public async Task TcpTransport_ConnectAsync_ThrowsWhenBrokerUnavailable()
    {
        // Arrange — direct registration; the idempotent Register() above
        // can leave the static factory pointing at a mock left over from
        // SurgewaveTransportFactoryTests, in which case ConnectAsync silently
        // returns instead of throwing.
        SurgewaveTransportFactory.RegisterTcpTransport(opts => new TcpTransport(opts));
        var options = new TransportOptions
        {
            Host = "127.0.0.1",
            Port = 19999 // unlikely to be in use
        };
        await using var transport = SurgewaveTransportFactory.CreateTcpTransport(options);

        // Act & Assert
        await Assert.ThrowsAnyAsync<Exception>(async () =>
            await transport.ConnectAsync(new CancellationTokenSource(TimeSpan.FromSeconds(2)).Token));
    }
}
