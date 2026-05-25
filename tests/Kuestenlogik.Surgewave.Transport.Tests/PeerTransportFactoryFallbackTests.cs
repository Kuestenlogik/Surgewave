using Xunit;

namespace Kuestenlogik.Surgewave.Transport.Tests;

/// <summary>
/// Tests for <see cref="PeerTransportFactory.CreateWithFallback"/>.
/// </summary>
public class PeerTransportFactoryFallbackTests
{
    [Fact]
    public void CreateWithFallback_PrimaryRegistered_ReturnsPrimary()
    {
        var name = $"test-primary-{Guid.NewGuid():N}";
        var fallback = $"test-fallback-{Guid.NewGuid():N}";

        PeerTransportFactory.Register(name, () => new FakePeerTransport(name));
        PeerTransportFactory.Register(fallback, () => new FakePeerTransport(fallback));

        var transport = PeerTransportFactory.CreateWithFallback(name, fallback, out var fellBack);

        Assert.False(fellBack);
        Assert.Equal(name, transport.Name);
    }

    [Fact]
    public void CreateWithFallback_PrimaryNotRegistered_ReturnsFallback()
    {
        var name = $"unregistered-{Guid.NewGuid():N}";
        var fallback = $"test-fallback-{Guid.NewGuid():N}";

        PeerTransportFactory.Register(fallback, () => new FakePeerTransport(fallback));

        var transport = PeerTransportFactory.CreateWithFallback(name, fallback, out var fellBack);

        Assert.True(fellBack);
        Assert.Equal(fallback, transport.Name);
    }

    [Fact]
    public void CreateWithFallback_PrimaryThrowsPlatformNotSupported_ReturnsFallback()
    {
        var name = $"broken-{Guid.NewGuid():N}";
        var fallback = $"test-fallback-{Guid.NewGuid():N}";

        PeerTransportFactory.Register(name, () => throw new PlatformNotSupportedException("no msquic"));
        PeerTransportFactory.Register(fallback, () => new FakePeerTransport(fallback));

        var transport = PeerTransportFactory.CreateWithFallback(name, fallback, out var fellBack);

        Assert.True(fellBack);
        Assert.Equal(fallback, transport.Name);
    }

    [Fact]
    public void Create_UnregisteredName_ThrowsInvalidOperation()
    {
        var name = $"ghost-{Guid.NewGuid():N}";
        Assert.Throws<InvalidOperationException>(() => PeerTransportFactory.Create(name));
    }

    [Fact]
    public void IsRegistered_KnownName_ReturnsTrue()
    {
        var name = $"known-{Guid.NewGuid():N}";
        PeerTransportFactory.Register(name, () => new FakePeerTransport(name));
        Assert.True(PeerTransportFactory.IsRegistered(name));
    }

    [Fact]
    public void IsRegistered_UnknownName_ReturnsFalse()
    {
        Assert.False(PeerTransportFactory.IsRegistered($"unknown-{Guid.NewGuid():N}"));
    }

    private sealed class FakePeerTransport : IPeerTransport
    {
        public FakePeerTransport(string name) => Name = name;
        public string Name { get; }
        public ValueTask<IPeerConnection> ConnectAsync(string host, int port, CancellationToken ct) =>
            throw new NotImplementedException();
        public IPeerListener CreateListener(System.Net.IPEndPoint endpoint) =>
            throw new NotImplementedException();
    }
}
