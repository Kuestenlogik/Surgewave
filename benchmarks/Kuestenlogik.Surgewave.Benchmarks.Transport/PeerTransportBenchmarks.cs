using System.Net;
using System.Net.Sockets;
using BenchmarkDotNet.Attributes;
using Kuestenlogik.Surgewave.Transport;
using Kuestenlogik.Surgewave.Transport.Tcp;

namespace Kuestenlogik.Surgewave.Benchmarks.Transport;

[MemoryDiagnoser]
public class PeerTransportBenchmarks
{
    private TcpListener _listener = null!;
    private int _port;
    private TcpPeerTransport _transport = null!;

    [GlobalSetup]
    public void Setup()
    {
        TcpTransportRegistration.Register();
        _transport = new TcpPeerTransport();
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
        _port = ((IPEndPoint)_listener.LocalEndpoint).Port;
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _listener.Stop();
    }

    [Benchmark(Description = "PeerTransportFactory.Create(\"tcp\")")]
    public IPeerTransport FactoryCreate()
    {
        return PeerTransportFactory.Create("tcp");
    }

    [Benchmark(Description = "PeerTransportFactory.IsRegistered(\"tcp\")")]
    public bool FactoryIsRegistered()
    {
        return PeerTransportFactory.IsRegistered("tcp");
    }

    [Benchmark(Description = "PeerTransportFactory.IsRegistered(\"unknown\")")]
    public bool FactoryIsRegisteredMiss()
    {
        return PeerTransportFactory.IsRegistered("unknown");
    }

    [Benchmark(Description = "TcpPeerTransport.ConnectAsync (loopback)")]
    public async Task<IPeerConnection> TcpConnect()
    {
        var acceptTask = _listener.AcceptTcpClientAsync();
        var connection = await _transport.ConnectAsync("127.0.0.1", _port);
        using var accepted = await acceptTask;
        return connection;
    }

    [Benchmark(Description = "IPeerStreamLease Acquire+Dispose (TCP)")]
    public async Task TcpStreamLeaseRoundtrip()
    {
        var acceptTask = _listener.AcceptTcpClientAsync();
        var connection = await _transport.ConnectAsync("127.0.0.1", _port);
        var accepted = await acceptTask;
        try
        {
            var lease = await connection.AcquireStreamAsync();
            await lease.DisposeAsync();
        }
        finally
        {
            accepted.Dispose();
            await connection.DisposeAsync();
        }
    }
}
