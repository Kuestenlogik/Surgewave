using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;

namespace Kuestenlogik.Surgewave.Testing.Network;

/// <summary>
/// A UDP forwarding proxy that injects real packet loss and latency between
/// a UDP client and an upstream server. Intended for benchmarks and tests
/// that need to measure behaviour of UDP-based transports (QUIC, DTLS,
/// custom protocols) under configurable network impairments.
/// </summary>
/// <remarks>
/// Because the proxy sits on the wire at the UDP datagram level, dropping
/// datagrams here faithfully simulates packet loss — any reliable protocol
/// layered on top (e.g. QUIC) will engage its retransmission machinery
/// exactly as it would on a real lossy path.
///
/// For each unique source endpoint the proxy allocates an ephemeral upstream
/// socket so responses can be routed back to the correct client, allowing
/// multiple clients to share a single proxy instance.
/// </remarks>
public sealed class LossyUdpProxy : IAsyncDisposable
{
    private readonly int _listenPort;
    private readonly IPEndPoint _upstream;
    private readonly double _dropRate;
    private readonly int _latencyMs;
    private readonly Random _random = new();
    private readonly CancellationTokenSource _cts = new();
    private readonly Socket _clientFacing;
    private readonly ConcurrentDictionary<IPEndPoint, UpstreamBinding> _bindings = new();

    private long _clientToBrokerForwarded;
    private long _clientToBrokerDropped;
    private long _brokerToClientForwarded;
    private long _brokerToClientDropped;

    public LossyUdpProxy(int listenPort, int upstreamPort, double dropRate, int latencyMs)
    {
        _listenPort = listenPort;
        _upstream = new IPEndPoint(IPAddress.Loopback, upstreamPort);
        _dropRate = dropRate;
        _latencyMs = latencyMs;

        _clientFacing = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        _clientFacing.Bind(new IPEndPoint(IPAddress.Loopback, _listenPort));
    }

    public int ListenPort => _listenPort;
    public long ClientToBrokerForwarded => Volatile.Read(ref _clientToBrokerForwarded);
    public long ClientToBrokerDropped => Volatile.Read(ref _clientToBrokerDropped);
    public long BrokerToClientForwarded => Volatile.Read(ref _brokerToClientForwarded);
    public long BrokerToClientDropped => Volatile.Read(ref _brokerToClientDropped);

    public long TotalDropped => ClientToBrokerDropped + BrokerToClientDropped;
    public long TotalForwarded => ClientToBrokerForwarded + BrokerToClientForwarded;

    public Task Start() => Task.Run(ReceiveFromClientLoopAsync);

    private async Task ReceiveFromClientLoopAsync()
    {
        var buffer = new byte[64 * 1024];
        var from = new IPEndPoint(IPAddress.Any, 0) as EndPoint;

        while (!_cts.IsCancellationRequested)
        {
            SocketReceiveFromResult result;
            try
            {
                result = await _clientFacing.ReceiveFromAsync(buffer, SocketFlags.None, from, _cts.Token);
            }
            catch (OperationCanceledException) { break; }
            catch (SocketException) { continue; }

            var clientEndpoint = (IPEndPoint)result.RemoteEndPoint;
            var payload = buffer.AsSpan(0, result.ReceivedBytes).ToArray();

            var binding = _bindings.GetOrAdd(clientEndpoint, ep => CreateBinding(ep));

            if (ShouldDrop())
            {
                Interlocked.Increment(ref _clientToBrokerDropped);
                continue;
            }

            _ = ForwardClientToBrokerAsync(binding, payload);
        }
    }

    private UpstreamBinding CreateBinding(IPEndPoint clientEndpoint)
    {
        var upstreamSocket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        upstreamSocket.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        var binding = new UpstreamBinding(upstreamSocket, clientEndpoint);
        _ = Task.Run(() => ReceiveFromBrokerLoopAsync(binding));
        return binding;
    }

    private async Task ForwardClientToBrokerAsync(UpstreamBinding binding, byte[] payload)
    {
        try
        {
            if (_latencyMs > 0)
            {
                await Task.Delay(_latencyMs, _cts.Token).ConfigureAwait(false);
            }
            await binding.Upstream.SendToAsync(payload, SocketFlags.None, _upstream, _cts.Token).ConfigureAwait(false);
            Interlocked.Increment(ref _clientToBrokerForwarded);
        }
        catch (OperationCanceledException) { }
        catch (SocketException) { }
        catch (ObjectDisposedException) { }
    }

    private async Task ReceiveFromBrokerLoopAsync(UpstreamBinding binding)
    {
        var buffer = new byte[64 * 1024];
        var from = new IPEndPoint(IPAddress.Any, 0) as EndPoint;

        while (!_cts.IsCancellationRequested)
        {
            SocketReceiveFromResult result;
            try
            {
                result = await binding.Upstream.ReceiveFromAsync(buffer, SocketFlags.None, from, _cts.Token);
            }
            catch (OperationCanceledException) { return; }
            catch (SocketException) { continue; }
            catch (ObjectDisposedException) { return; }

            var payload = buffer.AsSpan(0, result.ReceivedBytes).ToArray();

            if (ShouldDrop())
            {
                Interlocked.Increment(ref _brokerToClientDropped);
                continue;
            }

            _ = ForwardBrokerToClientAsync(binding, payload);
        }
    }

    private async Task ForwardBrokerToClientAsync(UpstreamBinding binding, byte[] payload)
    {
        try
        {
            if (_latencyMs > 0)
            {
                await Task.Delay(_latencyMs, _cts.Token).ConfigureAwait(false);
            }
            await _clientFacing.SendToAsync(payload, SocketFlags.None, binding.Client, _cts.Token).ConfigureAwait(false);
            Interlocked.Increment(ref _brokerToClientForwarded);
        }
        catch (OperationCanceledException) { }
        catch (SocketException) { }
        catch (ObjectDisposedException) { }
    }

    private bool ShouldDrop()
    {
        if (_dropRate <= 0) return false;
        double r;
        lock (_random) { r = _random.NextDouble(); }
        return r < _dropRate;
    }

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();
        try { _clientFacing.Dispose(); } catch { }
        foreach (var binding in _bindings.Values)
        {
            try { binding.Upstream.Dispose(); } catch { }
        }
        _cts.Dispose();
        await Task.CompletedTask;
    }

    private sealed class UpstreamBinding
    {
        public Socket Upstream { get; }
        public IPEndPoint Client { get; }

        public UpstreamBinding(Socket upstream, IPEndPoint client)
        {
            Upstream = upstream;
            Client = client;
        }
    }
}
