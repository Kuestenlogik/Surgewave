using System.Net;
using System.Net.Sockets;

namespace Kuestenlogik.Surgewave.Testing.Network;

/// <summary>
/// A TCP forwarding proxy that injects one-way latency between a TCP client
/// and an upstream server. Each accepted connection spawns a matching upstream
/// connection and bytes are pumped in both directions with configurable delay.
/// </summary>
/// <remarks>
/// <para>
/// <b>TCP loss simulation — what this is not:</b> Dropping bytes at the
/// application layer on a TCP stream is not equivalent to IP-layer packet loss.
/// The kernel has already acknowledged the bytes, so dropping them here corrupts
/// the stream rather than simulating loss. Real TCP loss simulation requires a
/// kernel-level tool such as <c>tc netem</c> (Linux) or Clumsy (Windows).
/// </para>
/// <para>
/// <b>What this does simulate:</b> added one-way latency per direction. This is
/// useful on its own for measuring how a protocol behaves under elevated RTT,
/// and it pairs fairly with <see cref="LossyUdpProxy"/> which also adds one-way
/// latency on top of real UDP-layer drops.
/// </para>
/// </remarks>
public sealed class LossyTcpProxy : IAsyncDisposable
{
    private readonly int _listenPort;
    private readonly IPEndPoint _upstream;
    private readonly int _latencyMs;
    private readonly CancellationTokenSource _cts = new();
    private readonly TcpListener _listener;
    private Task? _acceptTask;

    private long _bytesClientToBroker;
    private long _bytesBrokerToClient;
    private int _activeConnections;

    public LossyTcpProxy(int listenPort, int upstreamPort, int latencyMs)
    {
        _listenPort = listenPort;
        _upstream = new IPEndPoint(IPAddress.Loopback, upstreamPort);
        _latencyMs = latencyMs;
        _listener = new TcpListener(IPAddress.Loopback, listenPort);
    }

    public int ListenPort => _listenPort;
    public long BytesClientToBroker => Volatile.Read(ref _bytesClientToBroker);
    public long BytesBrokerToClient => Volatile.Read(ref _bytesBrokerToClient);

    public Task Start()
    {
        _listener.Start();
        _acceptTask = Task.Run(AcceptLoopAsync);
        return Task.CompletedTask;
    }

    private async Task AcceptLoopAsync()
    {
        while (!_cts.IsCancellationRequested)
        {
            TcpClient inbound;
            try
            {
                inbound = await _listener.AcceptTcpClientAsync(_cts.Token);
            }
            catch (OperationCanceledException) { break; }
            catch (ObjectDisposedException) { break; }
            catch (SocketException) { continue; }

            _ = HandleConnectionAsync(inbound);
        }
    }

    private async Task HandleConnectionAsync(TcpClient inbound)
    {
        Interlocked.Increment(ref _activeConnections);
        var outbound = new TcpClient { NoDelay = true };
        try
        {
            inbound.NoDelay = true;
            await outbound.ConnectAsync(_upstream.Address, _upstream.Port, _cts.Token);

            await using var inStream = inbound.GetStream();
            await using var outStream = outbound.GetStream();

            var upward = PumpAsync(inStream, outStream, clientToBroker: true, _cts.Token);
            var downward = PumpAsync(outStream, inStream, clientToBroker: false, _cts.Token);

            await Task.WhenAny(upward, downward);
        }
        catch (OperationCanceledException) { }
        catch (IOException) { }
        catch (SocketException) { }
        finally
        {
            try { inbound.Close(); } catch { }
            try { outbound.Close(); } catch { }
            outbound.Dispose();
            Interlocked.Decrement(ref _activeConnections);
        }
    }

    private async Task PumpAsync(Stream source, Stream destination, bool clientToBroker, CancellationToken ct)
    {
        var buffer = new byte[64 * 1024];
        while (!ct.IsCancellationRequested)
        {
            int read;
            try
            {
                read = await source.ReadAsync(buffer.AsMemory(), ct);
            }
            catch (OperationCanceledException) { return; }
            catch (IOException) { return; }

            if (read == 0) return;

            if (_latencyMs > 0)
            {
                try { await Task.Delay(_latencyMs, ct); }
                catch (OperationCanceledException) { return; }
            }

            try
            {
                await destination.WriteAsync(buffer.AsMemory(0, read), ct);
                await destination.FlushAsync(ct);
            }
            catch (OperationCanceledException) { return; }
            catch (IOException) { return; }

            if (clientToBroker)
                Interlocked.Add(ref _bytesClientToBroker, read);
            else
                Interlocked.Add(ref _bytesBrokerToClient, read);
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _cts.CancelAsync();
        _listener.Stop();
        _listener.Dispose();
        if (_acceptTask is not null)
        {
            try { await _acceptTask; } catch (OperationCanceledException) { } catch (ObjectDisposedException) { }
        }
        _cts.Dispose();
    }
}
