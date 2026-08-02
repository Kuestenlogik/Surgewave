using System.Net;
using System.Net.Sockets;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Transport;
using Kuestenlogik.Surgewave.Transport.Tcp;

namespace Kuestenlogik.Surgewave.Benchmarks.Transport;

/// <summary>
/// The two response read paths of the TCP transport over loopback (#80): the materializing one,
/// which must hand the caller memory it keeps, and the borrowed one, which serves the payload from
/// a pooled buffer the caller returns.
///
/// <para>Allocated bytes are the signal. The difference is one array per response — invisible on a
/// produce ack, dominant on a fetch — which is why the payload size is a parameter.</para>
/// </summary>
[SimpleJob(RuntimeMoniker.HostProcess)]
[MemoryDiagnoser]
[BenchmarkCategory("Transport", "Native")]
public class ResponseReadPathBenchmarks
{
    private EchoServer _server = null!;
    private TcpTransport _transport = null!;
    private byte[] _request = null!;

    /// <summary>Response payload size: a small ack versus a fetch-sized batch.</summary>
    [Params(64, 64 * 1024)]
    public int PayloadBytes { get; set; }

    [GlobalSetup]
    public async Task SetupAsync()
    {
        _server = new EchoServer();
        _transport = new TcpTransport(new TransportOptions
        {
            Host = "127.0.0.1",
            Port = _server.Port,
            EnablePipelining = true,
            EnableCompression = false
        });
        await _transport.ConnectAsync();

        _request = new byte[PayloadBytes];
        Random.Shared.NextBytes(_request);
    }

    [GlobalCleanup]
    public async Task CleanupAsync()
    {
        await _transport.DisposeAsync();
        await _server.DisposeAsync();
    }

    [Benchmark(Baseline = true)]
    public async Task<int> Materialized()
    {
        var (_, payload) = await _transport.SendRequestAsync(SurgewaveOpCode.Fetch, _request, compress: false);
        return payload.Length;
    }

    [Benchmark]
    public async Task<int> Leased()
    {
        using var response = await _transport.SendRequestLeasedAsync(SurgewaveOpCode.Fetch, _request, compress: false);
        return response.Payload.Length;
    }

    /// <summary>Minimal native-protocol server: handshake, then echo every request payload back.</summary>
    private sealed class EchoServer : IAsyncDisposable
    {
        private readonly TcpListener _listener;
        private readonly CancellationTokenSource _cts = new();
        private readonly Task _acceptTask;

        public int Port { get; }

        public EchoServer()
        {
            _listener = new TcpListener(IPAddress.Loopback, 0);
            _listener.Start();
            Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
            _acceptTask = Task.Run(() => AcceptLoopAsync(_cts.Token));
        }

        private async Task AcceptLoopAsync(CancellationToken cancellationToken)
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    var client = await _listener.AcceptTcpClientAsync(cancellationToken);
                    client.NoDelay = true;
                    _ = Task.Run(() => ServeAsync(client, cancellationToken), cancellationToken);
                }
            }
            catch (OperationCanceledException) { }
            catch (SocketException) { }
        }

        private static async Task ServeAsync(TcpClient client, CancellationToken cancellationToken)
        {
            using (client)
            {
                var stream = client.GetStream();
                try
                {
                    var handshake = new byte[5];
                    await stream.ReadExactlyAsync(handshake, cancellationToken);
                    await WriteFrameAsync(stream, new SurgewaveResponseHeader
                    {
                        RequestId = 0,
                        OpCode = SurgewaveOpCode.Ping,
                        ErrorCode = SurgewaveErrorCode.None,
                        PayloadLength = 2
                    }, [SurgewaveNativeProtocol.Version, 0], cancellationToken);

                    var requestHeader = new byte[SurgewaveNativeProtocol.HeaderSize];
                    while (!cancellationToken.IsCancellationRequested)
                    {
                        await stream.ReadExactlyAsync(requestHeader, cancellationToken);
                        var header = SurgewaveRequestHeader.ReadFrom(requestHeader);

                        var payload = new byte[header.PayloadLength];
                        if (payload.Length > 0)
                            await stream.ReadExactlyAsync(payload, cancellationToken);

                        await WriteFrameAsync(stream, new SurgewaveResponseHeader
                        {
                            RequestId = header.RequestId,
                            OpCode = header.OpCode,
                            ErrorCode = SurgewaveErrorCode.None,
                            PayloadLength = payload.Length
                        }, payload, cancellationToken);
                    }
                }
                catch (Exception ex) when (ex is OperationCanceledException or EndOfStreamException or IOException)
                {
                }
            }
        }

        private static async Task WriteFrameAsync(
            Stream stream, SurgewaveResponseHeader header, byte[] payload, CancellationToken cancellationToken)
        {
            var frame = new byte[SurgewaveResponseHeader.Size + payload.Length];
            header.WriteTo(frame);
            payload.CopyTo(frame, SurgewaveResponseHeader.Size);
            await stream.WriteAsync(frame, cancellationToken);
            await stream.FlushAsync(cancellationToken);
        }

        public async ValueTask DisposeAsync()
        {
            await _cts.CancelAsync();
            _listener.Stop();
            try { await _acceptTask; } catch { }
            _listener.Dispose();
            _cts.Dispose();
        }
    }
}
