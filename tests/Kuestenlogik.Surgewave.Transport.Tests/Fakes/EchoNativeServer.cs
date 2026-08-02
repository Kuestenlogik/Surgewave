using System.Net;
using System.Net.Sockets;
using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Transport.Tests.Fakes;

/// <summary>
/// A minimal native-protocol server for transport tests: completes the handshake, then answers
/// every request by echoing its payload back uncompressed.
///
/// <para>Uncompressed is the point — that is the frame shape the transport serves from a pooled
/// buffer (#80), so echoing lets a test assert on bytes it chose itself.</para>
/// </summary>
internal sealed class EchoNativeServer : IAsyncDisposable
{
    private readonly TcpListener _listener;
    private readonly CancellationTokenSource _cts = new();
    private readonly Task _acceptTask;
    private readonly TimeSpan _responseDelay;
    private readonly bool _neverRespond;

    public int Port { get; }

    /// <param name="responseDelay">
    /// Wait this long before answering. Needed to test what happens to a request that is already on
    /// the wire: without a delay, a client cancellation wins the race before the frame is ever sent,
    /// and the interesting branches on the response side are never reached.
    /// </param>
    /// <param name="neverRespond">
    /// Read requests and stay silent, so the caller can exercise a transport that is disposed with
    /// requests still in flight.
    /// </param>
    public EchoNativeServer(TimeSpan? responseDelay = null, bool neverRespond = false)
    {
        _responseDelay = responseDelay ?? TimeSpan.Zero;
        _neverRespond = neverRespond;
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
                _ = Task.Run(() => ServeAsync(client, _responseDelay, _neverRespond, cancellationToken), cancellationToken);
            }
        }
        catch (OperationCanceledException) { /* shutting down */ }
        catch (SocketException) { /* listener stopped */ }
    }

    private static async Task ServeAsync(TcpClient client, TimeSpan responseDelay, bool neverRespond, CancellationToken cancellationToken)
    {
        using (client)
        {
            var stream = client.GetStream();
            try
            {
                // Handshake: magic + version in, header + capabilities out.
                var handshake = new byte[5];
                await stream.ReadExactlyAsync(handshake, cancellationToken);
                await WriteFrameAsync(stream, new SurgewaveResponseHeader
                {
                    RequestId = 0,
                    OpCode = SurgewaveOpCode.Ping,
                    ErrorCode = SurgewaveErrorCode.None,
                    PayloadLength = 2
                }, [SurgewaveNativeProtocol.Version, 0 /* no compression */], cancellationToken);

                var requestHeader = new byte[SurgewaveNativeProtocol.HeaderSize];
                while (!cancellationToken.IsCancellationRequested)
                {
                    await stream.ReadExactlyAsync(requestHeader, cancellationToken);
                    var header = SurgewaveRequestHeader.ReadFrom(requestHeader);

                    var payload = new byte[header.PayloadLength];
                    if (payload.Length > 0)
                        await stream.ReadExactlyAsync(payload, cancellationToken);

                    if (neverRespond)
                        continue;

                    if (responseDelay > TimeSpan.Zero)
                        await Task.Delay(responseDelay, cancellationToken);

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
                // Client went away or the test finished.
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
        try { await _acceptTask; } catch { /* best-effort */ }
        _listener.Dispose();
        _cts.Dispose();
    }
}
