using System.Net;
using System.Net.Quic;
using System.Net.Security;
using System.Net.Sockets;
using System.Runtime.Versioning;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Transport.Tests.Fakes;

/// <summary>How a broker's end of a connection goes away.</summary>
public enum ConnectionDrop
{
    /// <summary>The broker closes it: the client reads the end of the stream.</summary>
    Closed,

    /// <summary>The connection is reset under the client: what a broker process that is killed leaves behind.</summary>
    Reset,
}

/// <summary>
/// A native-protocol server that completes the handshake, reads every request, answers none and then drops the
/// connection the way a test asks for — over TCP or over QUIC, the two transports the native client has.
/// </summary>
/// <remarks>
/// It says how many requests it has read, so a test drops the connection when its requests are on the broker's
/// side and not while they are still on the way.
/// </remarks>
[SupportedOSPlatform("windows")]
[SupportedOSPlatform("linux")]
[SupportedOSPlatform("macos")]
internal sealed class DroppingNativeServer : IAsyncDisposable
{
    private readonly CancellationTokenSource _stopping = new();
    private readonly TaskCompletionSource<IServerConnection> _connected = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly Lock _gate = new();
    private readonly List<(int Count, TaskCompletionSource Reached)> _waiters = [];
    private readonly TcpListener? _tcp;
    private readonly QuicListener? _quic;
    private readonly X509Certificate2? _certificate;
    private Task _serving = Task.CompletedTask;
    private int _requestsRead;

    private DroppingNativeServer(TcpListener? tcp, QuicListener? quic, X509Certificate2? certificate, int port)
    {
        _tcp = tcp;
        _quic = quic;
        _certificate = certificate;
        Port = port;
    }

    public int Port { get; }

    /// <summary>Whether this machine can run the transport at all: QUIC needs msquic.</summary>
    public static bool Supports(SurgewaveTransportType type) =>
        type != SurgewaveTransportType.Quic || (QuicListener.IsSupported && QuicConnection.IsSupported);

    public static async Task<DroppingNativeServer> StartAsync(SurgewaveTransportType type, CancellationToken cancellationToken)
    {
        if (type == SurgewaveTransportType.Quic)
        {
            var certificate = CreateCertificate();
            var listener = await QuicListener.ListenAsync(new QuicListenerOptions
            {
                ListenEndPoint = new IPEndPoint(IPAddress.Loopback, 0),
                ApplicationProtocols = [new SslApplicationProtocol("surgewave/1")],
                ConnectionOptionsCallback = (_, _, _) => ValueTask.FromResult(new QuicServerConnectionOptions
                {
                    DefaultStreamErrorCode = 0x100,
                    DefaultCloseErrorCode = 0x101,
                    ServerAuthenticationOptions = new SslServerAuthenticationOptions
                    {
                        ApplicationProtocols = [new SslApplicationProtocol("surgewave/1")],
                        ServerCertificate = certificate,
                    },
                }),
            }, cancellationToken);

            var server = new DroppingNativeServer(null, listener, certificate, listener.LocalEndPoint.Port);
            server._serving = server.ServeQuicAsync();
            return server;
        }

        var tcp = new TcpListener(IPAddress.Loopback, 0);
        tcp.Start();
        var tcpServer = new DroppingNativeServer(tcp, null, null, ((IPEndPoint)tcp.LocalEndpoint).Port);
        tcpServer._serving = tcpServer.ServeTcpAsync();
        return tcpServer;
    }

    /// <summary>Completes once the server has read <paramref name="count"/> requests from the client.</summary>
    public Task HasReadAsync(int count)
    {
        lock (_gate)
        {
            if (_requestsRead >= count)
                return Task.CompletedTask;

            var reached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _waiters.Add((count, reached));
            return reached.Task;
        }
    }

    /// <summary>Drops the client's connection. The server has read everything the client sent by then.</summary>
    public async Task DropAsync(ConnectionDrop how)
    {
        var connection = await _connected.Task;
        await _stopping.CancelAsync();
        await _serving;
        await connection.DropAsync(how);
    }

    private async Task ServeTcpAsync()
    {
        try
        {
            var client = await _tcp!.AcceptTcpClientAsync(_stopping.Token);
            client.NoDelay = true;
            _connected.TrySetResult(new TcpServerConnection(client));
            await ReadRequestsAsync(client.GetStream());
        }
        catch (Exception ex) when (ex is OperationCanceledException or IOException or SocketException)
        {
            // Stopped, or the client went first.
        }
    }

    private async Task ServeQuicAsync()
    {
        try
        {
            var connection = await _quic!.AcceptConnectionAsync(_stopping.Token);
            var stream = await connection.AcceptInboundStreamAsync(_stopping.Token);
            _connected.TrySetResult(new QuicServerConnection(connection, stream));
            await ReadRequestsAsync(stream);
        }
        catch (Exception ex) when (ex is OperationCanceledException or IOException)
        {
            // Stopped, or the client went first.
        }
    }

    private async Task ReadRequestsAsync(Stream stream)
    {
        var token = _stopping.Token;

        // Handshake: magic and version in, a header and the capabilities out (no compression).
        var handshake = new byte[SurgewaveNativeProtocol.Magic.Length + 1];
        await stream.ReadExactlyAsync(handshake, token);
        var answer = new byte[SurgewaveResponseHeader.Size + 2];
        new SurgewaveResponseHeader { OpCode = SurgewaveOpCode.Handshake, PayloadLength = 2 }.WriteTo(answer);
        answer[SurgewaveResponseHeader.Size] = SurgewaveNativeProtocol.Version;
        await stream.WriteAsync(answer, token);
        await stream.FlushAsync(token);

        var header = new byte[SurgewaveNativeProtocol.HeaderSize];
        while (true)
        {
            await stream.ReadExactlyAsync(header, token);
            var payload = new byte[SurgewaveRequestHeader.ReadFrom(header).PayloadLength];
            await stream.ReadExactlyAsync(payload, token);

            lock (_gate)
            {
                _requestsRead++;
                for (var i = _waiters.Count - 1; i >= 0; i--)
                {
                    if (_waiters[i].Count <= _requestsRead)
                    {
                        _waiters[i].Reached.TrySetResult();
                        _waiters.RemoveAt(i);
                    }
                }
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        await _stopping.CancelAsync();
        _tcp?.Stop();
        await _serving;
        if (_connected.Task.IsCompletedSuccessfully)
            await (await _connected.Task).DisposeAsync();
        if (_quic is not null)
            await _quic.DisposeAsync();
        _tcp?.Dispose();
        _certificate?.Dispose();
        _stopping.Dispose();
    }

    private static X509Certificate2 CreateCertificate()
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest("CN=surgewave-transport-test", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension([new Oid("1.3.6.1.5.5.7.3.1")], false));
        var names = new SubjectAlternativeNameBuilder();
        names.AddDnsName("localhost");
        names.AddIpAddress(IPAddress.Loopback);
        request.CertificateExtensions.Add(names.Build());

        // Through PKCS#12: QUIC on Windows needs a certificate whose private key can be exported.
        using var generated = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30));
        return X509CertificateLoader.LoadPkcs12(generated.Export(X509ContentType.Pkcs12), password: null);
    }

    private interface IServerConnection : IAsyncDisposable
    {
        Task DropAsync(ConnectionDrop how);
    }

    private sealed class TcpServerConnection(TcpClient client) : IServerConnection
    {
        public Task DropAsync(ConnectionDrop how)
        {
            if (how == ConnectionDrop.Reset)
            {
                // The socket itself, closed with a zero linger time: that sends a reset. TcpClient.Close shuts the
                // connection down first, which is the orderly end the other case is about.
                client.Client.LingerState = new LingerOption(true, 0);
                client.Client.Close(0);
            }
            else
            {
                client.Close();
            }

            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            client.Dispose();
            return ValueTask.CompletedTask;
        }
    }

    [SupportedOSPlatform("windows")]
    [SupportedOSPlatform("linux")]
    [SupportedOSPlatform("macos")]
    private sealed class QuicServerConnection(QuicConnection connection, QuicStream stream) : IServerConnection
    {
        public async Task DropAsync(ConnectionDrop how)
        {
            if (how == ConnectionDrop.Reset)
            {
                stream.Abort(QuicAbortDirection.Both, 0x1);
            }
            else
            {
                stream.CompleteWrites();
            }

            await connection.CloseAsync(0);
        }

        public async ValueTask DisposeAsync()
        {
            await stream.DisposeAsync();
            await connection.DisposeAsync();
        }
    }
}
