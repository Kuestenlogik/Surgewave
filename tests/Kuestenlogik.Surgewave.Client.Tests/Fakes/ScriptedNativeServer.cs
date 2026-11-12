using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Client.Tests.Fakes;

/// <summary>
/// A native-protocol server on a loopback socket that answers each request the way a test tells it to and closes
/// the connection when the test says so: what a native client sees of a broker, over the transport it really uses.
/// </summary>
/// <remarks>
/// <see cref="FakeSurgewaveTransport"/> stands in for the transport; this stands in for the broker behind a real
/// one, for what only shows there — a connection that ends, and the frames a broker answers with.
/// </remarks>
internal sealed class ScriptedNativeServer : IAsyncDisposable
{
    /// <summary>An answer: the frame's op code and error code and its payload. Null answers nothing.</summary>
    internal sealed record Answer(SurgewaveOpCode OpCode, SurgewaveErrorCode ErrorCode, byte[] Payload)
    {
        public static Answer Ok(SurgewaveOpCode opCode, byte[]? payload = null) => new(opCode, SurgewaveErrorCode.None, payload ?? []);

        /// <summary>An error frame as the broker writes it: the message behind a two-byte length.</summary>
        public static Answer Error(SurgewaveErrorCode errorCode, string message)
        {
            var text = Encoding.UTF8.GetBytes(message);
            var payload = new byte[2 + text.Length];
            BinaryPrimitives.WriteInt16BigEndian(payload, (short)text.Length);
            text.CopyTo(payload, 2);
            return new Answer(SurgewaveOpCode.Error, errorCode, payload);
        }
    }

    private readonly TcpListener _listener;
    private readonly Func<SurgewaveRequestHeader, Answer?> _answer;
    private readonly CancellationTokenSource _stopping = new();
    private readonly TaskCompletionSource<TcpClient> _connected = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly Task _serving;

    public ScriptedNativeServer(Func<SurgewaveRequestHeader, Answer?> answer)
    {
        _answer = answer;
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
        Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
        _serving = ServeAsync();
    }

    public int Port { get; }

    /// <summary>Closes the client's connection, as a broker that stops does. Everything the client sent has been read.</summary>
    public async Task CloseConnectionAsync()
    {
        var client = await _connected.Task;
        await _stopping.CancelAsync();
        await _serving;
        client.Close();
    }

    private async Task ServeAsync()
    {
        var token = _stopping.Token;
        try
        {
            var client = await _listener.AcceptTcpClientAsync(token);
            client.NoDelay = true;
            _connected.TrySetResult(client);
            var stream = client.GetStream();

            // Handshake: magic and version in, a header and the capabilities out (no compression).
            var handshake = new byte[SurgewaveNativeProtocol.Magic.Length + 1];
            await stream.ReadExactlyAsync(handshake, token);
            await WriteAsync(stream, 0, new Answer(SurgewaveOpCode.Handshake, SurgewaveErrorCode.None, [SurgewaveNativeProtocol.Version, 0]), token);

            var headerBytes = new byte[SurgewaveNativeProtocol.HeaderSize];
            while (true)
            {
                await stream.ReadExactlyAsync(headerBytes, token);
                var header = SurgewaveRequestHeader.ReadFrom(headerBytes);
                await stream.ReadExactlyAsync(new byte[header.PayloadLength], token);

                if (_answer(header) is { } answer)
                    await WriteAsync(stream, header.RequestId, answer, token);
            }
        }
        catch (Exception ex) when (ex is OperationCanceledException or IOException or SocketException)
        {
            // Stopped, or the client went first.
        }
    }

    private static async Task WriteAsync(NetworkStream stream, uint requestId, Answer answer, CancellationToken token)
    {
        var frame = new byte[SurgewaveResponseHeader.Size + answer.Payload.Length];
        new SurgewaveResponseHeader
        {
            RequestId = requestId,
            OpCode = answer.OpCode,
            ErrorCode = answer.ErrorCode,
            PayloadLength = answer.Payload.Length,
        }.WriteTo(frame);
        answer.Payload.CopyTo(frame, SurgewaveResponseHeader.Size);
        await stream.WriteAsync(frame, token);
    }

    public async ValueTask DisposeAsync()
    {
        await _stopping.CancelAsync();
        _listener.Stop();
        await _serving;
        if (_connected.Task.IsCompletedSuccessfully)
            (await _connected.Task).Dispose();
        _listener.Dispose();
        _stopping.Dispose();
    }
}
