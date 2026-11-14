using System.Net;
using System.Net.Sockets;
using Kuestenlogik.Surgewave.Broker.Native;
using Kuestenlogik.Surgewave.Broker.Native.Handlers;
using Kuestenlogik.Surgewave.Protocol.Native;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Broker.Tests.Fakes;

/// <summary>
/// The broker's native connection loop (<see cref="SurgewaveNativeHandler"/>) behind a loopback listener, with the
/// handlers a test supplies. It treats an accepted connection as <c>SurgewaveBroker</c> does: it reads the four
/// magic bytes, runs the native handler, and closes the socket when the handler returns or throws.
/// </summary>
internal sealed class NativeLoopbackBroker : IAsyncDisposable
{
    private readonly TcpListener _listener;
    private readonly SurgewaveNativeHandler _handler;
    private readonly CancellationTokenSource _stopping = new();
    private readonly Task _acceptTask;
    private readonly List<Task> _connections = [];

    public NativeLoopbackBroker(IEnumerable<INativeRequestHandler> handlers, ILogger<SurgewaveNativeHandler> logger)
    {
        _handler = new SurgewaveNativeHandler(new NativeRequestDispatcher(handlers), new BrokerConfig(), logger);
        _listener = new TcpListener(IPAddress.Loopback, 0);
        _listener.Start();
        Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
        _acceptTask = AcceptAsync();
    }

    public int Port { get; }

    /// <summary>What a stopping broker does to its connections: it cancels the token every handler was given.</summary>
    public Task StopAsync() => _stopping.CancelAsync();

    private async Task AcceptAsync()
    {
        try
        {
            while (true)
            {
                var client = await _listener.AcceptTcpClientAsync(_stopping.Token);
                lock (_connections)
                {
                    _connections.Add(ServeAsync(client));
                }
            }
        }
        catch (OperationCanceledException) { /* stopping */ }
        catch (SocketException) { /* listener stopped */ }
    }

    private async Task ServeAsync(TcpClient client)
    {
        using var _ = client;
        client.NoDelay = true;
        try
        {
            var stream = client.GetStream();
            var magic = new byte[SurgewaveNativeProtocol.Magic.Length];
            await stream.ReadExactlyAsync(magic, _stopping.Token);
            await _handler.HandleConnectionAsync(stream, _stopping.Token);
        }
        catch (Exception)
        {
            // Whatever ends a connection, the broker logs it and closes the socket, which leaving this method does.
        }
    }

    public async ValueTask DisposeAsync()
    {
        // The loop first, the listener after it: stopped under the loop, the listener answered the loop's next accept
        // with "Not listening" — an InvalidOperationException, which is neither of the two ends the loop expects —
        // and the dispose rethrew it into a test that had passed.
        await _stopping.CancelAsync();
        await _acceptTask;
        _listener.Stop();

        Task[] connections;
        lock (_connections)
        {
            connections = [.. _connections];
        }

        await Task.WhenAll(connections);
        _listener.Dispose();
        _stopping.Dispose();
    }
}
