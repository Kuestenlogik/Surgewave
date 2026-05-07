using System.Net;
using System.Net.Sockets;

namespace Kuestenlogik.Surgewave.Transport.Tcp;

/// <summary>
/// Peer transport backed by raw TCP sockets. Each <see cref="IPeerConnection"/>
/// wraps a <see cref="TcpClient"/> and exposes its <see cref="NetworkStream"/>.
/// </summary>
public sealed class TcpPeerTransport : IPeerTransport
{
    public const string TransportName = "tcp";

    public string Name => TransportName;

    public async ValueTask<IPeerConnection> ConnectAsync(
        string host,
        int port,
        CancellationToken cancellationToken = default)
    {
        TcpClient? client = null;
        try
        {
            client = new TcpClient { NoDelay = true };
            await client.ConnectAsync(host, port, cancellationToken).ConfigureAwait(false);
            var connection = new TcpPeerConnection(client);
            client = null; // ownership transferred
            PeerTransportMetrics.Instance.RecordConnectionOpened(TransportName);
            PeerTransportMetrics.Instance.IncrementActiveConnections(TransportName);
            return connection;
        }
        finally
        {
            client?.Dispose();
        }
    }

    public IPeerListener CreateListener(IPEndPoint endpoint)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        return new TcpPeerListener(endpoint);
    }
}

internal sealed class TcpPeerConnection : IPeerConnection
{
    private readonly TcpClient _client;
    private readonly NetworkStream _stream;
    // Single-stream serialisation for AcquireStreamAsync callers. TCP can only
    // safely carry one concurrent RPC on a given connection, so the lease
    // blocks on this semaphore and releases it on dispose.
    private readonly SemaphoreSlim _streamLock = new(1, 1);

    public TcpPeerConnection(TcpClient client)
    {
        _client = client;
        _stream = client.GetStream();
    }

    public Stream Stream => _stream;
    public EndPoint? RemoteEndPoint => _client.Client?.RemoteEndPoint;
    public bool IsConnected => _client.Connected;

    public async ValueTask<IPeerStreamLease> AcquireStreamAsync(CancellationToken cancellationToken = default)
    {
        await _streamLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        var semaphore = _streamLock;
        return new TcpPeerStreamLease(_stream, () => semaphore.Release());
    }

    public async ValueTask<IPeerStreamLease> AcceptInboundStreamAsync(CancellationToken cancellationToken = default)
    {
        // TCP: only one stream per connection. Serialise access via the same
        // lock so server-side reads don't overlap with client-side writes.
        await _streamLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        var semaphore = _streamLock;
        return new TcpPeerStreamLease(_stream, () => semaphore.Release());
    }

    public async ValueTask DisposeAsync()
    {
        await _stream.DisposeAsync().ConfigureAwait(false);
        _client.Dispose();
        _streamLock.Dispose();
        PeerTransportMetrics.Instance.RecordConnectionClosed(TcpPeerTransport.TransportName);
        PeerTransportMetrics.Instance.DecrementActiveConnections(TcpPeerTransport.TransportName);
    }

    private sealed class TcpPeerStreamLease : IPeerStreamLease
    {
        private readonly Action _release;
        private int _disposed;

        public TcpPeerStreamLease(Stream stream, Action release)
        {
            Stream = stream;
            _release = release;
        }

        public Stream Stream { get; }

        public ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                _release();
            }
            return ValueTask.CompletedTask;
        }
    }
}

internal sealed class TcpPeerListener : IPeerListener
{
    private readonly TcpListener _listener;
    private bool _started;

    public TcpPeerListener(IPEndPoint endpoint)
    {
        _listener = new TcpListener(endpoint);
    }

    public IPEndPoint LocalEndPoint => (IPEndPoint)_listener.LocalEndpoint;

    public ValueTask StartAsync(CancellationToken cancellationToken = default)
    {
        if (_started) return ValueTask.CompletedTask;
        _listener.Start();
        _started = true;
        return ValueTask.CompletedTask;
    }

    public async ValueTask<IPeerConnection> AcceptAsync(CancellationToken cancellationToken = default)
    {
        TcpClient? client = null;
        try
        {
            client = await _listener.AcceptTcpClientAsync(cancellationToken).ConfigureAwait(false);
            client.NoDelay = true;
            var connection = new TcpPeerConnection(client);
            client = null;
            return connection;
        }
        finally
        {
            client?.Dispose();
        }
    }

    public ValueTask DisposeAsync()
    {
        _listener.Stop();
        _listener.Dispose();
        return ValueTask.CompletedTask;
    }
}
