using System.Collections.Concurrent;
using Kuestenlogik.Surgewave.Transport;
using Microsoft.Extensions.Logging;

namespace Kuestenlogik.Surgewave.Clustering.Replication;

/// <summary>
/// Thread-safe connection pool for broker-to-broker replication connections.
/// Rides on top of <see cref="IPeerTransport"/> so the pooled connections can
/// be backed by TCP, QUIC, or any other registered peer transport.
/// </summary>
#pragma warning disable CA2000 // Connections are either re-queued or disposed in CleanupIdleConnections
public sealed partial class ConnectionPool : IDisposable
#pragma warning restore CA2000
{
    private readonly ConcurrentDictionary<string, ConcurrentQueue<PooledConnection>> _pools = new();
    private readonly ConcurrentDictionary<string, SemaphoreSlim> _semaphores = new();
    private readonly ILogger<ConnectionPool> _logger;
    private readonly IPeerTransport _peerTransport;
    private readonly int _maxConnectionsPerBroker;
    private readonly TimeSpan _connectionTimeout;
    private readonly TimeSpan _idleTimeout;
    private readonly Timer _cleanupTimer;
    private bool _disposed;

    public ConnectionPool(
        ILogger<ConnectionPool> logger,
        IPeerTransport peerTransport,
        int maxConnectionsPerBroker = 10,
        TimeSpan? connectionTimeout = null,
        TimeSpan? idleTimeout = null)
    {
        _logger = logger;
        _peerTransport = peerTransport;
        _maxConnectionsPerBroker = maxConnectionsPerBroker;
        _connectionTimeout = connectionTimeout ?? TimeSpan.FromSeconds(10);
        _idleTimeout = idleTimeout ?? TimeSpan.FromMinutes(5);

        // Cleanup idle connections every minute
        _cleanupTimer = new Timer(CleanupIdleConnections, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
    }

    /// <summary>
    /// Get a connection to the specified broker endpoint.
    /// Returns a pooled connection if available, or creates a new one.
    /// </summary>
    public async Task<PooledConnection> GetConnectionAsync(string host, int port, CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        var endpoint = $"{host}:{port}";

        // Get or create queue and semaphore for this endpoint. Explicit maxCount so an accounting
        // bug over-releasing a permit surfaces as SemaphoreFullException (logged) instead of
        // silently growing the pool bound (the one-arg ctor allows int.MaxValue releases).
        var queue = _pools.GetOrAdd(endpoint, _ => new ConcurrentQueue<PooledConnection>());
        var semaphore = _semaphores.GetOrAdd(endpoint, _ => new SemaphoreSlim(_maxConnectionsPerBroker, _maxConnectionsPerBroker));

        // Try to get an existing connection
        while (queue.TryDequeue(out var connection))
        {
            if (connection.IsAlive && !connection.IsExpired(_idleTimeout))
            {
                // Re-arm the lease: without resetting the returned flag, the SECOND Return()/
                // Discard() of a reused connection would silently no-op — the connection would
                // neither re-pool nor release its permit (one leaked permit + socket per reuse
                // cycle until the endpoint's semaphore is exhausted).
                connection.PrepareForReuse();
                LogConnectionReused(endpoint);
                return connection;
            }

            // Connection is dead or expired: dispose it AND release its creation permit — the
            // semaphore counts live connections, so skipping the release here would leak one permit
            // per dead pooled connection (e.g. after a peer restart) until every GetConnectionAsync
            // for the endpoint blocks on an exhausted semaphore.
            connection.Dispose();
            ReleaseSemaphore(endpoint);
        }

        // Need to create a new connection
        await semaphore.WaitAsync(cancellationToken);
        try
        {
            using var connectCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            connectCts.CancelAfter(_connectionTimeout);

            var peerConnection = await _peerTransport.ConnectAsync(host, port, connectCts.Token);

            var pooledConnection = new PooledConnection(peerConnection, endpoint, this);
            LogConnectionCreated(endpoint);
            return pooledConnection;
        }
        catch
        {
            semaphore.Release();
            throw;
        }
    }

    /// <summary>
    /// Return a connection to the pool for reuse.
    /// </summary>
    internal void ReturnConnection(PooledConnection connection)
    {
        if (_disposed || !connection.IsAlive)
        {
            connection.Dispose();
            ReleaseSemaphore(connection.Endpoint);
            return;
        }

        if (_pools.TryGetValue(connection.Endpoint, out var queue))
        {
            connection.MarkUsed();
            queue.Enqueue(connection);
            LogConnectionReturned(connection.Endpoint);
        }
        else
        {
            connection.Dispose();
            ReleaseSemaphore(connection.Endpoint);
        }
    }

    private void ReleaseSemaphore(string endpoint)
    {
        if (_semaphores.TryGetValue(endpoint, out var semaphore))
        {
            try
            {
                semaphore.Release();
            }
            catch (SemaphoreFullException)
            {
                // Semaphore already at max count - can happen during cleanup races
                LogSemaphoreReleaseSkipped(endpoint);
            }
            catch (ObjectDisposedException)
            {
                // Pool disposal raced this release — nothing left to account for.
            }
        }
    }

    private void CleanupIdleConnections(object? state)
    {
        foreach (var (endpoint, queue) in _pools)
        {
            var itemsToCheck = queue.Count;
            for (int i = 0; i < itemsToCheck; i++)
            {
                // CA2000: Connection is either re-queued or disposed below
#pragma warning disable CA2000
                if (queue.TryDequeue(out var connection))
#pragma warning restore CA2000
                {
                    if (connection.IsAlive && !connection.IsExpired(_idleTimeout))
                    {
                        queue.Enqueue(connection);
                    }
                    else
                    {
                        connection.Dispose();
                        ReleaseSemaphore(endpoint);
                        LogConnectionExpired(endpoint);
                    }
                }
            }
        }
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        _cleanupTimer.Dispose();

        foreach (var (_, queue) in _pools)
        {
            while (queue.TryDequeue(out var connection))
            {
                connection.Dispose();
            }
        }

        foreach (var (_, semaphore) in _semaphores)
        {
            semaphore.Dispose();
        }

        _pools.Clear();
        _semaphores.Clear();
    }

    [LoggerMessage(Level = LogLevel.Debug, Message = "Connection reused for {Endpoint}")]
    private partial void LogConnectionReused(string endpoint);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Connection created for {Endpoint}")]
    private partial void LogConnectionCreated(string endpoint);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Connection returned to pool for {Endpoint}")]
    private partial void LogConnectionReturned(string endpoint);

    [LoggerMessage(Level = LogLevel.Debug, Message = "Idle connection expired for {Endpoint}")]
    private partial void LogConnectionExpired(string endpoint);

    [LoggerMessage(Level = LogLevel.Trace, Message = "Semaphore release skipped for {Endpoint} - already at max count")]
    private partial void LogSemaphoreReleaseSkipped(string endpoint);
}

/// <summary>
/// A pooled peer connection that tracks its usage and can be returned to the
/// pool. Wraps an <see cref="IPeerConnection"/> so the underlying transport
/// (TCP, QUIC, ...) is opaque to the replication code.
/// </summary>
public sealed class PooledConnection : IDisposable
{
    private readonly IPeerConnection _connection;
    private readonly ConnectionPool _pool;
    private DateTime _lastUsed;
    private bool _disposed;

    // 0 = leased out (Return/Discard pending), 1 = returned or discarded. Interlocked so a
    // concurrent double-Return/Discard cannot both pass the guard and double-release the permit.
    private int _returned;

    public string Endpoint { get; }
    public Stream Stream => _connection.Stream;
    public bool IsAlive => _connection.IsConnected && !_disposed;

    internal PooledConnection(IPeerConnection connection, string endpoint, ConnectionPool pool)
    {
        _connection = connection;
        Endpoint = endpoint;
        _pool = pool;
        _lastUsed = DateTime.UtcNow;
    }

    internal void MarkUsed()
    {
        _lastUsed = DateTime.UtcNow;
    }

    /// <summary>
    /// Re-arm this connection as it is handed back out of the pool: refresh the idle timestamp and
    /// reset the returned flag so the NEXT Return()/Discard() is honored again. Without this reset a
    /// reused connection's second Return/Discard would silently no-op (permit + socket leak).
    /// </summary>
    internal void PrepareForReuse()
    {
        _lastUsed = DateTime.UtcNow;
        Volatile.Write(ref _returned, 0);
    }

    internal bool IsExpired(TimeSpan idleTimeout)
    {
        return DateTime.UtcNow - _lastUsed > idleTimeout;
    }

    /// <summary>
    /// Return this connection to the pool for reuse.
    /// After calling this, the connection should not be used anymore.
    /// </summary>
    public void Return()
    {
        if (_disposed || Interlocked.Exchange(ref _returned, 1) == 1) return;
        _pool.ReturnConnection(this);
    }

    /// <summary>
    /// Discard this connection instead of returning it: close it and release its pool permit.
    /// Use after a failed or incomplete exchange (timeout mid-read, protocol mismatch) — the
    /// connection may still be "alive" at the transport level but carry a late response in its
    /// buffer, so handing it back to the pool would poison the next request/response pairing.
    /// </summary>
    public void Discard()
    {
        if (Interlocked.Exchange(ref _returned, 1) == 1) return;
        Dispose();
        // Now !IsAlive, so the pool's dead-connection branch disposes (no-op) and releases the permit.
        _pool.ReturnConnection(this);
    }

    public void Dispose()
    {
        if (_disposed) return;
        _disposed = true;

        // Sync-over-async is acceptable here: we're in broker-side code with
        // no SynchronizationContext, and the underlying TCP dispose is
        // effectively synchronous. QUIC dispose has async work (close frame)
        // but is still non-deadlocking in this context.
        try
        {
            _connection.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
        catch
        {
            // Best-effort disposal of an already-broken connection.
        }
    }
}
