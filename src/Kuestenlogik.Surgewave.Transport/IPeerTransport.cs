using System.Net;

namespace Kuestenlogik.Surgewave.Transport;

/// <summary>
/// Byte-level transport abstraction for broker-to-broker communication
/// (Raft RPC, partition replication, cross-cluster geo-replication).
/// </summary>
/// <remarks>
/// <para>
/// This is the server-to-server counterpart of <see cref="ISurgewaveTransport"/>.
/// Whereas <c>ISurgewaveTransport</c> is shaped for client→broker request/response
/// with protocol-specific framing, <c>IPeerTransport</c> is a thin wrapper
/// around a bidirectional byte channel: implementations expose <c>TcpClient</c>
/// / <c>QuicConnection</c> (or equivalents) through a common <see cref="Stream"/>
/// surface so Raft, replication, and geo-link code can speak their existing
/// binary protocols on top without caring whether the underlying pipe is TCP
/// or QUIC.
/// </para>
/// <para>
/// Each <see cref="IPeerConnection"/> is long-lived — one connection per peer,
/// reused across many RPCs. Failures bubble up through <c>Stream</c> I/O
/// errors; the caller is responsible for disposing the failed connection and
/// re-establishing it (same pattern the existing <c>RaftTransport</c> uses).
/// </para>
/// </remarks>
public interface IPeerTransport
{
    /// <summary>Short identifier used in config and telemetry, e.g. "tcp" or "quic".</summary>
    string Name { get; }

    /// <summary>
    /// Opens a new bidirectional connection to the specified peer.
    /// </summary>
    ValueTask<IPeerConnection> ConnectAsync(string host, int port, CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates a listener bound to <paramref name="endpoint"/>. Call
    /// <see cref="IPeerListener.StartAsync"/> before accepting connections.
    /// </summary>
    IPeerListener CreateListener(IPEndPoint endpoint);
}

/// <summary>
/// A long-lived peer connection exposing a bidirectional byte stream. The
/// default <see cref="Stream"/> is the shared channel with TCP-style sequential
/// semantics. Callers that want to issue multiple concurrent RPCs on the same
/// connection should use <see cref="AcquireStreamAsync"/>, which on QUIC opens
/// a fresh stream per call (independent flow control, no head-of-line blocking
/// between concurrent RPCs) and on TCP serialises access via an internal lock.
/// </summary>
public interface IPeerConnection : IAsyncDisposable
{
    /// <summary>
    /// The default bidirectional stream. On both TCP and QUIC this returns
    /// the connection's primary stream. Callers must serialise access
    /// themselves; for parallel RPCs use <see cref="AcquireStreamAsync"/>.
    /// </summary>
    Stream Stream { get; }

    /// <summary>Remote endpoint of the connection, when known.</summary>
    EndPoint? RemoteEndPoint { get; }

    /// <summary>
    /// Best-effort liveness check. <c>false</c> means the connection is known
    /// to be broken and should be discarded; <c>true</c> does not guarantee
    /// the next I/O will succeed.
    /// </summary>
    bool IsConnected { get; }

    /// <summary>
    /// Acquires an outbound stream lease scoped to a single RPC. On QUIC the
    /// lease opens a fresh bidirectional stream, so concurrent RPCs on the same
    /// peer do not block each other under packet loss. On TCP the lease holds a
    /// lock on the connection's single stream, serialising access while preserving
    /// the same API. Always dispose the lease when the RPC completes (preferably
    /// with <c>await using</c>).
    /// </summary>
    ValueTask<IPeerStreamLease> AcquireStreamAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Server-side counterpart to <see cref="AcquireStreamAsync"/>: waits for an
    /// inbound stream opened by the remote peer. On QUIC this accepts a new
    /// bidirectional stream from the underlying connection, allowing the server to
    /// handle many concurrent RPCs per connection. On TCP it returns the single
    /// shared stream with a serialisation lock (one RPC at a time).
    /// </summary>
    ValueTask<IPeerStreamLease> AcceptInboundStreamAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// A scoped lease on a <see cref="Stream"/> carried by an
/// <see cref="IPeerConnection"/>. Disposing the lease releases the underlying
/// resource — for QUIC that closes the per-RPC stream; for TCP it releases
/// the connection-level write lock.
/// </summary>
public interface IPeerStreamLease : IAsyncDisposable
{
    /// <summary>The stream to read/write during this RPC.</summary>
    Stream Stream { get; }
}

/// <summary>
/// A bound listener that produces inbound <see cref="IPeerConnection"/>s.
/// </summary>
public interface IPeerListener : IAsyncDisposable
{
    /// <summary>Local endpoint the listener is bound to.</summary>
    IPEndPoint LocalEndPoint { get; }

    /// <summary>
    /// Starts accepting connections. For QUIC this performs the asynchronous
    /// listener handshake; for TCP it is essentially free but kept async to
    /// avoid branching at call sites.
    /// </summary>
    ValueTask StartAsync(CancellationToken cancellationToken = default);

    /// <summary>Awaits the next inbound peer connection.</summary>
    ValueTask<IPeerConnection> AcceptAsync(CancellationToken cancellationToken = default);
}
