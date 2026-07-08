using System.Net;

namespace Kuestenlogik.Surgewave.Protocol;

/// <summary>
/// A per-connection protocol handler for the broker's shared listener. On an
/// accepted connection the broker peeks the first magic bytes and hands the
/// connection to the first registered handler whose <see cref="CanHandle"/>
/// returns <c>true</c>, walking handlers by ascending <see cref="Order"/>. The
/// chosen handler owns the connection for its whole lifetime.
/// <para>
/// This replaces the hardwired native-vs-Kafka branch so protocols (Kafka today,
/// others later) plug in as registered connection handlers rather than being
/// baked into the broker — the seam that lets Kafka move into a plugin (#59).
/// Selection is per CONNECTION (walked once at handoff), never per request.
/// </para>
/// </summary>
public interface IConnectionHandler
{
    /// <summary>
    /// Selection order — lower runs first. The native protocol registers at
    /// <c>0</c> (specific magic match); a catch-all fallback (e.g. Kafka) registers
    /// at <see cref="int.MaxValue"/> so it only claims connections no more specific
    /// handler wanted.
    /// </summary>
    int Order { get; }

    /// <summary>
    /// Whether this handler claims a connection whose peeked prefix is
    /// <paramref name="magic"/> (the first bytes already read off the stream).
    /// </summary>
    bool CanHandle(ReadOnlySpan<byte> magic);

    /// <summary>
    /// Owns the connection on <paramref name="stream"/> until the peer disconnects
    /// or <paramref name="cancellationToken"/> fires. The already-peeked
    /// <paramref name="magic"/> bytes are passed so the handler can re-inject them
    /// if its framing needs them: the native protocol consumes them off the stream
    /// and ignores this copy, whereas the Kafka prefix IS the request size header
    /// and must be prepended back onto the stream.
    /// </summary>
    Task HandleConnectionAsync(
        Stream stream,
        ReadOnlyMemory<byte> magic,
        ConnectionContext context,
        CancellationToken cancellationToken);
}

/// <summary>
/// Per-connection metadata handed to an <see cref="IConnectionHandler"/>.
/// </summary>
public sealed record ConnectionContext(string ClientHost, EndPoint? Endpoint);
