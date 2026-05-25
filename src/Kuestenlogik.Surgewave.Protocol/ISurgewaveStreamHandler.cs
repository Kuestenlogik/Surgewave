using System.Net;

namespace Kuestenlogik.Surgewave.Protocol;

/// <summary>
/// Transport-neutral entry point for the broker's connection pipeline.
/// Given an already-established byte stream, an implementation must auto-detect
/// the wire protocol (Surgewave native, Kafka, ...) and dispatch accordingly.
///
/// Alternative transports (QUIC, shared memory, ...) plug in here without
/// knowing anything about the protocols themselves — they just hand off a
/// <see cref="Stream"/> and let the broker do the dispatch.
/// </summary>
public interface ISurgewaveStreamHandler
{
    /// <summary>
    /// Runs the broker's connection pipeline on <paramref name="stream"/> until the
    /// peer disconnects or <paramref name="cancellationToken"/> fires. The caller
    /// owns the stream and is responsible for disposing it after this method returns.
    /// </summary>
    Task HandleAsync(
        Stream stream,
        string clientHost,
        EndPoint? endpoint,
        CancellationToken cancellationToken);
}

/// <summary>
/// Static handoff point between the broker and alternative Surgewave transports.
/// The broker assigns <see cref="Instance"/> once during startup; protocol plugins
/// read it lazily when they need to process an accepted connection.
/// </summary>
public static class SurgewaveStreamHandlerHolder
{
    public static ISurgewaveStreamHandler? Instance { get; set; }
}
