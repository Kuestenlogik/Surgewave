namespace Kuestenlogik.Surgewave.Protocol;

/// <summary>
/// Interface for protocol-specific message handling.
/// Implementations handle parsing, serialization, and I/O for a specific wire protocol.
/// </summary>
public interface IProtocolHandler
{
    /// <summary>
    /// Get the protocol name (e.g., "kafka", "native", "grpc")
    /// </summary>
    string ProtocolName { get; }

    /// <summary>
    /// Get the protocol version
    /// </summary>
    string ProtocolVersion { get; }

    /// <summary>
    /// Parse a request from binary data
    /// </summary>
    IProtocolRequest ParseRequest(ReadOnlySpan<byte> data);

    /// <summary>
    /// Parse a response from binary data
    /// </summary>
    IProtocolResponse ParseResponse(ReadOnlySpan<byte> data);

    /// <summary>
    /// Read a complete request from the stream asynchronously.
    /// Handles protocol-specific framing (e.g., size prefix).
    /// </summary>
    Task<(int size, IProtocolRequest request)> ReadRequestAsync(Stream stream, CancellationToken cancellationToken = default);

    /// <summary>
    /// Write a response to the stream asynchronously.
    /// Handles protocol-specific framing (e.g., size prefix).
    /// </summary>
    Task WriteResponseAsync(Stream stream, IProtocolResponse response, CancellationToken cancellationToken = default);
}
