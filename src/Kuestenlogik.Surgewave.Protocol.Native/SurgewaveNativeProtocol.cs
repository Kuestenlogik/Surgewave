namespace Kuestenlogik.Surgewave.Protocol.Native;

/// <summary>
/// Surgewave Native Protocol - optimized binary protocol for high-performance clients.
///
/// Wire Format:
/// - Magic: "STRM" (4 bytes) - only on first message after connection
/// - Version: 1 byte (protocol version)
/// - Flags: 1 byte (compression, streaming, etc.)
/// - RequestId: 4 bytes (uint32, big-endian / network byte order)
/// - OpCode: 2 bytes (operation type)
/// - PayloadLength: 4 bytes (uint32, big-endian)
/// - Payload: variable length
///
/// Design advantages:
/// - Big-endian (network byte order) for protocol consistency with Kafka
/// - No per-field length prefixes for fixed-size types
/// - Batch operations in single request
/// - Optional compression at message level
/// - Request pipelining support
/// </summary>
public static class SurgewaveNativeProtocol
{
    /// <summary>
    /// Magic bytes to identify Surgewave native protocol: "STRM"
    /// </summary>
    public static ReadOnlySpan<byte> Magic => "STRM"u8;

    /// <summary>
    /// Current protocol version
    /// </summary>
    public const byte Version = 1;

    /// <summary>
    /// Header size (excluding magic bytes which are only sent once)
    /// </summary>
    public const int HeaderSize = 12; // flags(1) + reserved(1) + requestId(4) + opCode(2) + payloadLength(4)

    /// <summary>
    /// Maximum payload size (100MB)
    /// </summary>
    public const int MaxPayloadSize = 100 * 1024 * 1024;
}
