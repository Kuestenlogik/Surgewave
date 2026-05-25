namespace Kuestenlogik.Surgewave.Protocol.Native;

/// <summary>
/// Protocol flags for request/response
/// </summary>
[Flags]
public enum SurgewaveProtocolFlags : byte
{
    None = 0,
    Compressed = 1 << 0,      // Payload is compressed (LZ4)
    Streaming = 1 << 1,       // Response will be streamed
    BatchRequest = 1 << 2,    // Request contains multiple operations
    NoResponse = 1 << 3,      // Fire-and-forget, no response expected
    LastInBatch = 1 << 4,     // Last message in a batch
}
