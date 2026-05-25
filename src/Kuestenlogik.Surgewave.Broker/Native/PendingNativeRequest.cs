using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Broker.Native;

/// <summary>
/// Represents a pending native protocol request in the Channel pipeline.
/// Carries ownership of the rented payload buffer (must be returned to ArrayPool after processing).
/// </summary>
/// <param name="Header">The parsed request header.</param>
/// <param name="RentedPayload">Buffer rented from ArrayPool. Must be returned after processing.</param>
/// <param name="PayloadLength">Actual payload length within the rented buffer.</param>
/// <param name="DecompressedPayload">Decompressed payload if original was compressed, otherwise null.</param>
internal readonly record struct PendingNativeRequest(
    SurgewaveRequestHeader Header,
    byte[] RentedPayload,
    int PayloadLength,
    byte[]? DecompressedPayload);
