using Kuestenlogik.Surgewave.Protocol.Native.Payloads;

namespace Kuestenlogik.Surgewave.Protocol.Native.Serialization;

/// <summary>
/// Interface for payloads that support unified serialization.
/// Implementations provide both ref struct and interface-based write methods.
/// </summary>
/// <typeparam name="TSelf">The payload type itself (CRTP pattern).</typeparam>
public interface ISerializablePayload<TSelf> where TSelf : ISerializablePayload<TSelf>
{
    /// <summary>
    /// Read payload from binary data.
    /// </summary>
    static abstract TSelf Read(ref SurgewavePayloadReader reader);

    /// <summary>
    /// Write payload to ref struct writer (client-side, pre-sized buffers).
    /// </summary>
    void Write(ref SurgewavePayloadWriter writer);

    /// <summary>
    /// Write payload using IPayloadWriter interface (broker-side BigEndianWriter).
    /// </summary>
    void WriteTo(IPayloadWriter writer);

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    int EstimateSize();
}
