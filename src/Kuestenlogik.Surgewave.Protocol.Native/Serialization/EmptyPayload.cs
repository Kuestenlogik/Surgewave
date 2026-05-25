using Kuestenlogik.Surgewave.Protocol.Native.Payloads;

namespace Kuestenlogik.Surgewave.Protocol.Native.Serialization;

/// <summary>
/// Empty payload for operations that don't require request or response data.
/// </summary>
public readonly record struct EmptyPayload : ISerializablePayload<EmptyPayload>
{
    /// <summary>
    /// Singleton instance.
    /// </summary>
    public static readonly EmptyPayload Instance = new();

    public static EmptyPayload Read(ref SurgewavePayloadReader reader) => Instance;

    public void Write(ref SurgewavePayloadWriter writer) { }

    public void WriteTo(IPayloadWriter writer) { }

    public int EstimateSize() => 0;
}
