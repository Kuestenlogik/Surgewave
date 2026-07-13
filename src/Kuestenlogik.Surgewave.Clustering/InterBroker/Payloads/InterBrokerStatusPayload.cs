using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads;
using Kuestenlogik.Surgewave.Protocol.Native.Serialization;

namespace Kuestenlogik.Surgewave.Clustering.InterBroker.Payloads;

/// <summary>
/// #60 Inc4 — a bare status ack/response for the native inter-broker RPCs. Carries a single
/// <see cref="ClusterRpcStatus"/> (raw int16). Used as the response payload for fire-and-forget
/// control ops (e.g. UpdateMetadata) and as the body of a frame-level <see cref="SurgewaveOpCode.Error"/>
/// response.
/// </summary>
public readonly record struct InterBrokerStatusPayload(ClusterRpcStatus Status)
    : ISerializablePayload<InterBrokerStatusPayload>
{
    public static InterBrokerStatusPayload Read(ref SurgewavePayloadReader reader)
        => new((ClusterRpcStatus)reader.ReadInt16());

    public void Write(ref SurgewavePayloadWriter writer) => writer.WriteInt16((short)Status);

    public void WriteTo(IPayloadWriter writer) => writer.WriteInt16((short)Status);

    public int EstimateSize() => 2;
}
