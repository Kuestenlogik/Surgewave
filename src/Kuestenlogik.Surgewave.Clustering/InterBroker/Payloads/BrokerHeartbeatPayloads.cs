using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads;
using Kuestenlogik.Surgewave.Protocol.Native.Serialization;

namespace Kuestenlogik.Surgewave.Clustering.InterBroker.Payloads;

// #60 Inc2 — native SRWV payloads for the inter-broker BrokerHeartbeat RPC
// (opcode SurgewaveOpCode.InterBrokerHeartbeat). Definitions + serialization only; wired into the
// native inter-broker server (Inc4) and the native lifecycle client (Inc6) later. Native frames
// only ever travel between native-capable peers (old peers get the Kafka-wire fallback), so this
// format need NOT match the Kafka wire — it only has to round-trip with itself.

/// <summary>Native wire form of <see cref="BrokerHeartbeatInput"/>.</summary>
public readonly record struct BrokerHeartbeatRequestPayload(BrokerHeartbeatInput Input)
    : ISerializablePayload<BrokerHeartbeatRequestPayload>
{
    public static BrokerHeartbeatRequestPayload Read(ref SurgewavePayloadReader reader)
        => new(new BrokerHeartbeatInput(
            BrokerId: reader.ReadInt32(),
            BrokerEpoch: reader.ReadInt64(),
            CurrentMetadataOffset: reader.ReadInt64(),
            WantFence: reader.ReadBoolean(),
            WantShutDown: reader.ReadBoolean()));

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteInt32(Input.BrokerId);
        writer.WriteInt64(Input.BrokerEpoch);
        writer.WriteInt64(Input.CurrentMetadataOffset);
        writer.WriteBoolean(Input.WantFence);
        writer.WriteBoolean(Input.WantShutDown);
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteInt32(Input.BrokerId);
        writer.WriteInt64(Input.BrokerEpoch);
        writer.WriteInt64(Input.CurrentMetadataOffset);
        writer.WriteBoolean(Input.WantFence);
        writer.WriteBoolean(Input.WantShutDown);
    }

    public int EstimateSize() => 4 + 8 + 8 + 1 + 1;
}

/// <summary>Native wire form of <see cref="BrokerHeartbeatOutcome"/>.</summary>
public readonly record struct BrokerHeartbeatResponsePayload(BrokerHeartbeatOutcome Outcome)
    : ISerializablePayload<BrokerHeartbeatResponsePayload>
{
    public static BrokerHeartbeatResponsePayload Read(ref SurgewavePayloadReader reader)
        => new(new BrokerHeartbeatOutcome(
            Status: (ClusterRpcStatus)reader.ReadInt16(),
            IsFenced: reader.ReadBoolean(),
            IsCaughtUp: reader.ReadBoolean(),
            ShouldShutDown: reader.ReadBoolean()));

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteInt16((short)Outcome.Status);
        writer.WriteBoolean(Outcome.IsFenced);
        writer.WriteBoolean(Outcome.IsCaughtUp);
        writer.WriteBoolean(Outcome.ShouldShutDown);
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteInt16((short)Outcome.Status);
        writer.WriteBoolean(Outcome.IsFenced);
        writer.WriteBoolean(Outcome.IsCaughtUp);
        writer.WriteBoolean(Outcome.ShouldShutDown);
    }

    public int EstimateSize() => 2 + 1 + 1 + 1;
}
