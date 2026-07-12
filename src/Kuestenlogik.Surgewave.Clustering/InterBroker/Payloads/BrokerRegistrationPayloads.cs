using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Clustering.Replication;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads;
using Kuestenlogik.Surgewave.Protocol.Native.Serialization;

namespace Kuestenlogik.Surgewave.Clustering.InterBroker.Payloads;

// #60 Inc2 — native SRWV payloads for the inter-broker BrokerRegistration RPC
// (opcode SurgewaveOpCode.InterBrokerRegistration). See BrokerHeartbeatPayloads for the rationale:
// native-only wire, round-trip-consistent, not Kafka-byte-compatible.

/// <summary>Native wire form of <see cref="BrokerRegistrationInput"/>.</summary>
public readonly record struct BrokerRegistrationRequestPayload(BrokerRegistrationInput Input)
    : ISerializablePayload<BrokerRegistrationRequestPayload>
{
    public static BrokerRegistrationRequestPayload Read(ref SurgewavePayloadReader reader)
    {
        var brokerId = reader.ReadInt32();
        var clusterId = reader.ReadString() ?? string.Empty;
        var incarnationId = new Guid(reader.ReadBytes());

        var listenerCount = reader.ReadInt32();
        var listeners = new List<ListenerSpec>(listenerCount);
        for (var i = 0; i < listenerCount; i++)
        {
            listeners.Add(new ListenerSpec(
                Name: reader.ReadString() ?? string.Empty,
                Host: reader.ReadString() ?? string.Empty,
                Port: reader.ReadInt32(),
                SecurityProtocol: reader.ReadInt16()));
        }

        var featureCount = reader.ReadInt32();
        var features = new List<FeatureSpec>(featureCount);
        for (var i = 0; i < featureCount; i++)
        {
            features.Add(new FeatureSpec(
                Name: reader.ReadString() ?? string.Empty,
                MinSupportedVersion: reader.ReadInt16(),
                MaxSupportedVersion: reader.ReadInt16()));
        }

        var rack = reader.ReadNullableString();
        var previousBrokerEpoch = reader.ReadInt64();

        return new(new BrokerRegistrationInput(brokerId, clusterId, incarnationId, listeners, features, rack, previousBrokerEpoch));
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteInt32(Input.BrokerId);
        writer.WriteString(Input.ClusterId);
        writer.WriteBytes(Input.IncarnationId.ToByteArray());

        writer.WriteInt32(Input.Listeners.Count);
        foreach (var l in Input.Listeners)
        {
            writer.WriteString(l.Name);
            writer.WriteString(l.Host);
            writer.WriteInt32(l.Port);
            writer.WriteInt16(l.SecurityProtocol);
        }

        writer.WriteInt32(Input.Features.Count);
        foreach (var f in Input.Features)
        {
            writer.WriteString(f.Name);
            writer.WriteInt16(f.MinSupportedVersion);
            writer.WriteInt16(f.MaxSupportedVersion);
        }

        writer.WriteNullableString(Input.Rack);
        writer.WriteInt64(Input.PreviousBrokerEpoch);
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteInt32(Input.BrokerId);
        writer.WriteString(Input.ClusterId);
        writer.WriteBytes(Input.IncarnationId.ToByteArray());

        writer.WriteInt32(Input.Listeners.Count);
        foreach (var l in Input.Listeners)
        {
            writer.WriteString(l.Name);
            writer.WriteString(l.Host);
            writer.WriteInt32(l.Port);
            writer.WriteInt16(l.SecurityProtocol);
        }

        writer.WriteInt32(Input.Features.Count);
        foreach (var f in Input.Features)
        {
            writer.WriteString(f.Name);
            writer.WriteInt16(f.MinSupportedVersion);
            writer.WriteInt16(f.MaxSupportedVersion);
        }

        writer.WriteNullableString(Input.Rack);
        writer.WriteInt64(Input.PreviousBrokerEpoch);
    }

    public int EstimateSize()
    {
        var size = 4                          // BrokerId
            + Str(Input.ClusterId)
            + 4 + 16                          // IncarnationId as length-prefixed 16 bytes
            + 4                               // listener count
            + 4                               // feature count
            + 1 + (Input.Rack is null ? 0 : Str(Input.Rack))  // nullable rack (1-byte marker)
            + 8;                              // PreviousBrokerEpoch
        foreach (var l in Input.Listeners)
            size += Str(l.Name) + Str(l.Host) + 4 + 2;
        foreach (var f in Input.Features)
            size += Str(f.Name) + 2 + 2;
        return size;
    }

    // Upper bound: 2-byte length prefix + worst-case UTF-8 (<=3 bytes per UTF-16 char).
    private static int Str(string s) => 2 + s.Length * 3;
}

/// <summary>Native wire form of <see cref="BrokerRegistrationOutcome"/>.</summary>
public readonly record struct BrokerRegistrationResponsePayload(BrokerRegistrationOutcome Outcome)
    : ISerializablePayload<BrokerRegistrationResponsePayload>
{
    public static BrokerRegistrationResponsePayload Read(ref SurgewavePayloadReader reader)
        => new(new BrokerRegistrationOutcome(
            Status: (ClusterRpcStatus)reader.ReadInt16(),
            BrokerEpoch: reader.ReadInt64()));

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteInt16((short)Outcome.Status);
        writer.WriteInt64(Outcome.BrokerEpoch);
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteInt16((short)Outcome.Status);
        writer.WriteInt64(Outcome.BrokerEpoch);
    }

    public int EstimateSize() => 2 + 8;
}
