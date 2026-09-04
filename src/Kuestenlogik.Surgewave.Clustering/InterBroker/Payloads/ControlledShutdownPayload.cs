using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads;
using Kuestenlogik.Surgewave.Protocol.Native.Serialization;

namespace Kuestenlogik.Surgewave.Clustering.InterBroker.Payloads;

/// <summary>
/// A broker asking the controller to take its partition leaderships away before it stops serving.
/// </summary>
/// <remarks>
/// Only the controller may elect a leader, so a departing broker cannot hand its own partitions
/// over — it has to ask. Without this request the cluster learns of the departure only when the
/// heartbeat times out, and until then every partition the broker led has a leader that has left:
/// produces to it fail for the whole detection window even though healthy, in-sync replicas are
/// standing by (#180). This is the same job as Kafka's ControlledShutdown API.
/// <para>
/// The epoch is the sender's broker epoch, so a controller can tell a current request from one
/// left over by a previous incarnation of the same broker id.
/// </para>
/// </remarks>
public readonly record struct ControlledShutdownPayload(int BrokerId, long BrokerEpoch)
    : ISerializablePayload<ControlledShutdownPayload>
{
    public static ControlledShutdownPayload Read(ref SurgewavePayloadReader reader)
    {
        var brokerId = reader.ReadInt32();
        var brokerEpoch = reader.ReadInt64();
        return new(brokerId, brokerEpoch);
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteInt32(BrokerId);
        writer.WriteInt64(BrokerEpoch);
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteInt32(BrokerId);
        writer.WriteInt64(BrokerEpoch);
    }

    public int EstimateSize() => 4 + 8;
}
