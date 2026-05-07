using Kuestenlogik.Surgewave.Client.Native.Operations.Cluster;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.Cluster;

namespace Kuestenlogik.Surgewave.Client.Native.Commands.Cluster;

/// <summary>
/// Command to list all brokers in the cluster.
/// </summary>
public sealed class ListBrokersCommand : NoRequestCommand<List<BrokerInfo>>
{
    public override SurgewaveOpCode OpCode => SurgewaveOpCode.ListBrokers;

    public override List<BrokerInfo> ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        var wirePayload = ListBrokersPayload.Read(ref reader);
        var brokers = new List<BrokerInfo>(wirePayload.Brokers.Count);

        foreach (var b in wirePayload.Brokers)
        {
            brokers.Add(new BrokerInfo
            {
                BrokerId = b.BrokerId,
                Host = b.Host,
                Port = b.Port,
                ReplicationPort = b.ReplicationPort,
                IsController = b.IsController,
                IsAlive = b.IsAlive,
                Rack = b.Rack
            });
        }

        return brokers;
    }
}
