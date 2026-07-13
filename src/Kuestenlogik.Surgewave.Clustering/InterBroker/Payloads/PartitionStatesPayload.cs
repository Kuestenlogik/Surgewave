using Kuestenlogik.Surgewave.Clustering.Cluster;
using Kuestenlogik.Surgewave.Core.Models;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads;
using Kuestenlogik.Surgewave.Protocol.Native.Serialization;

namespace Kuestenlogik.Surgewave.Clustering.InterBroker.Payloads;

/// <summary>
/// #60 Inc2/Inc5 — native SRWV payload carrying the sending controller's identity/epoch, the live
/// broker endpoints, and a list of (TopicPartition, PartitionState). Serves both the
/// InterBrokerLeaderAndIsr and InterBrokerUpdateMetadata opcodes (they carry the same shape; the
/// opcode distinguishes intent).
/// <para>
/// <see cref="ControllerId"/>/<see cref="ControllerEpoch"/> exist so the receiver can fence stale
/// pushes: a delayed frame from a demoted controller carries an older epoch and is rejected with
/// <see cref="Replication.ClusterRpcStatus.StaleControllerEpoch"/> instead of regressing partition
/// metadata during failover (Inc4 review finding #2).
/// </para>
/// <para>
/// <see cref="LiveBrokers"/> is the native counterpart of the Kafka-wire LiveLeaders/LiveBrokers
/// sections: the push that makes a broker a follower must also teach it the leader's endpoint, or a
/// dynamically joined broker could never be fetched from (#69). Unlike the Kafka wire — which only
/// carries the client endpoint, forcing the receiver to GUESS the replication port — each entry
/// carries the real <see cref="LiveBrokerSpec.ReplicationPort"/> plus the broker's advertised
/// <see cref="LiveBrokerSpec.InterBrokerProtocol"/> level, so finalized-level views converge on
/// non-controller brokers.
/// </para>
/// <para>
/// Delayed/reordered same-epoch frames are handled per PARTITION, not per push: each entry carries
/// its <see cref="PartitionState.LeaderEpoch"/>, and the receiver applies a partition only when the
/// incoming leader epoch is not older than the stored one (Inc6a). This orders partition content
/// correctly while letting disjoint partial pushes through — a coarse per-push version would instead
/// drop an unrelated partition's update that happened to arrive with a lower number.
/// </para>
/// </summary>
public readonly record struct PartitionStatesPayload(
    int ControllerId,
    int ControllerEpoch,
    IReadOnlyList<LiveBrokerSpec> LiveBrokers,
    IReadOnlyList<(TopicPartition Tp, PartitionState State)> Entries)
    : ISerializablePayload<PartitionStatesPayload>
{
    // Conservative lower bounds on the bytes one entry occupies. Used to reject a bogus/hostile
    // count before pre-allocating — the native receive server (#60 Inc4) is the first path that
    // feeds this decoder untrusted network bytes.
    private const int MinEntryBytes = 6;   // 2-byte topic length + 4-byte partition, state follows
    private const int MinBrokerBytes = 18; // id(4) + host len(2) + port(4) + replPort(4) + level(2) + rack len prefix(2, -1 for null)

    public static PartitionStatesPayload Read(ref SurgewavePayloadReader reader)
    {
        var controllerId = reader.ReadInt32();
        var controllerEpoch = reader.ReadInt32();

        var brokerCount = reader.ReadInt32();
        if (brokerCount < 0 || brokerCount > reader.Remaining / MinBrokerBytes)
            throw new InvalidDataException($"Corrupt PartitionStates payload: broker count {brokerCount} exceeds {reader.Remaining} remaining bytes");

        var brokers = new List<LiveBrokerSpec>(brokerCount);
        for (var i = 0; i < brokerCount; i++)
        {
            brokers.Add(new LiveBrokerSpec(
                BrokerId: reader.ReadInt32(),
                Host: reader.ReadString() ?? string.Empty,
                Port: reader.ReadInt32(),
                ReplicationPort: reader.ReadInt32(),
                InterBrokerProtocol: reader.ReadInt16(),
                Rack: reader.ReadString()));
        }

        var count = reader.ReadInt32();
        if (count < 0 || count > reader.Remaining / MinEntryBytes)
            throw new InvalidDataException($"Corrupt PartitionStates payload: entry count {count} exceeds {reader.Remaining} remaining bytes");

        var entries = new List<(TopicPartition, PartitionState)>(count);
        for (var i = 0; i < count; i++)
        {
            var tp = InterBrokerWire.ReadTopicPartition(ref reader);
            var state = InterBrokerWire.ReadState(ref reader, tp);
            entries.Add((tp, state));
        }
        return new(controllerId, controllerEpoch, brokers, entries);
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteInt32(ControllerId);
        writer.WriteInt32(ControllerEpoch);
        writer.WriteInt32(LiveBrokers.Count);
        foreach (var b in LiveBrokers)
        {
            writer.WriteInt32(b.BrokerId);
            writer.WriteString(b.Host);
            writer.WriteInt32(b.Port);
            writer.WriteInt32(b.ReplicationPort);
            writer.WriteInt16(b.InterBrokerProtocol);
            writer.WriteString(b.Rack);
        }
        writer.WriteInt32(Entries.Count);
        foreach (var (tp, state) in Entries)
        {
            InterBrokerWire.Write(ref writer, tp);
            InterBrokerWire.WriteState(ref writer, state);
        }
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteInt32(ControllerId);
        writer.WriteInt32(ControllerEpoch);
        writer.WriteInt32(LiveBrokers.Count);
        foreach (var b in LiveBrokers)
        {
            writer.WriteInt32(b.BrokerId);
            writer.WriteString(b.Host);
            writer.WriteInt32(b.Port);
            writer.WriteInt32(b.ReplicationPort);
            writer.WriteInt16(b.InterBrokerProtocol);
            writer.WriteString(b.Rack);
        }
        writer.WriteInt32(Entries.Count);
        foreach (var (tp, state) in Entries)
        {
            InterBrokerWire.Write(writer, tp);
            InterBrokerWire.WriteState(writer, state);
        }
    }

    public int EstimateSize()
    {
        var size = 4 + 4 + 4 + 4; // controllerId + epoch + brokerCount + entryCount
        foreach (var b in LiveBrokers)
            size += 4 + (2 + b.Host.Length * 3) + 4 + 4 + 2 + (2 + (b.Rack?.Length ?? 0) * 3);
        foreach (var (tp, state) in Entries)
            size += InterBrokerWire.SizeOf(tp) + InterBrokerWire.SizeOfState(state);
        return size;
    }
}

/// <summary>
/// #60 Inc5 — one live-broker endpoint entry inside <see cref="PartitionStatesPayload"/>: the full
/// inter-broker identity of a peer (client endpoint, real replication endpoint, advertised
/// inter-broker protocol level, rack).
/// </summary>
public readonly record struct LiveBrokerSpec(
    int BrokerId,
    string Host,
    int Port,
    int ReplicationPort,
    short InterBrokerProtocol,
    string? Rack);
