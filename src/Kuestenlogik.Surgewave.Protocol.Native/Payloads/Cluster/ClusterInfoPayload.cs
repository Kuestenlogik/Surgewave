using Kuestenlogik.Surgewave.Protocol.Native.Payloads;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Cluster;

/// <summary>
/// Wire format for GetClusterInfo response.
/// Shared between broker (write) and client (read) to ensure consistency.
/// </summary>
public readonly record struct ClusterInfoPayload
{
    public int BrokerId { get; init; }
    public string Host { get; init; }
    public int Port { get; init; }
    public bool IsController { get; init; }
    public int ControllerId { get; init; }
    public int ControllerEpoch { get; init; }
    public bool UseRaftConsensus { get; init; }
    public bool IsRaftLeader { get; init; }
    public int RaftTerm { get; init; }
    public int TopicCount { get; init; }
    public int TotalPartitions { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static ClusterInfoPayload Read(ref SurgewavePayloadReader reader)
    {
        return new ClusterInfoPayload
        {
            BrokerId = reader.ReadInt32(),
            Host = reader.ReadString() ?? string.Empty,
            Port = reader.ReadInt32(),
            IsController = reader.ReadUInt8() != 0,
            ControllerId = reader.ReadInt32(),
            ControllerEpoch = reader.ReadInt32(),
            UseRaftConsensus = reader.ReadUInt8() != 0,
            IsRaftLeader = reader.ReadUInt8() != 0,
            RaftTerm = reader.ReadInt32(),
            TopicCount = reader.ReadInt32(),
            TotalPartitions = reader.ReadInt32()
        };
    }

    /// <summary>
    /// Write payload to binary buffer. Use with pre-sized or pooled buffers.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteInt32(BrokerId);
        writer.WriteString(Host);
        writer.WriteInt32(Port);
        writer.WriteUInt8(IsController ? (byte)1 : (byte)0);
        writer.WriteInt32(ControllerId);
        writer.WriteInt32(ControllerEpoch);
        writer.WriteUInt8(UseRaftConsensus ? (byte)1 : (byte)0);
        writer.WriteUInt8(IsRaftLeader ? (byte)1 : (byte)0);
        writer.WriteInt32(RaftTerm);
        writer.WriteInt32(TopicCount);
        writer.WriteInt32(TotalPartitions);
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface (for BigEndianWriter and other implementations).
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteInt32(BrokerId);
        writer.WriteString(Host);
        writer.WriteInt32(Port);
        writer.WriteUInt8(IsController ? (byte)1 : (byte)0);
        writer.WriteInt32(ControllerId);
        writer.WriteInt32(ControllerEpoch);
        writer.WriteUInt8(UseRaftConsensus ? (byte)1 : (byte)0);
        writer.WriteUInt8(IsRaftLeader ? (byte)1 : (byte)0);
        writer.WriteInt32(RaftTerm);
        writer.WriteInt32(TopicCount);
        writer.WriteInt32(TotalPartitions);
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize() =>
        4 +                                           // BrokerId
        2 + System.Text.Encoding.UTF8.GetByteCount(Host ?? "") + // Host (length prefix + bytes)
        4 +                                           // Port
        1 +                                           // IsController
        4 +                                           // ControllerId
        4 +                                           // ControllerEpoch
        1 +                                           // UseRaftConsensus
        1 +                                           // IsRaftLeader
        4 +                                           // RaftTerm
        4 +                                           // TopicCount
        4;                                            // TotalPartitions
}
