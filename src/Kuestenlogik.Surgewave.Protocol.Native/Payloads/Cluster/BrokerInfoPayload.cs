namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.Cluster;

/// <summary>
/// Wire format for broker info in ListBrokers response.
/// </summary>
public readonly record struct BrokerInfoPayload
{
    public int BrokerId { get; init; }
    public string Host { get; init; }
    public int Port { get; init; }
    public int ReplicationPort { get; init; }
    public bool IsController { get; init; }
    public bool IsAlive { get; init; }
    public string? Rack { get; init; }

    /// <summary>
    /// Read payload from binary data.
    /// </summary>
    public static BrokerInfoPayload Read(ref SurgewavePayloadReader reader)
    {
        return new BrokerInfoPayload
        {
            BrokerId = reader.ReadInt32(),
            Host = reader.ReadString() ?? string.Empty,
            Port = reader.ReadInt32(),
            ReplicationPort = reader.ReadInt32(),
            IsController = reader.ReadUInt8() != 0,
            IsAlive = reader.ReadUInt8() != 0,
            Rack = reader.ReadString()
        };
    }

    /// <summary>
    /// Write payload to binary buffer.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteInt32(BrokerId);
        writer.WriteString(Host);
        writer.WriteInt32(Port);
        writer.WriteInt32(ReplicationPort);
        writer.WriteUInt8(IsController ? (byte)1 : (byte)0);
        writer.WriteUInt8(IsAlive ? (byte)1 : (byte)0);
        writer.WriteString(Rack);
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface.
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteInt32(BrokerId);
        writer.WriteString(Host);
        writer.WriteInt32(Port);
        writer.WriteInt32(ReplicationPort);
        writer.WriteUInt8(IsController ? (byte)1 : (byte)0);
        writer.WriteUInt8(IsAlive ? (byte)1 : (byte)0);
        writer.WriteString(Rack);
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize() =>
        4 +                                                      // BrokerId
        2 + System.Text.Encoding.UTF8.GetByteCount(Host ?? "") + // Host
        4 +                                                      // Port
        4 +                                                      // ReplicationPort
        1 +                                                      // IsController
        1 +                                                      // IsAlive
        2 + (Rack != null ? System.Text.Encoding.UTF8.GetByteCount(Rack) : 0); // Rack
}

/// <summary>
/// Wire format for ListBrokers response.
/// </summary>
public readonly record struct ListBrokersPayload
{
    public IReadOnlyList<BrokerInfoPayload> Brokers { get; init; }

    /// <summary>
    /// Read payload from binary data.
    /// </summary>
    public static ListBrokersPayload Read(ref SurgewavePayloadReader reader)
    {
        var count = reader.ReadInt32();
        var brokers = new BrokerInfoPayload[count];

        for (int i = 0; i < count; i++)
        {
            brokers[i] = BrokerInfoPayload.Read(ref reader);
        }

        return new ListBrokersPayload { Brokers = brokers };
    }

    /// <summary>
    /// Write payload to binary buffer.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteInt32(Brokers.Count);
        foreach (var broker in Brokers)
        {
            broker.Write(ref writer);
        }
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface.
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteInt32(Brokers.Count);
        foreach (var broker in Brokers)
        {
            broker.WriteTo(writer);
        }
    }
}
