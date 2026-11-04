using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads;
using Kuestenlogik.Surgewave.Protocol.Native.Serialization;

namespace Kuestenlogik.Surgewave.Clustering.InterBroker.Payloads;

/// <summary>
/// A broker asking the controller to create a topic (#207): only the controller writes topics to the metadata log,
/// so a broker that is asked for one — CreateTopics, or a Metadata request that auto-creates — hands it on, as Kafka's
/// brokers forward CreateTopics to the controller. The answer is the generic status frame.
/// </summary>
public readonly record struct CreateTopicPayload(
    string Topic, int Partitions, short ReplicationFactor, IReadOnlyDictionary<string, string> Config)
    : ISerializablePayload<CreateTopicPayload>
{
    public static CreateTopicPayload Read(ref SurgewavePayloadReader reader)
    {
        var topic = reader.ReadString() ?? string.Empty;
        var partitions = reader.ReadInt32();
        var replicationFactor = reader.ReadInt16();
        var count = reader.ReadInt32();
        var config = new Dictionary<string, string>(count, StringComparer.Ordinal);
        for (var i = 0; i < count; i++)
        {
            var key = reader.ReadString() ?? string.Empty;
            config[key] = reader.ReadString() ?? string.Empty;
        }
        return new(topic, partitions, replicationFactor, config);
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(Topic);
        writer.WriteInt32(Partitions);
        writer.WriteInt16(ReplicationFactor);
        writer.WriteInt32(Config.Count);
        foreach (var (key, value) in Config)
        {
            writer.WriteString(key);
            writer.WriteString(value);
        }
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(Topic);
        writer.WriteInt32(Partitions);
        writer.WriteInt16(ReplicationFactor);
        writer.WriteInt32(Config.Count);
        foreach (var (key, value) in Config)
        {
            writer.WriteString(key);
            writer.WriteString(value);
        }
    }

    public int EstimateSize()
        => 2 + System.Text.Encoding.UTF8.GetByteCount(Topic) + 4 + 2 + 4
            + Config.Sum(kv => 4 + System.Text.Encoding.UTF8.GetByteCount(kv.Key) + System.Text.Encoding.UTF8.GetByteCount(kv.Value));
}
