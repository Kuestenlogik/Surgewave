using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads;
using Kuestenlogik.Surgewave.Protocol.Native.Serialization;

namespace Kuestenlogik.Surgewave.Clustering.InterBroker.Payloads;

/// <summary>
/// A broker asking the controller to change a topic's config (#119): the configs it sets and those it removes. Only the
/// controller writes to the metadata log, as Kafka's brokers forward AlterConfigs to the controller. The answer is the
/// generic status frame.
/// </summary>
public readonly record struct TopicConfigChangePayload(
    string Topic, IReadOnlyDictionary<string, string> Set, IReadOnlyList<string> Removed)
    : ISerializablePayload<TopicConfigChangePayload>
{
    public static TopicConfigChangePayload Read(ref SurgewavePayloadReader reader)
    {
        var topic = reader.ReadString() ?? string.Empty;
        var setCount = reader.ReadInt32();
        var set = new Dictionary<string, string>(setCount, StringComparer.Ordinal);
        for (var i = 0; i < setCount; i++)
        {
            var key = reader.ReadString() ?? string.Empty;
            set[key] = reader.ReadString() ?? string.Empty;
        }
        var removedCount = reader.ReadInt32();
        var removed = new List<string>(removedCount);
        for (var i = 0; i < removedCount; i++)
            removed.Add(reader.ReadString() ?? string.Empty);
        return new(topic, set, removed);
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(Topic);
        writer.WriteInt32(Set.Count);
        foreach (var (key, value) in Set)
        {
            writer.WriteString(key);
            writer.WriteString(value);
        }
        writer.WriteInt32(Removed.Count);
        foreach (var key in Removed)
            writer.WriteString(key);
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(Topic);
        writer.WriteInt32(Set.Count);
        foreach (var (key, value) in Set)
        {
            writer.WriteString(key);
            writer.WriteString(value);
        }
        writer.WriteInt32(Removed.Count);
        foreach (var key in Removed)
            writer.WriteString(key);
    }

    public int EstimateSize()
        => 2 + System.Text.Encoding.UTF8.GetByteCount(Topic) + 4
            + Set.Sum(kv => 4 + System.Text.Encoding.UTF8.GetByteCount(kv.Key) + System.Text.Encoding.UTF8.GetByteCount(kv.Value))
            + 4 + Removed.Sum(k => 2 + System.Text.Encoding.UTF8.GetByteCount(k));
}
