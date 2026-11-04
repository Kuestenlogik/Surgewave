using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads;
using Kuestenlogik.Surgewave.Protocol.Native.Serialization;

namespace Kuestenlogik.Surgewave.Clustering.InterBroker.Payloads;

/// <summary>
/// A broker asking the controller to set one config of a group, or remove it with a null value (#228): only the
/// controller writes to the metadata log, as Kafka's brokers forward IncrementalAlterConfigs to the controller. The
/// answer is the generic status frame.
/// </summary>
public readonly record struct GroupConfigPayload(string GroupId, string Name, string? Value) : ISerializablePayload<GroupConfigPayload>
{
    public static GroupConfigPayload Read(ref SurgewavePayloadReader reader)
    {
        var groupId = reader.ReadString() ?? string.Empty;
        var name = reader.ReadString() ?? string.Empty;
        var value = reader.ReadBoolean() ? reader.ReadString() ?? string.Empty : null;
        return new(groupId, name, value);
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(GroupId);
        writer.WriteString(Name);
        writer.WriteBoolean(Value is not null);
        if (Value is not null)
            writer.WriteString(Value);
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(GroupId);
        writer.WriteString(Name);
        writer.WriteBoolean(Value is not null);
        if (Value is not null)
            writer.WriteString(Value);
    }

    public int EstimateSize() =>
        2 + System.Text.Encoding.UTF8.GetByteCount(GroupId) + 2 + System.Text.Encoding.UTF8.GetByteCount(Name)
        + 1 + (Value is null ? 0 : 2 + System.Text.Encoding.UTF8.GetByteCount(Value));
}
