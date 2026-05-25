using System.Text;
using Kuestenlogik.Surgewave.Protocol.Native;

namespace Kuestenlogik.Surgewave.Client.Native.Commands.ConsumerGroups;

/// <summary>
/// Command to fetch the committed offset for a consumer group.
/// </summary>
public sealed class FetchOffsetCommand : ISurgewaveCommand<long>
{
    private readonly string _groupId;
    private readonly string _topic;
    private readonly int _partition;

    public FetchOffsetCommand(string groupId, string topic, int partition)
    {
        _groupId = groupId;
        _topic = topic;
        _partition = partition;
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.FetchOffset;

    public void WriteRequest(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(_groupId);
        writer.WriteString(_topic);
        writer.WriteInt32(_partition);
    }

    public int EstimateRequestSize() =>
        2 + Encoding.UTF8.GetByteCount(_groupId ?? "") +
        2 + Encoding.UTF8.GetByteCount(_topic ?? "") +
        4;

    public long ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        var errorCode = reader.ReadUInt16();
        if (errorCode != 0)
            throw new InvalidOperationException($"FetchOffset failed with error code: {errorCode}");

        return reader.ReadInt64();
    }
}
