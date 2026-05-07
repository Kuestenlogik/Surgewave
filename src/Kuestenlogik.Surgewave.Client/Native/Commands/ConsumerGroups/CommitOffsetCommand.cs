using System.Text;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Serialization;

namespace Kuestenlogik.Surgewave.Client.Native.Commands.ConsumerGroups;

/// <summary>
/// Command to commit an offset for a consumer group.
/// </summary>
public sealed class CommitOffsetCommand : ISurgewaveVoidCommand
{
    private readonly string _groupId;
    private readonly string _memberId;
    private readonly int _generationId;
    private readonly string _topic;
    private readonly int _partition;
    private readonly long _offset;

    public CommitOffsetCommand(
        string groupId,
        string memberId,
        int generationId,
        string topic,
        int partition,
        long offset)
    {
        _groupId = groupId;
        _memberId = memberId;
        _generationId = generationId;
        _topic = topic;
        _partition = partition;
        _offset = offset;
    }

    public SurgewaveOpCode OpCode => SurgewaveOpCode.CommitOffset;

    public void WriteRequest(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(_groupId);
        writer.WriteString(_memberId);
        writer.WriteInt32(_generationId);
        writer.WriteString(_topic);
        writer.WriteInt32(_partition);
        writer.WriteInt64(_offset);
    }

    public int EstimateRequestSize() =>
        2 + Encoding.UTF8.GetByteCount(_groupId ?? "") +
        2 + Encoding.UTF8.GetByteCount(_memberId ?? "") +
        4 +
        2 + Encoding.UTF8.GetByteCount(_topic ?? "") +
        4 +
        8;

    public Unit ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        ValidateResponse(ref reader, header);
        return Unit.Value;
    }

    public void ValidateResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        var errorCode = reader.ReadUInt16();
        if (errorCode != 0)
            throw new InvalidOperationException($"CommitOffset failed with error code: {errorCode}");
    }
}
