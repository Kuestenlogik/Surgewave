namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.ConsumerGroups;

/// <summary>
/// Wire format for GetGroupLag request.
/// </summary>
public readonly record struct GetGroupLagRequestPayload
{
    public string GroupId { get; init; }

    public static GetGroupLagRequestPayload Read(ref SurgewavePayloadReader reader)
    {
        return new GetGroupLagRequestPayload
        {
            GroupId = reader.ReadString() ?? ""
        };
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteString(GroupId);
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteString(GroupId);
    }

    public int EstimateSize() =>
        2 + System.Text.Encoding.UTF8.GetByteCount(GroupId ?? "");
}
