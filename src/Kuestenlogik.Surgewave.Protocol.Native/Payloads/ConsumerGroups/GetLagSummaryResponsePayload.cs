namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.ConsumerGroups;

/// <summary>
/// Wire format for GetLagSummary response.
/// </summary>
public readonly record struct GetLagSummaryResponsePayload
{
    public ushort ErrorCode { get; init; }
    public int GroupCount { get; init; }
    public int GroupsWithHighLag { get; init; }
    public long TotalLag { get; init; }
    public long MaxLag { get; init; }
    public string? MaxLagGroup { get; init; }
    public LagSummaryGroupPayload[] Groups { get; init; }

    public static GetLagSummaryResponsePayload Read(ref SurgewavePayloadReader reader)
    {
        var errorCode = reader.ReadUInt16();
        var groupCount = reader.ReadInt32();
        var groupsWithHighLag = reader.ReadInt32();
        var totalLag = reader.ReadInt64();
        var maxLag = reader.ReadInt64();
        var maxLagGroup = reader.ReadString();
        var groupArrayCount = reader.ReadInt16();

        var groups = new LagSummaryGroupPayload[groupArrayCount];
        for (int i = 0; i < groupArrayCount; i++)
        {
            groups[i] = new LagSummaryGroupPayload
            {
                GroupId = reader.ReadString() ?? "",
                State = reader.ReadString() ?? "",
                TotalLag = reader.ReadInt64(),
                PartitionCount = reader.ReadInt32(),
                MemberCount = reader.ReadInt32()
            };
        }

        return new GetLagSummaryResponsePayload
        {
            ErrorCode = errorCode,
            GroupCount = groupCount,
            GroupsWithHighLag = groupsWithHighLag,
            TotalLag = totalLag,
            MaxLag = maxLag,
            MaxLagGroup = maxLagGroup,
            Groups = groups
        };
    }

    public void Write(ref SurgewavePayloadWriter writer)
    {
        writer.WriteUInt16(ErrorCode);
        writer.WriteInt32(GroupCount);
        writer.WriteInt32(GroupsWithHighLag);
        writer.WriteInt64(TotalLag);
        writer.WriteInt64(MaxLag);
        writer.WriteString(MaxLagGroup);
        writer.WriteInt16((short)(Groups?.Length ?? 0));

        if (Groups != null)
        {
            foreach (var group in Groups)
            {
                writer.WriteString(group.GroupId);
                writer.WriteString(group.State);
                writer.WriteInt64(group.TotalLag);
                writer.WriteInt32(group.PartitionCount);
                writer.WriteInt32(group.MemberCount);
            }
        }
    }

    public void WriteTo(IPayloadWriter writer)
    {
        writer.WriteUInt16(ErrorCode);
        writer.WriteInt32(GroupCount);
        writer.WriteInt32(GroupsWithHighLag);
        writer.WriteInt64(TotalLag);
        writer.WriteInt64(MaxLag);
        writer.WriteString(MaxLagGroup ?? "");
        writer.WriteInt16((short)(Groups?.Length ?? 0));

        if (Groups != null)
        {
            foreach (var group in Groups)
            {
                writer.WriteString(group.GroupId);
                writer.WriteString(group.State);
                writer.WriteInt64(group.TotalLag);
                writer.WriteInt32(group.PartitionCount);
                writer.WriteInt32(group.MemberCount);
            }
        }
    }

    public int EstimateSize()
    {
        var size = 2 + // ErrorCode
            4 + // GroupCount
            4 + // GroupsWithHighLag
            8 + // TotalLag
            8 + // MaxLag
            2 + System.Text.Encoding.UTF8.GetByteCount(MaxLagGroup ?? "") +
            2;  // Group array count

        if (Groups != null)
        {
            foreach (var group in Groups)
            {
                size += 2 + System.Text.Encoding.UTF8.GetByteCount(group.GroupId ?? "");
                size += 2 + System.Text.Encoding.UTF8.GetByteCount(group.State ?? "");
                size += 8 + 4 + 4; // TotalLag, PartitionCount, MemberCount
            }
        }

        return size;
    }
}

/// <summary>
/// Group summary info in GetLagSummary response.
/// </summary>
public readonly record struct LagSummaryGroupPayload
{
    public string GroupId { get; init; }
    public string State { get; init; }
    public long TotalLag { get; init; }
    public int PartitionCount { get; init; }
    public int MemberCount { get; init; }
}
