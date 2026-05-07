using System.Text;

namespace Kuestenlogik.Surgewave.Protocol.Native.Payloads.StreamsGroups;

/// <summary>
/// Wire format for Streams Group Describe request (KIP-1071).
///
/// Wire layout:
///   GroupIds                     string[] (int32 count + strings)
///   IncludeAuthorizedOperations  bool (1 byte)
/// </summary>
public readonly record struct StreamsGroupDescribeRequestPayload
{
    public string[] GroupIds { get; init; }
    public bool IncludeAuthorizedOperations { get; init; }

    /// <summary>
    /// Read payload from binary data. Zero-copy for the span.
    /// </summary>
    public static StreamsGroupDescribeRequestPayload Read(ref SurgewavePayloadReader reader)
    {
        var count = reader.ReadInt32();
        var groupIds = new string[count];
        for (var i = 0; i < count; i++)
            groupIds[i] = reader.ReadString() ?? "";

        var includeAuthorizedOperations = reader.ReadBoolean();

        return new StreamsGroupDescribeRequestPayload
        {
            GroupIds = groupIds,
            IncludeAuthorizedOperations = includeAuthorizedOperations
        };
    }

    /// <summary>
    /// Write payload to binary buffer. Use with pre-sized or pooled buffers.
    /// </summary>
    public void Write(ref SurgewavePayloadWriter writer)
    {
        var ids = GroupIds ?? [];
        writer.WriteInt32(ids.Length);
        foreach (var id in ids)
            writer.WriteString(id);
        writer.WriteBoolean(IncludeAuthorizedOperations);
    }

    /// <summary>
    /// Write payload using IPayloadWriter interface.
    /// </summary>
    public void WriteTo(IPayloadWriter writer)
    {
        var ids = GroupIds ?? [];
        writer.WriteInt32(ids.Length);
        foreach (var id in ids)
            writer.WriteString(id);
        writer.WriteBoolean(IncludeAuthorizedOperations);
    }

    /// <summary>
    /// Estimate buffer size needed for this payload.
    /// </summary>
    public int EstimateSize()
    {
        var ids = GroupIds ?? [];
        var size = 4; // array count
        foreach (var id in ids)
            size += 2 + Encoding.UTF8.GetByteCount(id ?? "");
        size += 1; // IncludeAuthorizedOperations
        return size;
    }
}
