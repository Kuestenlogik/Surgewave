using Kuestenlogik.Surgewave.Client.Native.Operations.ConsumerGroups;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Payloads.ConsumerGroups;

namespace Kuestenlogik.Surgewave.Client.Native.Commands.ConsumerGroups;

/// <summary>
/// Command to get lag summary for all consumer groups.
/// </summary>
public sealed class GetLagSummaryCommand : NoRequestCommand<LagSummaryResult>
{
    public override SurgewaveOpCode OpCode => SurgewaveOpCode.GetLagSummary;

    public override LagSummaryResult ReadResponse(ref SurgewavePayloadReader reader, SurgewaveResponseHeader header)
    {
        var response = GetLagSummaryResponsePayload.Read(ref reader);

        if (response.ErrorCode != 0)
            throw new InvalidOperationException($"GetLagSummary failed with error code: {response.ErrorCode}");

        var groups = new List<LagSummaryGroup>(response.Groups.Length);
        foreach (var group in response.Groups)
        {
            groups.Add(new LagSummaryGroup(
                group.GroupId,
                group.State,
                group.TotalLag,
                group.PartitionCount,
                group.MemberCount));
        }

        return new LagSummaryResult(
            response.GroupCount,
            response.GroupsWithHighLag,
            response.TotalLag,
            response.MaxLag,
            response.MaxLagGroup,
            groups);
    }
}
