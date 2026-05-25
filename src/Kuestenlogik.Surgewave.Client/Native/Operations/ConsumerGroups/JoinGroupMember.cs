namespace Kuestenlogik.Surgewave.Client.Native.Operations.ConsumerGroups;

/// <summary>
/// Member information in join group response.
/// </summary>
public record JoinGroupMember(string MemberId, string? GroupInstanceId, byte[] Metadata);
