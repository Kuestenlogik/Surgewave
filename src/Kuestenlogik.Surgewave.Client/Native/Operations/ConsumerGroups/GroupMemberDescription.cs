namespace Kuestenlogik.Surgewave.Client.Native.Operations.ConsumerGroups;

/// <summary>
/// Consumer group member description.
/// </summary>
public record GroupMemberDescription(
    string MemberId,
    string? GroupInstanceId,
    string ClientId,
    byte[] Metadata,
    byte[] Assignment);
