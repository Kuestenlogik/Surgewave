namespace Kuestenlogik.Surgewave.Client.Native.Operations.ConsumerGroups;

/// <summary>
/// Consumer group description.
/// </summary>
public record ConsumerGroupDescription(
    string GroupId,
    string State,
    string ProtocolType,
    string ProtocolName,
    int GenerationId,
    List<GroupMemberDescription> Members,
    ushort ErrorCode);
