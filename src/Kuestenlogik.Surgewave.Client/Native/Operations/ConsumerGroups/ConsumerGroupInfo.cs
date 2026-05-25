namespace Kuestenlogik.Surgewave.Client.Native.Operations.ConsumerGroups;

/// <summary>
/// Consumer group information.
/// </summary>
public record ConsumerGroupInfo(string GroupId, string ProtocolType, string State);
