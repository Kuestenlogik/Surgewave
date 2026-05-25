namespace Kuestenlogik.Surgewave.Client.Native.Operations.ConsumerGroups;

/// <summary>
/// Response from syncing a consumer group.
/// </summary>
public record SyncGroupResponse(ushort ErrorCode, byte[] Assignment);
